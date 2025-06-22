import math
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class Venue:
    id: str
    ask: float
    ask_size: int
    fee: float = 0.003  
    rebate: float = 0.002
    historical_fill_rate: float = 1.0
    price_impact_factor: float = 0.0001

class ContKukanovAllocator:
    #Implements the Cont-Kukanov optimal order allocation algorithm
   
    
    def __init__(self, lambda_over: float, lambda_under: float, theta_queue: float):
        self.lambda_over = lambda_over
        self.lambda_under = lambda_under
        self.theta_queue = theta_queue
        self.venue_performance = {}
        self.use_price_impact = True
        self.smart_venue_selection = True
    
    def allocate(self, order_size: int, venues: List[Venue]) -> Tuple[List[int], float]:
        self._update_venue_metrics(venues)
        
        venues = sorted(venues, key=lambda v: self._calculate_effective_price(v, order_size))
        

        step = max(10, min(100, order_size // 50))
        
        splits = [[]]
        
        for v_idx in range(len(venues)):
            new_splits = []
            for alloc in splits:
                used = sum(alloc)
                max_v = min(order_size - used, venues[v_idx].ask_size, order_size // max(1, len(venues) - v_idx))
                
                for q in range(0, max_v + 1, step):
                    new_splits.append(alloc + [q])
            
            if len(new_splits) > 1000:
                new_splits = sorted(new_splits, key=lambda s: sum(s), reverse=True)[:1000]
            
            splits = new_splits
        
        best_cost = float('inf')
        best_split = []
        
        # Find the best allocation
        for alloc in splits:
            if sum(alloc) != order_size:
                continue
                
            cost = self._compute_cost(alloc, venues, order_size)
            if cost < best_cost:
                best_cost = cost
                best_split = alloc
        
        if not best_split:
            best_split = self._create_smart_allocation(order_size, venues)
            best_cost = self._compute_cost(best_split, venues, order_size)
        
        if best_cost > order_size * max(v.ask for v in venues) * 2:
            best_split = self._create_smart_allocation(order_size, venues)
            best_cost = self._compute_cost(best_split, venues, order_size)
        
        return best_split, best_cost
    
    def _update_venue_metrics(self, venues: List[Venue]):
        for venue in venues:
            venue_id = venue.id
            if venue_id not in self.venue_performance:
                self.venue_performance[venue_id] = {
                    'fill_count': 0,
                    'total_fills': 0,
                    'avg_price': venue.ask,
                    'price_impact': venue.price_impact_factor
                }
    
    def _calculate_effective_price(self, venue: Venue, order_size: int) -> float:
        base_price = venue.ask
        
        # Add fee impact
        price_with_fees = base_price + venue.fee
        
        if self.use_price_impact:
            impact_ratio = min(1.0, order_size / max(1, venue.ask_size))
            impact_factor = math.sqrt(impact_ratio) * 1.5  # More aggressive impact
            price_impact = venue.price_impact_factor * impact_factor * base_price
            price_with_fees += price_impact
        
        if self.smart_venue_selection and venue.id in self.venue_performance:
            perf = self.venue_performance[venue.id]
            if perf['fill_count'] > 0:
                fill_rate_factor = 1.0 / max(0.5, venue.historical_fill_rate)
                price_with_fees *= fill_rate_factor
        
        return price_with_fees
    
    def _create_greedy_allocation(self, order_size: int, venues: List[Venue]) -> List[int]:
        allocation = [0] * len(venues)
        remaining = order_size
        
        sorted_indices = sorted(range(len(venues)), key=lambda i: venues[i].ask)
        
        for idx in sorted_indices:
            if remaining <= 0:
                break
            shares = min(remaining, venues[idx].ask_size)
            allocation[idx] = shares
            remaining -= shares
        
        return allocation
    
    def _create_smart_allocation(self, order_size: int, venues: List[Venue]) -> List[int]:
        allocation = [0] * len(venues)
        remaining = order_size
        

        effective_prices = []
        for i, venue in enumerate(venues):
            small_alloc = min(int(order_size * 0.05), venue.ask_size)
            if small_alloc > 0:
                eff_price = self._calculate_effective_price(venue, small_alloc)
                effective_prices.append((i, eff_price, venue))
        
        effective_prices.sort(key=lambda x: x[1])
        
        best_venues = effective_prices[:max(1, int(len(effective_prices) * 0.2))]
        
        for idx, _, venue in best_venues:
            alloc_size = min(remaining, int(venue.ask_size * 0.4))
            if alloc_size > 0:
                allocation[idx] += alloc_size
                remaining -= alloc_size
                
            if remaining <= 0:
                break

        if remaining > 0:
            round_robin_venues = [idx for idx, _, _ in effective_prices[:min(5, len(effective_prices))]]
            
            while remaining > 0 and round_robin_venues:
                for idx in round_robin_venues[:]:
                    chunk_size = min(remaining, int(venues[idx].ask_size * 0.15), 300)
                    if chunk_size <= 0:
                        round_robin_venues.remove(idx)
                        continue
                    
                    allocation[idx] += chunk_size
                    remaining -= chunk_size
                    
                    if remaining <= 0:
                        break
        
        if remaining > 0:
            for idx, _, _ in effective_prices:
                available = venues[idx].ask_size - allocation[idx]
                if available > 0:
                    shares_to_add = min(remaining, available)
                    allocation[idx] += shares_to_add
                    remaining -= shares_to_add
                
                if remaining <= 0:
                    break
        
        return allocation
    
    def _compute_cost(self, split: List[int], venues: List[Venue], order_size: int) -> float:
        executed = 0
        cash_spent = 0
        
        for i, venue in enumerate(venues):
            exe = min(split[i], venue.ask_size)
            
            price_with_impact = venue.ask
            if self.use_price_impact and exe > 0:
                impact_ratio = exe / max(1, venue.ask_size)
                impact_factor = math.sqrt(impact_ratio) * 1.5
                price_with_impact += venue.price_impact_factor * impact_factor * venue.ask
            
            executed += exe
            cash_spent += exe * (price_with_impact + venue.fee)
            
            maker_rebate = max(split[i] - exe, 0) * venue.rebate
            cash_spent -= maker_rebate
        
        underfill = max(order_size - executed, 0)
        overfill = max(executed - order_size, 0)
        
        risk_penalty = self.theta_queue * (underfill + overfill)
        cost_penalty = self.lambda_under * underfill + self.lambda_over * overfill
        
        return cash_spent + risk_penalty + cost_penalty
    
    def update_parameters(self, lambda_over: float, lambda_under: float, theta_queue: float):
        self.lambda_over = lambda_over
        self.lambda_under = lambda_under
        self.theta_queue = theta_queue
        
    def set_price_impact_modeling(self, enabled: bool):
        self.use_price_impact = enabled
        
    def set_smart_venue_selection(self, enabled: bool):
        self.smart_venue_selection = enabled

def create_venues_from_snapshot(snapshot_data: Dict) -> List[Venue]:
    venues = []
    
    venue_data = {}
    for record in snapshot_data:
        venue_id = str(record['publisher_id'])
        if venue_id not in venue_data:
            venue_data[venue_id] = {
                'ask': record['ask_px_00'],
                'ask_size': record['ask_sz_00']
            }
    
    for venue_id, data in venue_data.items():

        price_impact = 0.0001 + (int(venue_id) % 10) * 0.00005
        
        venues.append(Venue(
            id=venue_id,
            ask=data['ask'],
            ask_size=data['ask_size'],
            price_impact_factor=price_impact
        ))
    
    return venues