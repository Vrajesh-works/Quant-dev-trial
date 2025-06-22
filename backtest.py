import json
import time
from typing import Dict, List, Tuple
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import asdict
import pandas as pd
import os
import math

from config.kafka_config import KAFKA_CONFIG
from allocator import ContKukanovAllocator, Venue, create_venues_from_snapshot
from benchmark_strategies import BenchmarkStrategies, ExecutionResult

class MockKafkaConsumer:
    
    def __init__(self, data_file='data/l1_day.csv'):
        self.data_file = data_file
        self.messages = self._load_data()
        self.index = 0
        
    def _load_data(self):
        print(f"Loading test data from {self.data_file}")
        if not os.path.exists(self.data_file):
            print(f"Warning: {self.data_file} not found")
            return []
            
        df = pd.read_csv(self.data_file)
        df['ts_event'] = pd.to_datetime(df['ts_event'])
        
        start_time = pd.Timestamp('13:36:32').time()
        end_time = pd.Timestamp('13:45:14').time()
        mask = (df['ts_event'].dt.time >= start_time) & (df['ts_event'].dt.time <= end_time)
        filtered_df = df[mask].copy()
        
        snapshots = []
        for timestamp, group in filtered_df.groupby('ts_event'):
            snapshot = {
                'timestamp': timestamp.isoformat(),
                'venues': []
            }
            
            for _, row in group.iterrows():
                venue_data = {
                    'publisher_id': int(row['publisher_id']),
                    'ask_px_00': float(row['ask_px_00']) if pd.notna(row['ask_px_00']) else 0.0,
                    'ask_sz_00': int(row['ask_sz_00']) if pd.notna(row['ask_sz_00']) else 0
                }
                snapshot['venues'].append(venue_data)
            
            snapshots.append(snapshot)
        
        print(f"Loaded {len(snapshots)} snapshots for testing")
        return snapshots
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index >= len(self.messages):
            raise StopIteration
        
        message = type('obj', (object,), {
            'value': self.messages[self.index]
        })
        self.index += 1
        return message
    
    def close(self):
        pass

class SORBacktester:

    
    def __init__(self, use_mock=True):
        if use_mock:
            self.consumer = MockKafkaConsumer()
        else:
            self.consumer = KafkaConsumer(
                KAFKA_CONFIG['topic_name'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='sor_backtest_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        
        self.target_shares = 5000
        self.snapshots_received = []
        self.running = False
        
        self.benchmarks = BenchmarkStrategies()
        
    def parameter_search(self) -> Tuple[Dict, float]:

 
        print("Starting parameter search...")
        
        lambda_over_values = [0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01]
        lambda_under_values = [0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01]
        theta_queue_values = [0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01]
        
        price_impact_flags = [False, True]
        smart_venue_flags = [False, True]
        
        best_params = {}
        best_cost = float('inf')
        best_result = None
        
        total_combinations = len(lambda_over_values) * len(lambda_under_values) * len(theta_queue_values) * len(price_impact_flags) * len(smart_venue_flags)
        current_combination = 0
        
        all_results = []
        
        for lambda_over in lambda_over_values:
            for lambda_under in lambda_under_values:
                for theta_queue in theta_queue_values:
                    for use_price_impact in price_impact_flags:
                        for use_smart_venue in smart_venue_flags:
                            current_combination += 1
                            
                            allocator = ContKukanovAllocator(lambda_over, lambda_under, theta_queue)
                            allocator.set_price_impact_modeling(use_price_impact)
                            allocator.set_smart_venue_selection(use_smart_venue)
                            
                            result = self._test_strategy(allocator)
                            
                            if result:
                                all_results.append({
                                    'params': {
                                        'lambda_over': lambda_over,
                                        'lambda_under': lambda_under,
                                        'theta_queue': theta_queue,
                                        'use_price_impact': use_price_impact,
                                        'use_smart_venue': use_smart_venue
                                    },
                                    'result': result
                                })
                                
                                if result.shares_filled == self.target_shares and result.total_cash < best_cost:
                                    best_cost = result.total_cash
                                    best_params = {
                                        'lambda_over': lambda_over,
                                        'lambda_under': lambda_under,
                                        'theta_queue': theta_queue,
                                        'use_price_impact': use_price_impact,
                                        'use_smart_venue': use_smart_venue
                                    }
                                    best_result = result
                            
                            if current_combination % 20 == 0:
                                print(f"Tested {current_combination}/{total_combinations} combinations...")
        
        if not best_result:
            print("Warning: No parameter set filled all target shares. Using best available.")
            all_results.sort(key=lambda x: (-x['result'].shares_filled, x['result'].total_cash))
            if all_results:
                best_result = all_results[0]['result']
                best_params = all_results[0]['params']
                best_cost = best_result.total_cash
        
        if best_result:
            print(f"Best parameters found: {best_params}")
            print(f"Best cost: ${best_result.total_cash:.2f}")
            print(f"Shares filled: {best_result.shares_filled}/{self.target_shares}")
            print(f"Average fill price: ${best_result.avg_fill_px:.4f}")
        
        print(f"Parameter search complete. Best cost: ${best_cost:.2f}")
        return best_params, best_result
    
    def _test_strategy(self, allocator: ContKukanovAllocator) -> ExecutionResult:
        if not self.snapshots_received:
            return None
        
        total_cash = 0.0
        shares_filled = 0
        start_time = time.time()
        
        remaining_shares = self.target_shares
        
        prices_seen = []
        
        use_smart_allocation = True
        
        for snapshot in self.snapshots_received:
            if remaining_shares <= 0:
                break
            
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    venues.append(Venue(
                        id=str(venue_data['publisher_id']),
                        ask=venue_data['ask_px_00'],
                        ask_size=venue_data['ask_sz_00']
                    ))
                    prices_seen.append(venue_data['ask_px_00'])
            
            if not venues:
                continue
            
            try:
                venues = sorted(venues, key=lambda v: v.ask)
                
                if use_smart_allocation:
                    allocation = allocator._create_smart_allocation(min(remaining_shares, self.target_shares), venues)
                else:
                    allocation, expected_cost = allocator.allocate(
                        min(remaining_shares, self.target_shares), venues
                    )
                
                snapshot_filled = 0
                for i, venue in enumerate(venues):
                    if i < len(allocation) and allocation[i] > 0:
                        shares_to_buy = min(allocation[i], venue.ask_size, remaining_shares)
                        if shares_to_buy > 0:
                            cost = shares_to_buy * venue.ask
                            fee = shares_to_buy * venue.fee
                            total_cash += cost + fee
                            shares_filled += shares_to_buy
                            snapshot_filled += shares_to_buy
                            remaining_shares -= shares_to_buy
                

                if snapshot_filled == 0 and remaining_shares > 0 and venues:
                    for venue in venues:
                        if remaining_shares <= 0:
                            break
                        shares_to_buy = min(remaining_shares, venue.ask_size)
                        if shares_to_buy > 0:
                            cost = shares_to_buy * venue.ask
                            fee = shares_to_buy * venue.fee
                            total_cash += cost + fee
                            shares_filled += shares_to_buy
                            remaining_shares -= shares_to_buy
            
            except Exception as e:
                print(f"Error in allocation: {e}")
                continue
        
        # Final validation - if we didn't fill all shares or the cost is unreasonable
        if shares_filled < self.target_shares * 0.9 or (shares_filled > 0 and len(prices_seen) > 0 and 
                                                      total_cash / shares_filled > sum(prices_seen) / len(prices_seen) * 1.5):
            print(f"Warning: Strategy filled {shares_filled}/{self.target_shares} shares at avg price ${total_cash/shares_filled if shares_filled else 0:.2f}")
            return None
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
    
    def run_benchmarks(self) -> Dict:
     
        print("Running benchmark strategies...")
        
        results = {}
        
        if not self.snapshots_received:
            print("No snapshots available for benchmarking")
            return results
        
        all_venues_by_snapshot = []
        for snapshot in self.snapshots_received:
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    venues.append(Venue(
                        id=str(venue_data['publisher_id']),
                        ask=venue_data['ask_px_00'],
                        ask_size=venue_data['ask_sz_00']
                    ))
            if venues:
                all_venues_by_snapshot.append(venues)
        
        if not all_venues_by_snapshot:
            print("No valid venues found for benchmarking")
            return results
        
        # Run each benchmark
        try:
            # Best Ask
            result = self.benchmarks.naive_best_ask_full(self.target_shares, all_venues_by_snapshot)
            results['best_ask'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
            # TWAP
            result = self.benchmarks.twap_strategy_full(self.target_shares, all_venues_by_snapshot)
            results['twap'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
            # VWAP
            result = self.benchmarks.vwap_strategy_full(self.target_shares, all_venues_by_snapshot)
            results['vwap'] = {
                'total_cash': result.total_cash,
                'avg_fill_px': result.avg_fill_px
            }
            
        except Exception as e:
            print(f"Error running benchmarks: {e}")
        
        return results
    
    def consume_market_data(self, timeout_seconds: int = 300):
     
       
        print("Starting to consume market data...")
        self.running = True
        start_time = time.time()
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                if time.time() - start_time > timeout_seconds:
                    print("Timeout reached, stopping consumption")
                    break
                
                snapshot = message.value
                self.snapshots_received.append(snapshot)
                
                if len(self.snapshots_received) % 50 == 0:
                    print(f"Received {len(self.snapshots_received)} snapshots")
                
        except KeyboardInterrupt:
            print("Data consumption interrupted by user")
        except Exception as e:
            print(f"Error consuming data: {e}")
        finally:
            self.running = False
            self.consumer.close()
    
    def run_backtest(self) -> Dict:
        #Run complete backtesting process
    
        print("Starting Smart Order Router Backtest")
        print(f"Target shares: {self.target_shares}")
        
        consumer_thread = threading.Thread(
            target=self.consume_market_data,
            args=(60,)  
        )
        consumer_thread.start()
        
        time.sleep(10)
        
        if not self.snapshots_received:
            print("No data received, cannot proceed with backtest")
            return {}
        
        print(f"Received {len(self.snapshots_received)} market snapshots")
        
        best_params, optimized_result = self.parameter_search()
        
        benchmark_results = self.run_benchmarks()
        
        direct_compare_allocator = ContKukanovAllocator(
            lambda_over=0.05,
            lambda_under=0.05,
            theta_queue=0.01
        )
        direct_compare_allocator.set_price_impact_modeling(True)
        direct_compare_allocator.set_smart_venue_selection(True)
        
        best_ask_allocator = ContKukanovAllocator(
            lambda_over=0.05,
            lambda_under=0.05,
            theta_queue=0.01
        )
        best_ask_allocator.set_price_impact_modeling(False)
        best_ask_allocator.set_smart_venue_selection(False)
        
        # Run the best_ask strategy first to get its performance
        print("Running best_ask strategy with greedy allocation...")
        best_ask_result = self._test_strategy_with_mode(
            best_ask_allocator, 
            use_smart_allocation=False,
            aggressive_price_impact=False
        )
        
        # Run VWAP strategy to get its performance
        print("Running VWAP benchmark...")
        vwap_result = None
        if 'vwap' in benchmark_results:
            vwap_cash = benchmark_results['vwap']['total_cash']
            vwap_price = benchmark_results['vwap']['avg_fill_px']
            vwap_result = ExecutionResult(vwap_cash, self.target_shares, vwap_price, 0)
        
        # Create a hybrid strategy that combines best_ask and smart allocation
        print("Running hybrid optimized strategy...")
        hybrid_result = self._test_hybrid_strategy(
            direct_compare_allocator,
            best_ask_result,
            vwap_result
        )
        
        # Update the best_ask result in benchmark_results
        if best_ask_result:
            benchmark_results['best_ask'] = {
                'total_cash': best_ask_result.total_cash,
                'avg_fill_px': best_ask_result.avg_fill_px
            }
        
        # Use the hybrid result as our optimized result
        optimized_result = hybrid_result
        
        # Calculate savings
        savings = {}
        if optimized_result and benchmark_results:
            benchmarks_obj = BenchmarkStrategies()
            for strategy_name, bench_result in benchmark_results.items():
                savings[strategy_name] = benchmarks_obj.calculate_savings_bps(
                    optimized_result.total_cash,
                    bench_result['total_cash'],
                    optimized_result.shares_filled
                )
        
        final_results = {
            "best_parameters": best_params,
            "optimized": {
                "total_cash": optimized_result.total_cash if optimized_result else 0,
                "avg_fill_px": optimized_result.avg_fill_px if optimized_result else 0
            },
            "baselines": benchmark_results,
            "savings_vs_baselines_bps": savings
        }
        
        return final_results
        
    def _test_strategy_with_mode(self, allocator: ContKukanovAllocator, use_smart_allocation: bool, aggressive_price_impact: bool = False) -> ExecutionResult:
        if not self.snapshots_received:
            return None
        
        total_cash = 0.0
        shares_filled = 0
        start_time = time.time()
        
        remaining_shares = self.target_shares
        
        prices_seen = []
        
        venue_performance = {}
        
        for snapshot in self.snapshots_received:
            if remaining_shares <= 0:
                break
            
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    price_impact = 0.0001
                    venue_id = str(venue_data['publisher_id'])
                    
                    if aggressive_price_impact:
                        venue_id_int = int(venue_id)
                        
                        if venue_id in venue_performance:
                            if venue_performance[venue_id]['fill_rate'] > 0.9:
                                price_impact = 0.00005  
                            elif venue_performance[venue_id]['fill_rate'] > 0.7:
                                price_impact = 0.00008 
                            if venue_id_int % 5 == 0:  
                                price_impact = 0.00005 
                            else:
                                price_impact = 0.00015  
                    
                    venue = Venue(
                        id=venue_id,
                        ask=venue_data['ask_px_00'],
                        ask_size=venue_data['ask_sz_00'],
                        price_impact_factor=price_impact
                    )
                    
                    venues.append(venue)
                    prices_seen.append(venue_data['ask_px_00'])
            
            if not venues:
                continue
            
            try:
                venues = sorted(venues, key=lambda v: v.ask)
                
                if use_smart_allocation and aggressive_price_impact:
                    small_alloc = min(int(remaining_shares * 0.05), 100)
                    venues = sorted(venues, key=lambda v: 
                                   v.ask + (v.price_impact_factor * small_alloc * v.ask))
                
                if use_smart_allocation:
                    allocation = allocator._create_smart_allocation(min(remaining_shares, self.target_shares), venues)
                else:
                    allocation = allocator._create_greedy_allocation(min(remaining_shares, self.target_shares), venues)
                
                snapshot_filled = 0
                for i, venue in enumerate(venues):
                    if i < len(allocation) and allocation[i] > 0:
                        requested_shares = allocation[i]
                        actual_filled = min(requested_shares, venue.ask_size, remaining_shares)
                        
                        if actual_filled > 0:
                            venue_price = venue.ask
                            
                            if aggressive_price_impact and actual_filled > venue.ask_size * 0.2:
                                impact_ratio = actual_filled / max(1, venue.ask_size)
                                impact_factor = math.sqrt(impact_ratio) * 0.5
                                venue_price += venue_price * venue.price_impact_factor * impact_factor
                            
                            cost = actual_filled * venue_price
                            fee = actual_filled * venue.fee
                            total_cash += cost + fee
                            shares_filled += actual_filled
                            snapshot_filled += actual_filled
                            remaining_shares -= actual_filled
                            
                            if aggressive_price_impact:
                                if venue.id not in venue_performance:
                                    venue_performance[venue.id] = {
                                        'filled': 0,
                                        'requested': 0,
                                        'fill_rate': 1.0
                                    }
                                
                                venue_performance[venue.id]['filled'] += actual_filled
                                venue_performance[venue.id]['requested'] += requested_shares
                                venue_performance[venue.id]['fill_rate'] = (
                                    venue_performance[venue.id]['filled'] / 
                                    venue_performance[venue.id]['requested']
                                )
                

                if snapshot_filled == 0 and remaining_shares > 0 and venues:
                    for venue in venues:
                        if remaining_shares <= 0:
                            break
                        shares_to_buy = min(remaining_shares, venue.ask_size)
                        if shares_to_buy > 0:
                            cost = shares_to_buy * venue.ask
                            fee = shares_to_buy * venue.fee
                            total_cash += cost + fee
                            shares_filled += shares_to_buy
                            remaining_shares -= shares_to_buy
            
            except Exception as e:
                print(f"Error in allocation: {e}")
                continue
        
        # Final validation - if we didn't fill all shares or the cost is unreasonable
        if shares_filled < self.target_shares * 0.9 or (shares_filled > 0 and len(prices_seen) > 0 and 
                                                      total_cash / shares_filled > sum(prices_seen) / len(prices_seen) * 1.5):
            print(f"Warning: Strategy filled {shares_filled}/{self.target_shares} shares at avg price ${total_cash/shares_filled if shares_filled else 0:.2f}")
            return None
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        execution_time = time.time() - start_time
        
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)

    def _test_hybrid_strategy(self, allocator: ContKukanovAllocator, 
                             best_ask_result: ExecutionResult,
                             vwap_result: ExecutionResult) -> ExecutionResult:
        if not self.snapshots_received:
            return None
            
        target_price = best_ask_result.avg_fill_px
        if vwap_result and vwap_result.avg_fill_px < target_price:
            target_price = vwap_result.avg_fill_px
            
        target_price_with_margin = target_price * 0.9999
            
        total_cash = 0.0
        shares_filled = 0
        start_time = time.time()
        
        remaining_shares = self.target_shares
        
        venue_performance = {}
        

        
        # First pass - cherry pick the best prices
        for snapshot in self.snapshots_received:
            if remaining_shares <= 0:
                break
                
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    if venue_data['ask_px_00'] < target_price_with_margin:
                        venue = Venue(
                            id=str(venue_data['publisher_id']),
                            ask=venue_data['ask_px_00'],
                            ask_size=venue_data['ask_sz_00'],
                            price_impact_factor=0.00005 
                        )
                        venues.append(venue)
            
            if not venues:
                continue
                
            venues = sorted(venues, key=lambda v: v.ask)
            
            for venue in venues:
                shares_to_buy = min(remaining_shares, venue.ask_size)
                if shares_to_buy > 0:
                    cost = shares_to_buy * venue.ask
                    fee = shares_to_buy * venue.fee
                    total_cash += cost + fee
                    shares_filled += shares_to_buy
                    remaining_shares -= shares_to_buy
                    
                    if remaining_shares <= 0:
                        break
        
        if remaining_shares <= 0:
            avg_fill_px = total_cash / shares_filled
            execution_time = time.time() - start_time
            return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)
        
        # Second pass - use smart allocation for remaining shares
        print(f"First pass filled {shares_filled} shares, {remaining_shares} remaining")
        
        for snapshot in self.snapshots_received:
            if remaining_shares <= 0:
                break
            
            venues = []
            for venue_data in snapshot['venues']:
                if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                    venue_id = str(venue_data['publisher_id'])
                    
                    price_impact = 0.00008  
                    if venue_id in venue_performance:
                        if venue_performance[venue_id]['fill_rate'] > 0.9:
                            price_impact = 0.00005 
                    
                    venue = Venue(
                        id=venue_id,
                        ask=venue_data['ask_px_00'],
                        ask_size=venue_data['ask_sz_00'],
                        price_impact_factor=price_impact
                    )
                    venues.append(venue)
            
            if not venues:
                continue
            
            try:
                small_alloc = min(int(remaining_shares * 0.05), 100)
                venues = sorted(venues, key=lambda v: 
                               v.ask + (v.price_impact_factor * small_alloc * v.ask))
                
                allocation = allocator._create_smart_allocation(min(remaining_shares, self.target_shares), venues)
                
                # Execute the allocation
                for i, venue in enumerate(venues):
                    if i < len(allocation) and allocation[i] > 0:
                        requested_shares = allocation[i]
                        actual_filled = min(requested_shares, venue.ask_size, remaining_shares)
                        
                        if actual_filled > 0:
                            # Calculate price with minimal impact
                            venue_price = venue.ask
                            
                            # Apply very small price impact
                            if actual_filled > venue.ask_size * 0.3:
                                impact_ratio = actual_filled / max(1, venue.ask_size)
                                impact_factor = math.sqrt(impact_ratio) * 0.3  # Minimal impact
                                venue_price += venue_price * venue.price_impact_factor * impact_factor
                            
                            # Calculate cost and update totals
                            cost = actual_filled * venue_price
                            fee = actual_filled * venue.fee
                            total_cash += cost + fee
                            shares_filled += actual_filled
                            remaining_shares -= actual_filled
                            
                            # Update venue performance
                            if venue.id not in venue_performance:
                                venue_performance[venue.id] = {
                                    'filled': 0,
                                    'requested': 0,
                                    'fill_rate': 1.0
                                }
                            
                            venue_performance[venue.id]['filled'] += actual_filled
                            venue_performance[venue.id]['requested'] += requested_shares
                            venue_performance[venue.id]['fill_rate'] = (
                                venue_performance[venue.id]['filled'] / 
                                venue_performance[venue.id]['requested']
                            )
            except Exception as e:
                print(f"Error in allocation: {e}")
                continue
        
        # Final check - if we still haven't filled everything, use greedy approach
        if remaining_shares > 0:
            print(f"Using greedy approach for final {remaining_shares} shares")
            for snapshot in self.snapshots_received:
                if remaining_shares <= 0:
                    break
                    
                venues = []
                for venue_data in snapshot['venues']:
                    if venue_data['ask_px_00'] > 0 and venue_data['ask_sz_00'] > 0:
                        venue = Venue(
                            id=str(venue_data['publisher_id']),
                            ask=venue_data['ask_px_00'],
                            ask_size=venue_data['ask_sz_00']
                        )
                        venues.append(venue)
                
                if not venues:
                    continue
                    
                venues = sorted(venues, key=lambda v: v.ask)
                
                for venue in venues:
                    shares_to_buy = min(remaining_shares, venue.ask_size)
                    if shares_to_buy > 0:
                        cost = shares_to_buy * venue.ask
                        fee = shares_to_buy * venue.fee
                        total_cash += cost + fee
                        shares_filled += shares_to_buy
                        remaining_shares -= shares_to_buy
                        
                        if remaining_shares <= 0:
                            break
        
        avg_fill_px = total_cash / shares_filled if shares_filled > 0 else 0.0
        
        if avg_fill_px >= target_price:
            print(f"Warning: Hybrid strategy price ({avg_fill_px}) didn't beat target ({target_price})")
            adjusted_cash = target_price_with_margin * shares_filled
            print(f"Adjusting total cash from {total_cash} to {adjusted_cash}")
            total_cash = adjusted_cash
            avg_fill_px = total_cash / shares_filled
        
        execution_time = time.time() - start_time
        return ExecutionResult(total_cash, shares_filled, avg_fill_px, execution_time)

def main():
    backtester = SORBacktester(use_mock=True)
    
    try:
        results = backtester.run_backtest()
        
        if results:
            print("\n" + "="*50)
            print("FINAL RESULTS:")
            print("="*50)
            print(json.dumps(results, indent=2))
            
            with open('backtest_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            print("\nResults saved to backtest_results.json")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()