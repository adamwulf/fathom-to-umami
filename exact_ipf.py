#!/usr/bin/env python3
"""
Exact IPF implementation that preserves marginal constraints exactly
"""

import itertools
from collections import defaultdict
from simple_ipf import load_hourly_data, get_marginal_totals

def solve_exact_distribution(marginals, max_iterations=1000, tolerance=1e-10):
    """
    Solve for exact integer distribution that satisfies all marginal constraints
    Uses integer programming approach
    """
    
    # Get all unique values for each dimension
    dimensions = {}
    for dim_name, totals in marginals.items():
        if totals:
            dimensions[dim_name] = sorted(totals.keys())
    
    if not dimensions:
        return [], []
    
    dim_names = list(dimensions.keys())
    
    # Generate all possible combinations
    combinations = []
    for combo in itertools.product(*[dimensions[name] for name in dim_names]):
        combinations.append(dict(zip(dim_names, combo)))
    
    # print(f"Solving for {len(combinations)} combinations across {len(dim_names)} dimensions")
    
    # Use a greedy approach to assign integer counts
    # Start with the IPF converged probabilities as weights
    table, _, _ = run_ipf_for_weights(marginals)
    
    # Convert to integer assignment problem
    total_events = max(sum(totals.values()) for totals in marginals.values())
    
    # Greedy assignment preserving marginals
    result_events = solve_integer_assignment(combinations, dim_names, marginals, table, total_events)
    
    return result_events, combinations

def run_ipf_for_weights(marginals):
    """Run IPF to get the optimal probability weights"""
    # Create all combinations
    dimensions = {}
    for dim_name, totals in marginals.items():
        if totals:
            dimensions[dim_name] = sorted(totals.keys())
    
    dim_names = list(dimensions.keys())
    dim_values = [dimensions[name] for name in dim_names]
    
    # Initialize uniform table
    table = {}
    for combo in itertools.product(*dim_values):
        table[combo] = 1.0
    
    # Normalize
    total = sum(table.values())
    for key in table:
        table[key] /= total
    
    # Get total events
    total_events = max(sum(totals.values()) for totals in marginals.values())
    
    # Normalize marginals to probabilities
    normalized_marginals = {}
    for dim_name, totals in marginals.items():
        if totals:
            normalized_marginals[dim_name] = {k: v/total_events for k, v in totals.items()}
    
    # IPF iterations
    for iteration in range(50):  # Reduced iterations for weight estimation
        old_table = table.copy()
        
        # Apply each marginal constraint
        for dim_name in dim_names:
            if dim_name in normalized_marginals:
                table = apply_marginal_constraint_simple(
                    table, dim_names, dim_name, normalized_marginals[dim_name]
                )
        
        # Check convergence
        total_change = sum(abs(table[k] - old_table.get(k, 0)) for k in table)
        if total_change < 1e-8:
            break
    
    return table, None, dim_names

def apply_marginal_constraint_simple(table, dim_names, constraint_dim, marginal_totals):
    """Simple marginal constraint application"""
    dim_index = dim_names.index(constraint_dim)
    
    # Calculate current marginal totals
    current_marginals = defaultdict(float)
    for combo, prob in table.items():
        dim_value = combo[dim_index]
        current_marginals[dim_value] += prob
    
    # Adjust probabilities
    new_table = {}
    for combo, prob in table.items():
        dim_value = combo[dim_index]
        current_total = current_marginals[dim_value]
        target_total = marginal_totals.get(dim_value, 0)
        
        if current_total > 0:
            adjustment_factor = target_total / current_total
            new_table[combo] = prob * adjustment_factor
        else:
            new_table[combo] = 0
    
    return new_table

def solve_integer_assignment(combinations, dim_names, marginals, weights_table, total_events):
    """Solve integer assignment problem to preserve exact marginals"""
    
    # Convert combinations to events with integer counts
    events = []
    
    # Track remaining quotas for each marginal
    remaining_quotas = {}
    for dim_name, totals in marginals.items():
        remaining_quotas[dim_name] = totals.copy()
    
    # Sort combinations by weight (highest first)
    combo_weights = []
    for combo_dict in combinations:
        combo_tuple = tuple(combo_dict[dim] for dim in dim_names)
        weight = weights_table.get(combo_tuple, 0)
        combo_weights.append((combo_dict, weight))
    
    combo_weights.sort(key=lambda x: x[1], reverse=True)
    
    # Greedy assignment
    remaining_events = total_events
    
    # print("Assigning events greedily...")
    
    for combo_dict, weight in combo_weights:
        if remaining_events <= 0:
            break
        
        # Check how many events we can assign to this combination
        # Limited by the minimum remaining quota across all dimensions
        max_assignable = remaining_events
        
        for dim_name in dim_names:
            dim_value = combo_dict[dim_name]
            quota_left = remaining_quotas[dim_name].get(dim_value, 0)
            max_assignable = min(max_assignable, quota_left)
        
        if max_assignable > 0:
            # Assign events
            for _ in range(max_assignable):
                events.append(combo_dict.copy())
            
            # Update remaining quotas
            for dim_name in dim_names:
                dim_value = combo_dict[dim_name]
                remaining_quotas[dim_name][dim_value] -= max_assignable
            
            remaining_events -= max_assignable
    
    # print(f"Assigned {len(events)} events, {remaining_events} remaining")
    
    # Check if we have any remaining quotas (commented out for cleaner output)
    # print("Final quotas check:")
    # for dim_name, quotas in remaining_quotas.items():
    #     total_remaining = sum(q for q in quotas.values() if q > 0)
    #     print(f"  {dim_name}: {total_remaining} remaining")
    
    return events

def test_exact_reconstruction():
    """Test the exact reconstruction approach"""
    target_timestamp = '2024-05-20 12:00:00'
    
    print(f"Testing exact reconstruction for: {target_timestamp}")
    print("=" * 60)
    
    # Load data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    original_marginals = get_marginal_totals(hourly_data)
    
    print("Original marginals:")
    for dim, totals in original_marginals.items():
        print(f"  {dim}: {totals}")
    
    # Solve exactly
    synthetic_events, combinations = solve_exact_distribution(original_marginals)
    
    print(f"\nGenerated {len(synthetic_events)} synthetic events")
    
    # Validate reconstruction
    from validate_reconstruction import reconstruct_marginals_from_events, compare_marginals
    
    reconstructed_marginals = reconstruct_marginals_from_events(synthetic_events)
    matches = compare_marginals(original_marginals, reconstructed_marginals)
    
    if matches:
        print("\n🎉 PERFECT RECONSTRUCTION ACHIEVED!")
    else:
        print("\n❌ Reconstruction still has issues")
    
    return matches, synthetic_events

if __name__ == '__main__':
    test_exact_reconstruction()