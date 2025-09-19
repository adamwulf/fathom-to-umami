#!/usr/bin/env python3
"""
Simple IPF implementation using only standard library
"""

import csv
import random
from pathlib import Path
from collections import defaultdict
import itertools

def load_hourly_data(website_path, target_timestamp):
    """Load data for a specific timestamp"""
    website_path = Path(website_path)
    
    csv_files = {
        'site': 'Site.csv',
        'pages': 'Pages.csv', 
        'browsers': 'Browsers.csv',
        'countries': 'Countries.csv',
        'devices': 'DeviceTypes.csv',
        'referrers': 'Referrers.csv'
    }
    
    hourly_data = {}
    
    for key, filename in csv_files.items():
        file_path = website_path / filename
        if file_path.exists():
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = [row for row in reader if row['Timestamp'] == target_timestamp]
                hourly_data[key] = rows
    
    return hourly_data

def get_marginal_totals(hourly_data):
    """Extract marginal totals for each dimension"""
    marginals = {}
    
    # Get total pageviews for validation
    def safe_int(value):
        try:
            return int(value) if value and str(value).strip() else 0
        except (ValueError, TypeError):
            return 0
    
    total_pageviews = sum(safe_int(row['Pageviews']) for row in hourly_data.get('site', []))
    
    # Pages marginal
    marginals['pages'] = {}
    for row in hourly_data.get('pages', []):
        path = row['Pathname']
        views = safe_int(row['Views'])
        marginals['pages'][path] = marginals['pages'].get(path, 0) + views
    
    # Browsers marginal
    marginals['browsers'] = {}
    for row in hourly_data.get('browsers', []):
        browser = row['Browser']
        pageviews = safe_int(row['Pageviews'])
        marginals['browsers'][browser] = marginals['browsers'].get(browser, 0) + pageviews
    
    # Countries marginal
    marginals['countries'] = {}
    for row in hourly_data.get('countries', []):
        country = row['Country']
        pageviews = safe_int(row['Pageviews'])
        marginals['countries'][country] = marginals['countries'].get(country, 0) + pageviews
    
    # Devices marginal
    marginals['devices'] = {}
    for row in hourly_data.get('devices', []):
        device = row['Device Type']
        pageviews = safe_int(row['Pageviews'])
        marginals['devices'][device] = marginals['devices'].get(device, 0) + pageviews
    
    # Referrers marginal - handle direct visits
    marginals['referrers'] = {}
    for row in hourly_data.get('referrers', []):
        referrer = row['Referrer Hostname']
        views = safe_int(row['Views'])
        marginals['referrers'][referrer] = marginals['referrers'].get(referrer, 0) + views
    
    # Add direct visits if referrer total is less than total pageviews
    referrer_total = sum(marginals['referrers'].values())
    direct_visits = total_pageviews - referrer_total
    if direct_visits > 0:
        marginals['referrers']['(direct)'] = direct_visits
        # print(f"Added {direct_visits} direct visits to referrers")
    
    return marginals

def create_combination_table(marginals):
    """Create table with all possible combinations"""
    dimensions = {}
    for dim_name, totals in marginals.items():
        if totals:  # Only include dimensions that have data
            dimensions[dim_name] = list(totals.keys())
    
    if not dimensions:
        return {}, []
    
    # Generate all combinations
    dim_names = list(dimensions.keys())
    dim_values = [dimensions[name] for name in dim_names]
    
    combinations = []
    table = {}
    
    for combo in itertools.product(*dim_values):
        combo_dict = dict(zip(dim_names, combo))
        combinations.append(combo_dict)
        table[tuple(combo)] = 1.0  # Initialize uniform
    
    return table, combinations, dim_names

def normalize_table(table):
    """Normalize table so all values sum to 1"""
    total = sum(table.values())
    if total > 0:
        for key in table:
            table[key] /= total
    return table

def apply_marginal_constraint(table, combinations, dim_names, constraint_dim, marginal_totals):
    """Apply constraint for one dimension"""
    if constraint_dim not in dim_names:
        return table
    
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

def simple_ipf(marginals, max_iterations=50, tolerance=1e-6):
    """Simple IPF implementation"""
    table, combinations, dim_names = create_combination_table(marginals)
    
    if not table:
        return table, combinations, dim_names
    
    # Normalize marginals to probabilities
    total_events = max(sum(totals.values()) for totals in marginals.values() if totals)
    normalized_marginals = {}
    for dim_name, totals in marginals.items():
        if totals:
            normalized_marginals[dim_name] = {k: v/total_events for k, v in totals.items()}
    
    # Initialize uniform
    table = normalize_table(table)
    
    print(f"Starting IPF with {len(combinations)} combinations")
    print(f"Dimensions: {dim_names}")
    print(f"Total events to distribute: {total_events}")
    
    for iteration in range(max_iterations):
        old_table = table.copy()
        
        # Apply each marginal constraint
        for dim_name in dim_names:
            if dim_name in normalized_marginals:
                table = apply_marginal_constraint(
                    table, combinations, dim_names, 
                    dim_name, normalized_marginals[dim_name]
                )
        
        # Check convergence
        total_change = sum(abs(table[k] - old_table.get(k, 0)) for k in table)
        print(f"Iteration {iteration + 1}: total change = {total_change:.8f}")
        
        if total_change < tolerance:
            print(f"Converged after {iteration + 1} iterations")
            break
    
    # Scale back to actual event counts
    for key in table:
        table[key] *= total_events
    
    return table, combinations, dim_names

def sample_events_deterministic(table, combinations, dim_names, num_events):
    """Deterministically sample events to exactly match the table counts"""
    if not table or not combinations:
        return []
    
    events = []
    
    # Sort combinations by probability for consistent ordering
    sorted_combos = sorted(table.items(), key=lambda x: x[1], reverse=True)
    
    # Convert probabilities to actual event counts (round to nearest integer)
    remaining_events = int(num_events)
    combo_counts = {}
    
    for combo, prob in sorted_combos:
        count = round(prob)
        if count > remaining_events:
            count = remaining_events
        combo_counts[combo] = count
        remaining_events -= count
        if remaining_events <= 0:
            break
    
    # If we still have remaining events due to rounding, add them to highest probability combinations
    for combo, prob in sorted_combos:
        if remaining_events <= 0:
            break
        if combo in combo_counts:
            combo_counts[combo] += 1
            remaining_events -= 1
    
    # Generate events based on exact counts
    for combo, count in combo_counts.items():
        for _ in range(count):
            event = dict(zip(dim_names, combo))
            events.append(event)
    
    return events

def sample_events(table, combinations, dim_names, num_events):
    """Sample individual events from the probability table"""
    if not table or not combinations:
        return []
    
    # Convert to probability distribution
    total_prob = sum(table.values())
    if total_prob == 0:
        return []
    
    events = []
    
    for _ in range(int(num_events)):
        # Select combination based on probability
        rand_val = random.random() * total_prob
        cumulative = 0
        
        for combo, prob in table.items():
            cumulative += prob
            if rand_val <= cumulative:
                # Convert tuple back to dict
                event = dict(zip(dim_names, combo))
                events.append(event)
                break
    
    return events

def test_simple_hour():
    """Test IPF on a single hour"""
    target_timestamp = '2024-05-20 12:00:00'
    
    print(f"Testing IPF for timestamp: {target_timestamp}")
    print("=" * 50)
    
    # Load data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    
    print("Loaded hourly data:")
    for key, rows in hourly_data.items():
        print(f"  {key}: {len(rows)} entries")
    
    # Get marginals
    marginals = get_marginal_totals(hourly_data)
    
    print("\nMarginal totals:")
    for dim, totals in marginals.items():
        print(f"  {dim}: {totals}")
    
    # Run IPF
    table, combinations, dim_names = simple_ipf(marginals)
    
    if table:
        print(f"\nGenerated probability table with {len(table)} entries")
        
        # Show top combinations
        sorted_table = sorted(table.items(), key=lambda x: x[1], reverse=True)
        print("\nTop 10 combinations:")
        for i, (combo, prob) in enumerate(sorted_table[:10]):
            event = dict(zip(dim_names, combo))
            print(f"  {i+1}: {event} -> {prob:.3f}")
        
        # Sample some events
        total_pageviews = sum(int(row['Pageviews']) for row in hourly_data.get('site', []))
        print(f"\nSampling {total_pageviews} events...")
        
        events = sample_events(table, combinations, dim_names, total_pageviews)
        print(f"Generated {len(events)} events")
        
        # Show sample events
        print("\nSample events:")
        for i, event in enumerate(events[:5]):
            print(f"  {i+1}: {event}")
    
    return table, combinations, dim_names

if __name__ == '__main__':
    random.seed(42)  # For reproducible results
    test_simple_hour()