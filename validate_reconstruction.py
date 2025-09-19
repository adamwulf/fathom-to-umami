#!/usr/bin/env python3
"""
Validation module to verify synthetic events reconstruct to original data
"""

from simple_ipf import load_hourly_data, get_marginal_totals, simple_ipf, sample_events_deterministic
from collections import defaultdict

def reconstruct_marginals_from_events(events):
    """Reconstruct marginal totals from synthetic events"""
    reconstructed = {
        'pages': defaultdict(int),
        'browsers': defaultdict(int),
        'countries': defaultdict(int),
        'devices': defaultdict(int),
        'referrers': defaultdict(int)
    }
    
    for event in events:
        reconstructed['pages'][event['pages']] += 1
        reconstructed['browsers'][event['browsers']] += 1
        reconstructed['countries'][event['countries']] += 1
        reconstructed['devices'][event['devices']] += 1
        reconstructed['referrers'][event['referrers']] += 1
    
    # Convert to regular dicts
    return {k: dict(v) for k, v in reconstructed.items()}

def compare_marginals(original, reconstructed):
    """Compare original and reconstructed marginals"""
    print("Validation Results:")
    print("=" * 50)
    
    all_match = True
    
    for dim in ['pages', 'browsers', 'countries', 'devices', 'referrers']:
        print(f"\n{dim.capitalize()}:")
        
        orig_dict = original.get(dim, {})
        recon_dict = reconstructed.get(dim, {})
        
        # Get all unique keys
        all_keys = set(orig_dict.keys()) | set(recon_dict.keys())
        
        matches = True
        for key in sorted(all_keys):
            orig_val = orig_dict.get(key, 0)
            recon_val = recon_dict.get(key, 0)
            
            match_str = "✓" if orig_val == recon_val else "✗"
            if orig_val != recon_val:
                matches = False
                all_match = False
            
            print(f"  {key}: {orig_val} → {recon_val} {match_str}")
        
        if matches:
            print(f"  {dim}: PERFECT MATCH ✓")
        else:
            print(f"  {dim}: MISMATCH ✗")
    
    print(f"\nOverall result: {'PERFECT RECONSTRUCTION ✓' if all_match else 'RECONSTRUCTION FAILED ✗'}")
    return all_match

def test_reconstruction_accuracy():
    """Test that synthetic events perfectly reconstruct original data"""
    target_timestamp = '2024-05-20 12:00:00'
    
    print(f"Testing reconstruction accuracy for: {target_timestamp}")
    print("=" * 60)
    
    # Load original data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    original_marginals = get_marginal_totals(hourly_data)
    
    print("Original marginals:")
    for dim, totals in original_marginals.items():
        print(f"  {dim}: {totals}")
    
    # Generate synthetic events
    table, combinations, dim_names = simple_ipf(original_marginals)
    total_pageviews = sum(int(row['Pageviews']) for row in hourly_data.get('site', []))
    synthetic_events = sample_events_deterministic(table, combinations, dim_names, total_pageviews)
    
    print(f"\nGenerated {len(synthetic_events)} synthetic events")
    
    # Reconstruct marginals from synthetic events
    reconstructed_marginals = reconstruct_marginals_from_events(synthetic_events)
    
    # Compare
    matches = compare_marginals(original_marginals, reconstructed_marginals)
    
    return matches, synthetic_events

def add_session_modeling(events, visit_count, bounce_rate, avg_duration):
    """Add session and visit modeling to events"""
    if not events:
        return []
    
    # Group events into visits
    visits = []
    bounced_visits = int(visit_count * bounce_rate)
    multi_page_visits = visit_count - bounced_visits
    
    print(f"\nSession modeling:")
    print(f"  Target visits: {visit_count}")
    print(f"  Bounced visits (1 page): {bounced_visits}")
    print(f"  Multi-page visits: {multi_page_visits}")
    print(f"  Average duration: {avg_duration} seconds")
    
    # Create bounced visits (1 event each)
    events_copy = events.copy()
    for i in range(bounced_visits):
        if events_copy:
            visit = [events_copy.pop(0)]
            visits.append(visit)
    
    # Distribute remaining events across multi-page visits
    if multi_page_visits > 0 and events_copy:
        events_per_visit = len(events_copy) // multi_page_visits
        remainder = len(events_copy) % multi_page_visits
        
        for i in range(multi_page_visits):
            visit_size = events_per_visit + (1 if i < remainder else 0)
            visit = []
            for j in range(visit_size):
                if events_copy:
                    visit.append(events_copy.pop(0))
            if visit:
                visits.append(visit)
    
    print(f"  Created {len(visits)} visits")
    print(f"  Visit sizes: {[len(v) for v in visits]}")
    
    # Add session metadata
    enhanced_events = []
    import uuid
    from datetime import datetime, timedelta
    
    session_id = str(uuid.uuid4())
    base_time = datetime.fromisoformat('2024-05-20 12:00:00')
    
    for visit_idx, visit in enumerate(visits):
        visit_id = str(uuid.uuid4())
        
        # Distribute visit duration
        visit_duration = avg_duration if len(visits) == 1 else avg_duration * (len(visit) / len(events))
        time_per_event = visit_duration / len(visit) if len(visit) > 1 else 0
        
        for event_idx, event in enumerate(visit):
            # Add timing within the hour
            event_time = base_time + timedelta(seconds=event_idx * time_per_event)
            
            enhanced_event = event.copy()
            enhanced_event.update({
                'session_id': session_id,
                'visit_id': visit_id,
                'event_id': str(uuid.uuid4()),
                'event_time': event_time,
                'visit_page_number': event_idx + 1,
                'is_bounce': len(visit) == 1
            })
            enhanced_events.append(enhanced_event)
    
    return enhanced_events

def test_full_pipeline():
    """Test the complete pipeline including session modeling"""
    target_timestamp = '2024-05-20 12:00:00'
    
    print("FULL PIPELINE TEST")
    print("=" * 60)
    
    # Load original data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    original_marginals = get_marginal_totals(hourly_data)
    
    # Get site-level constraints
    site_data = hourly_data.get('site', [{}])[0]
    total_pageviews = int(site_data.get('Pageviews', 0))
    total_visits = int(site_data.get('Visits', 0))
    bounce_rate = float(site_data.get('Bounce Rate', 0))
    avg_duration = float(site_data.get('Avg Duration', 0))
    
    print(f"Site constraints:")
    print(f"  Pageviews: {total_pageviews}")
    print(f"  Visits: {total_visits}")
    print(f"  Bounce Rate: {bounce_rate:.3f}")
    print(f"  Avg Duration: {avg_duration:.1f}s")
    
    # Generate base events
    table, combinations, dim_names = simple_ipf(original_marginals)
    base_events = sample_events_deterministic(table, combinations, dim_names, total_pageviews)
    
    # Add session modeling
    enhanced_events = add_session_modeling(base_events, total_visits, bounce_rate, avg_duration)
    
    # Validate visit structure
    visits_by_id = defaultdict(list)
    for event in enhanced_events:
        visits_by_id[event['visit_id']].append(event)
    
    actual_visits = len(visits_by_id)
    actual_bounces = sum(1 for visit in visits_by_id.values() if len(visit) == 1)
    actual_bounce_rate = actual_bounces / actual_visits if actual_visits > 0 else 0
    
    print(f"\nValidation:")
    print(f"  Target visits: {total_visits}, Actual: {actual_visits} {'✓' if actual_visits == total_visits else '✗'}")
    print(f"  Target bounce rate: {bounce_rate:.3f}, Actual: {actual_bounce_rate:.3f} {'✓' if abs(bounce_rate - actual_bounce_rate) < 0.1 else '✗'}")
    
    # Show sample enhanced events
    print(f"\nSample enhanced events:")
    for i, event in enumerate(enhanced_events[:3]):
        print(f"  {i+1}: {event}")
    
    return enhanced_events

if __name__ == '__main__':
    # Test reconstruction accuracy
    print("TESTING RECONSTRUCTION ACCURACY")
    matches, events = test_reconstruction_accuracy()
    
    if matches:
        print("\n" + "="*60)
        # Test full pipeline with session modeling
        enhanced_events = test_full_pipeline()
    else:
        print("❌ Reconstruction failed - cannot proceed to session modeling")