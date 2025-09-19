#!/usr/bin/env python3
"""
Full pipeline from Fathom data to Umami format
"""

import uuid
import csv
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from exact_ipf import solve_exact_distribution
from simple_ipf import load_hourly_data, get_marginal_totals

def create_session_visits(events, visit_count, bounce_rate, avg_duration, base_timestamp):
    """Create realistic sessions and visits from events"""
    if not events:
        return []
    
    # Calculate visit distribution
    bounced_visits = round(visit_count * bounce_rate)
    multi_page_visits = visit_count - bounced_visits
    
    print(f"Session modeling:")
    print(f"  Target visits: {visit_count}")
    print(f"  Bounced visits (1 page): {bounced_visits}")
    print(f"  Multi-page visits: {multi_page_visits}")
    
    # Create visit structure
    visits = []
    events_copy = events.copy()
    
    # Create bounced visits (1 event each)
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
    
    print(f"  Created {len(visits)} visits with sizes: {[len(v) for v in visits]}")
    
    # Add session/visit metadata
    enhanced_events = []
    website_id = str(uuid.uuid4())
    
    for visit_idx, visit in enumerate(visits):
        session_id = str(uuid.uuid4())
        visit_id = str(uuid.uuid4())
        
        # Calculate visit duration and timing
        visit_duration = avg_duration if len(visits) == 1 else (avg_duration * len(visit) / len(events))
        
        # Distribute events within the hour
        time_spread = 3600  # 1 hour in seconds
        visit_start_offset = (visit_idx / len(visits)) * time_spread if len(visits) > 1 else 0
        
        for event_idx, event in enumerate(visit):
            # Calculate event timing
            if len(visit) > 1:
                event_offset = visit_start_offset + (event_idx * visit_duration / len(visit))
            else:
                event_offset = visit_start_offset
            
            event_time = base_timestamp + timedelta(seconds=event_offset)
            
            # Create enhanced event
            enhanced_event = {
                'website_id': website_id,
                'session_id': session_id,
                'visit_id': visit_id,
                'event_id': str(uuid.uuid4()),
                
                # Session data
                'hostname': event.get('hostname', 'https://example.com'),
                'browser': event['browsers'],
                'os': infer_os(event['browsers']),
                'device': event['devices'],
                'screen': infer_screen(event['devices']),
                'language': 'en-US',
                'country': event['countries'],
                'region': '',  # Could be enhanced with state data
                'city': None,
                
                # Pageview data
                'url_path': event['pages'],
                'url_query': None,
                'utm_source': None,
                'utm_medium': None,
                'utm_campaign': None,
                'utm_content': None,
                'utm_term': None,
                'referrer_path': '/' if event['referrers'] != '(direct)' else None,
                'referrer_query': None,
                'referrer_domain': event['referrers'] if event['referrers'] != '(direct)' else None,
                'page_title': None,
                
                # Click IDs
                'gclid': None,
                'fbclid': None,
                'msclkid': None,
                'ttclid': None,
                'li_fat_id': None,
                'twclid': None,
                
                # Event data
                'event_type': 1,  # 1=pageview
                'event_name': 'pageview',
                'tag': None,
                'distinct_id': None,
                'created_at': event_time.isoformat() + 'Z',
                
                # Metadata
                'visit_page_number': event_idx + 1,
                'is_bounce': len(visit) == 1,
                'visit_duration_estimate': visit_duration
            }
            
            enhanced_events.append(enhanced_event)
    
    return enhanced_events

def infer_os(browser):
    """Infer OS from browser"""
    browser_lower = browser.lower()
    if 'safari' in browser_lower:
        return 'macOS'
    elif 'chrome' in browser_lower:
        return 'Windows'
    elif 'firefox' in browser_lower:
        return 'Linux'
    elif 'edge' in browser_lower:
        return 'Windows'
    return 'Unknown'

def infer_screen(device):
    """Infer screen resolution from device type"""
    device_lower = device.lower()
    if 'desktop' in device_lower:
        return '1920x1080'
    elif 'phone' in device_lower:
        return '390x844'
    elif 'tablet' in device_lower:
        return '768x1024'
    return '1920x1080'

def process_custom_events(events_data, base_timestamp):
    """Process custom events from Events.csv"""
    custom_events = []
    
    for row in events_data:
        if row['Timestamp'] == base_timestamp.strftime('%Y-%m-%d %H:%M:%S'):
            completions = int(row['Completions'])
            for _ in range(completions):
                custom_event = {
                    'website_id': str(uuid.uuid4()),  # Would use same as pageviews
                    'session_id': str(uuid.uuid4()),  # Could correlate with pageview sessions
                    'visit_id': str(uuid.uuid4()),
                    'event_id': str(uuid.uuid4()),
                    
                    # Default session data for custom events
                    'hostname': '',
                    'browser': '',
                    'os': '',
                    'device': '',
                    'screen': '',
                    'language': '',
                    'country': '',
                    'region': '',
                    'city': None,
                    
                    # No pageview data
                    'url_path': None,
                    'url_query': None,
                    'utm_source': None,
                    'utm_medium': None,
                    'utm_campaign': None,
                    'utm_content': None,
                    'utm_term': None,
                    'referrer_path': None,
                    'referrer_query': None,
                    'referrer_domain': None,
                    'page_title': None,
                    
                    # Click IDs
                    'gclid': None,
                    'fbclid': None,
                    'msclkid': None,
                    'ttclid': None,
                    'li_fat_id': None,
                    'twclid': None,
                    
                    # Custom event data
                    'event_type': 2,  # 2=custom event
                    'event_name': row['Event Name'],
                    'tag': row['Event Code'],
                    'distinct_id': None,
                    'created_at': base_timestamp.isoformat() + 'Z'
                }
                custom_events.append(custom_event)
    
    return custom_events

def test_full_hour_conversion():
    """Test complete conversion for one hour"""
    target_timestamp = '2024-05-20 12:00:00'
    print(f"Testing full conversion for: {target_timestamp}")
    print("=" * 60)
    
    # Load hourly data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    
    # Get constraints
    original_marginals = get_marginal_totals(hourly_data)
    site_data = hourly_data.get('site', [{}])[0]
    
    total_pageviews = int(site_data.get('Pageviews', 0))
    total_visits = int(site_data.get('Visits', 0))
    bounce_rate = float(site_data.get('Bounce Rate', 0))
    avg_duration = float(site_data.get('Avg Duration', 0))
    
    print(f"Constraints:")
    print(f"  Pageviews: {total_pageviews}")
    print(f"  Visits: {total_visits}")
    print(f"  Bounce Rate: {bounce_rate:.3f}")
    print(f"  Avg Duration: {avg_duration:.1f}s")
    
    # Generate base events
    print(f"\nGenerating synthetic events...")
    base_events, _ = solve_exact_distribution(original_marginals)
    
    # Create sessions and visits
    print(f"\nCreating sessions and visits...")
    base_timestamp = datetime.fromisoformat(target_timestamp)
    enhanced_events = create_session_visits(
        base_events, total_visits, bounce_rate, avg_duration, base_timestamp
    )
    
    # Process custom events (if any)
    custom_events = process_custom_events(hourly_data.get('events', []), base_timestamp)
    
    # Combine all events
    all_events = enhanced_events + custom_events
    
    print(f"\nGenerated {len(enhanced_events)} pageview events and {len(custom_events)} custom events")
    print(f"Total: {len(all_events)} events")
    
    # Show samples
    print(f"\nSample events:")
    for i, event in enumerate(all_events[:3]):
        print(f"  {i+1}: {event['event_name']} - {event['url_path']} - {event['country']} - {event['browser']}")
    
    return all_events

def save_to_umami_csv(events, output_path):
    """Save events to Umami-compatible CSV"""
    if not events:
        print("No events to save")
        return
    
    # Define Umami schema fields in order
    umami_fields = [
        'website_id', 'session_id', 'visit_id', 'event_id',
        'hostname', 'browser', 'os', 'device', 'screen', 'language', 
        'country', 'region', 'city',
        'url_path', 'url_query', 'utm_source', 'utm_medium', 'utm_campaign', 
        'utm_content', 'utm_term', 'referrer_path', 'referrer_query', 
        'referrer_domain', 'page_title',
        'gclid', 'fbclid', 'msclkid', 'ttclid', 'li_fat_id', 'twclid',
        'event_type', 'event_name', 'tag', 'distinct_id', 'created_at'
    ]
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=umami_fields)
        writer.writeheader()
        
        for event in events:
            # Ensure all fields are present
            row = {}
            for field in umami_fields:
                row[field] = event.get(field, None)
            writer.writerow(row)
    
    print(f"Saved {len(events)} events to {output_path}")

def test_csv_output():
    """Test the complete pipeline including CSV output"""
    events = test_full_hour_conversion()
    
    if events:
        output_path = 'output/example_sample.csv'
        save_to_umami_csv(events, output_path)
        
        # Validate CSV structure
        with open(output_path, 'r') as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            print(f"\nCSV validation:")
            print(f"  Columns: {len(first_row)}")
            print(f"  Sample row keys: {list(first_row.keys())[:5]}...")
    
    return events

if __name__ == '__main__':
    test_csv_output()