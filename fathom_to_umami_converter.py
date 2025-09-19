#!/usr/bin/env python3
"""
Production Fathom to Umami Converter
Complete pipeline for converting Fathom analytics exports to Umami format
"""

import uuid
import csv
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Import our modules
from exact_ipf import solve_exact_distribution
from simple_ipf import load_hourly_data, get_marginal_totals

class FathomToUmamiConverter:
    """Main converter class"""
    
    def __init__(self, website_path, output_path, verbose=False, debug_date=None):
        self.website_path = Path(website_path)
        self.output_path = Path(output_path)
        self.verbose = verbose
        self.debug_date = debug_date  # Format: "YYYY-MM-DD"
        self.website_id = str(uuid.uuid4())
        self.stats = {
            'total_hours': 0,
            'total_events': 0,
            'total_pageviews': 0,
            'total_custom_events': 0,
            'skipped_hours': 0
        }
        self.timing = {
            'preloading': 0,
            'data_loading': 0,
            'ipf_solving': 0,
            'session_modeling': 0,
            'csv_writing': 0,
            'other': 0
        }
        # Pre-loaded data indexed by timestamp
        self.indexed_data = None
    
    def log(self, message):
        """Log message if verbose mode enabled"""
        if self.verbose:
            print(message)
    
    def preload_all_data(self):
        """Pre-load all CSV files and index by timestamp for fast lookups"""
        print("📁 Pre-loading all CSV data...")
        start_time = time.time()
        
        csv_files = {
            'site': 'Site.csv',
            'pages': 'Pages.csv', 
            'browsers': 'Browsers.csv',
            'countries': 'Countries.csv',
            'devices': 'DeviceTypes.csv',
            'referrers': 'Referrers.csv',
            'events': 'Events.csv'
        }
        
        # Load all data
        all_data = {}
        for key, filename in csv_files.items():
            file_path = self.website_path / filename
            if file_path.exists():
                with open(file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    all_data[key] = rows
                    print(f"  Loaded {filename}: {len(rows)} rows")
            else:
                print(f"  File not found: {filename}")
                all_data[key] = []
        
        # Create timestamp index
        self.indexed_data = defaultdict(lambda: defaultdict(list))
        
        for data_type, rows in all_data.items():
            for row in rows:
                if 'Timestamp' in row and row['Timestamp']:
                    timestamp = row['Timestamp']
                    self.indexed_data[timestamp][data_type].append(row)
        
        load_time = time.time() - start_time
        total_timestamps = len(self.indexed_data)
        print(f"  ✅ Indexed {total_timestamps} unique timestamps in {load_time:.2f}s")
        
        return load_time
    
    def get_hourly_data_fast(self, timestamp_str):
        """Fast lookup of hourly data from pre-loaded index"""
        if self.indexed_data is None:
            raise RuntimeError("Data not pre-loaded. Call preload_all_data() first.")
        
        return dict(self.indexed_data.get(timestamp_str, {}))
    
    def convert_website(self):
        """Convert entire website"""
        print(f"Converting {self.website_path.name} to Umami format...")
        
        # Pre-load all data once
        preload_time = self.preload_all_data()
        self.timing['preloading'] = preload_time
        
        # Get timestamps from indexed data
        all_timestamps = sorted(self.indexed_data.keys())
        
        # Filter for debug date if specified
        if self.debug_date:
            debug_timestamps = [ts for ts in all_timestamps if ts.startswith(self.debug_date)]
            print(f"🐛 DEBUG MODE: Processing only {self.debug_date}")
            print(f"Found {len(debug_timestamps)} hourly records for {self.debug_date} (of {len(all_timestamps)} total)")
            all_timestamps = debug_timestamps
        else:
            print(f"Found {len(all_timestamps)} hourly records to process")
        
        if not all_timestamps:
            print(f"❌ No records found{' for date ' + self.debug_date if self.debug_date else ''}")
            return
        
        # Initialize CSV file with header
        start_time = time.time()
        csv_file, writer = self.init_csv_file()
        self.timing['csv_writing'] += time.time() - start_time
        
        last_progress_percent = -1
        
        try:
            for i, timestamp_str in enumerate(all_timestamps):
                # Process hour
                hourly_events = self.process_hour(timestamp_str)
                
                # Write events immediately
                start_time = time.time()
                self.write_events_to_csv(writer, hourly_events)
                self.timing['csv_writing'] += time.time() - start_time
                
                self.stats['total_hours'] += 1
                
                # Progress reporting - only when % changes
                current_percent = int((i+1)/len(all_timestamps)*100)
                if current_percent != last_progress_percent:
                    events_written = self.stats['total_events']
                    print(f"Progress: {current_percent}% ({i+1}/{len(all_timestamps)}) - {events_written:,} events")
                    last_progress_percent = current_percent
        finally:
            csv_file.close()
        
        self.print_summary()
    
    def get_all_timestamps(self):
        """Get all unique timestamps from Site.csv"""
        site_file = self.website_path / 'Site.csv'
        if not site_file.exists():
            raise FileNotFoundError(f"Site.csv not found in {self.website_path}")
        
        timestamps = []
        with open(site_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                timestamps.append(row['Timestamp'])
        
        return sorted(set(timestamps))
    
    def process_hour(self, timestamp_str):
        """Process a single hour of data"""
        self.log(f"Processing {timestamp_str}")
        
        # Get hourly data from pre-loaded index (very fast!)
        start_time = time.time()
        hourly_data = self.get_hourly_data_fast(timestamp_str)
        self.timing['data_loading'] += time.time() - start_time
        
        # Check if we have data
        site_data = hourly_data.get('site', [])
        if not site_data:
            self.log(f"  No site data for {timestamp_str}")
            self.stats['skipped_hours'] += 1
            return []
        
        site_row = site_data[0]
        
        def safe_int(value, default=0):
            try:
                return int(value) if value and value.strip() else default
            except (ValueError, AttributeError):
                return default
        
        def safe_float(value, default=0.0):
            try:
                return float(value) if value and value.strip() else default
            except (ValueError, AttributeError):
                return default
        
        total_pageviews = safe_int(site_row.get('Pageviews'))
        total_visits = safe_int(site_row.get('Visits'))
        bounce_rate = safe_float(site_row.get('Bounce Rate'))
        avg_duration = safe_float(site_row.get('Avg Duration'))
        
        if total_pageviews == 0:
            self.log(f"  No pageviews for {timestamp_str}")
            self.stats['skipped_hours'] += 1
            return []
        
        # Handle case where visits is 0 but pageviews > 0
        if total_visits == 0:
            total_visits = 1  # Assume at least 1 visit if there are pageviews
            bounce_rate = 1.0  # All are bounces
        
        self.log(f"  {total_pageviews} pageviews, {total_visits} visits")
        
        # Generate synthetic events
        try:
            start_time = time.time()
            events = self.generate_hour_events(
                hourly_data, timestamp_str, total_pageviews, 
                total_visits, bounce_rate, avg_duration
            )
            # Note: Individual timing is tracked within generate_hour_events
            
            self.stats['total_events'] += len(events)
            self.stats['total_pageviews'] += sum(1 for e in events if e['event_type'] == 1)
            self.stats['total_custom_events'] += sum(1 for e in events if e['event_type'] == 2)
            
            return events
            
        except Exception as e:
            print(f"Error processing {timestamp_str}: {e}")
            self.stats['skipped_hours'] += 1
            return []
    
    def generate_hour_events(self, hourly_data, timestamp_str, total_pageviews, 
                           total_visits, bounce_rate, avg_duration):
        """Generate events for a single hour"""
        
        # Get marginal constraints
        start_time = time.time()
        marginals = get_marginal_totals(hourly_data)
        self.timing['other'] += time.time() - start_time
        
        # Generate base pageview events using exact IPF
        start_time = time.time()
        base_events, _ = solve_exact_distribution(marginals)
        self.timing['ipf_solving'] += time.time() - start_time
        
        # Create sessions and visits
        start_time = time.time()
        base_timestamp = datetime.fromisoformat(timestamp_str)
        pageview_events = self.create_session_visits(
            base_events, total_visits, bounce_rate, avg_duration, base_timestamp
        )
        self.timing['session_modeling'] += time.time() - start_time
        
        # Add custom events
        start_time = time.time()
        custom_events = self.process_custom_events(
            hourly_data.get('events', []), base_timestamp
        )
        self.timing['other'] += time.time() - start_time
        
        return pageview_events + custom_events
    
    def create_session_visits(self, events, visit_count, bounce_rate, avg_duration, base_timestamp):
        """Create realistic sessions and visits from events"""
        if not events:
            return []
        
        # Calculate visit distribution
        bounced_visits = round(visit_count * bounce_rate)
        multi_page_visits = visit_count - bounced_visits
        
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
        
        # Add session/visit metadata
        enhanced_events = []
        
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
                
                # Create Umami event
                enhanced_event = {
                    'website_id': self.website_id,
                    'session_id': session_id,
                    'visit_id': visit_id,
                    'event_id': str(uuid.uuid4()),
                    
                    # Session data
                    'hostname': self.infer_hostname(event),
                    'browser': event['browsers'],
                    'os': self.infer_os(event['browsers']),
                    'device': event['devices'],
                    'screen': self.infer_screen(event['devices']),
                    'language': 'en-US',
                    'country': event['countries'],
                    'region': '',
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
                    'created_at': event_time.isoformat() + 'Z'
                }
                
                enhanced_events.append(enhanced_event)
        
        return enhanced_events
    
    def process_custom_events(self, events_data, base_timestamp):
        """Process custom events from Events.csv"""
        custom_events = []
        
        for row in events_data:
            if row['Timestamp'] == base_timestamp.strftime('%Y-%m-%d %H:%M:%S'):
                completions = int(row['Completions'])
                for _ in range(completions):
                    custom_event = {
                        'website_id': self.website_id,
                        'session_id': str(uuid.uuid4()),
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
    
    def infer_hostname(self, event):
        """Infer hostname from website folder name"""
        website_name = self.website_path.name
        # If folder name looks like a domain, use it directly
        if '.' in website_name:
            return f'https://{website_name}'
        # Otherwise assume it's a simple name and add .com
        return f'https://{website_name}.com'
    
    def infer_os(self, browser):
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
    
    def infer_screen(self, device):
        """Infer screen resolution from device type"""
        device_lower = device.lower()
        if 'desktop' in device_lower:
            return '1920x1080'
        elif 'phone' in device_lower:
            return '390x844'
        elif 'tablet' in device_lower:
            return '768x1024'
        return '1920x1080'
    
    def init_csv_file(self):
        """Initialize CSV file and return file handle and writer"""
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
        
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        csv_file = open(self.output_path, 'w', newline='')
        writer = csv.DictWriter(csv_file, fieldnames=umami_fields)
        writer.writeheader()
        
        return csv_file, writer
    
    def write_events_to_csv(self, writer, events):
        """Write events to CSV incrementally"""
        if not events:
            return
        
        umami_fields = writer.fieldnames
        
        for event in events:
            # Ensure all fields are present
            row = {}
            for field in umami_fields:
                row[field] = event.get(field, None)
            writer.writerow(row)
        
        # print(f"✅ Saved {len(events)} events to {self.output_path}")
    
    def print_summary(self):
        """Print conversion summary"""
        print(f"\n✅ Conversion completed successfully!")
        print(f"📁 Output file: {self.output_path}")
        print(f"📊 Total events: {self.stats['total_events']:,}")
        print(f"📊 Hours processed: {self.stats['total_hours']:,}")
        
        # Show file size
        if self.output_path.exists():
            file_size = self.output_path.stat().st_size / (1024 * 1024)  # MB
            print(f"💾 File size: {file_size:.1f} MB")
        
        # Print timing
        total_time = sum(self.timing.values())
        if total_time > 0:
            print(f"⏱️  Total time: {total_time:.2f}s")

def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(
        description='Convert Fathom Analytics data to Umami format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available dates
  python3 fathom_to_umami_converter.py example.com output/test.csv --list-dates

  # Debug mode: process only one date (fast!)
  python3 fathom_to_umami_converter.py example.com output/debug.csv --debug-date 2024-01-15 --verbose

  # Convert website data (full dataset)
  python3 fathom_to_umami_converter.py mywebsite.com output/mywebsite.csv

  # Convert with verbose output
  python3 fathom_to_umami_converter.py example.com output/example.csv --verbose
        """
    )
    
    parser.add_argument('website_path', type=Path,
                       help='Path to website CSV export folder (e.g., example.com)')
    parser.add_argument('output_path', type=Path, 
                       help='Output CSV file path (e.g., output/website.csv)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    parser.add_argument('--debug-date', '-d', type=str,
                       help='Debug mode: process only a single date (format: YYYY-MM-DD)')
    parser.add_argument('--list-dates', '-l', action='store_true',
                       help='List available dates and exit')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.website_path.exists():
        print(f"❌ Error: Website path not found: {args.website_path}")
        return 1
    
    site_file = args.website_path / 'Site.csv'
    if not site_file.exists():
        print(f"❌ Error: Site.csv not found in {args.website_path}")
        return 1
    
    # Handle list dates option
    if args.list_dates:
        converter = FathomToUmamiConverter(args.website_path, args.output_path, args.verbose)
        converter.preload_all_data()
        timestamps = sorted(converter.indexed_data.keys())
        dates = sorted(set(ts[:10] for ts in timestamps))  # Extract YYYY-MM-DD
        
        print(f"Available dates in {args.website_path.name}:")
        print(f"Total: {len(dates)} unique dates")
        print(f"Range: {dates[0]} to {dates[-1]}")
        print(f"\nFirst 10 dates:")
        for date in dates[:10]:
            hourly_count = sum(1 for ts in timestamps if ts.startswith(date))
            print(f"  {date}: {hourly_count} hours")
        
        if len(dates) > 10:
            print(f"  ... and {len(dates) - 10} more dates")
        
        return 0
    
    # Run conversion
    converter = FathomToUmamiConverter(args.website_path, args.output_path, args.verbose, args.debug_date)
    
    try:
        converter.convert_website()
        print(f"\n🎉 Conversion complete! Output saved to {args.output_path}")
        return 0
    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == '__main__':
    exit(main())