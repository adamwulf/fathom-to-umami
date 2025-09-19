#!/usr/bin/env python3
"""
Filter synthetic events CSV to only include events from a specific date
"""

import csv
import argparse
from datetime import datetime
from pathlib import Path

def filter_events_by_date(input_csv, output_csv, target_date):
    """Filter events to only include those from the target date"""
    
    # Parse target date
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    events_written = 0
    
    with open(input_csv, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        
        # Write header
        writer.writeheader()
        
        for row in reader:
            # Parse event timestamp
            created_at = row['created_at'].replace('Z', '')
            event_timestamp = datetime.fromisoformat(created_at)
            event_date = event_timestamp.date()
            
            # Check if event is from target date
            if event_date == target_date_obj:
                writer.writerow(row)
                events_written += 1
    
    return events_written

def main():
    parser = argparse.ArgumentParser(description='Filter synthetic events CSV by date')
    parser.add_argument('input_csv', help='Input CSV file')
    parser.add_argument('target_date', help='Target date (YYYY-MM-DD)')
    parser.add_argument('--output', help='Output CSV file (default: filtered_{date}.csv)')
    
    args = parser.parse_args()
    
    if args.output:
        output_csv = args.output
    else:
        output_csv = f"output/filtered_{args.target_date}.csv"
    
    print(f"Filtering events from {args.input_csv} for date {args.target_date}...")
    
    events_count = filter_events_by_date(args.input_csv, output_csv, args.target_date)
    
    print(f"✅ Filtered {events_count} events from {args.target_date}")
    print(f"📁 Output saved to: {output_csv}")
    
    # Show file size
    output_path = Path(output_csv)
    if output_path.exists():
        file_size = output_path.stat().st_size / 1024  # KB
        print(f"💾 File size: {file_size:.1f} KB")

if __name__ == '__main__':
    main()