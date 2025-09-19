#!/usr/bin/env python3
"""
Simple test script to verify data loading without external dependencies
"""

import csv
import sys
from pathlib import Path
from datetime import datetime

def test_basic_loading(website_folder='example.com'):
    """Test basic CSV loading using only standard library"""
    website_path = Path(website_folder)
    
    csv_files = {
        'site': 'Site.csv',
        'pages': 'Pages.csv', 
        'browsers': 'Browsers.csv',
        'countries': 'Countries.csv',
        'devices': 'DeviceTypes.csv',
        'referrers': 'Referrers.csv',
        'events': 'Events.csv'
    }
    
    data = {}
    
    for key, filename in csv_files.items():
        file_path = website_path / filename
        if file_path.exists():
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                data[key] = rows
                print(f"Loaded {filename}: {len(rows)} rows")
                
                # Show first few rows for inspection
                if rows and len(rows) > 0:
                    print(f"  Columns: {list(rows[0].keys())}")
                    print(f"  Sample: {rows[0]}")
                print()
        else:
            print(f"File not found: {filename}")
            data[key] = []
    
    return data

def analyze_data_structure(data):
    """Analyze the data to understand constraints"""
    print("=== Data Structure Analysis ===")
    
    # Check timestamp coverage
    all_timestamps = set()
    for key, rows in data.items():
        if rows and 'Timestamp' in rows[0]:
            for row in rows:
                all_timestamps.add(row['Timestamp'])
    
    print(f"Total unique timestamps: {len(all_timestamps)}")
    if all_timestamps:
        timestamps_sorted = sorted(all_timestamps)
        print(f"Date range: {timestamps_sorted[0]} to {timestamps_sorted[-1]}")
    
    # Check data for a specific hour
    if all_timestamps:
        sample_timestamp = list(all_timestamps)[0]
        print(f"\nSample hour analysis for: {sample_timestamp}")
        
        for key, rows in data.items():
            if rows and 'Timestamp' in rows[0]:
                hour_data = [row for row in rows if row['Timestamp'] == sample_timestamp]
                print(f"  {key}: {len(hour_data)} entries")
                if hour_data:
                    print(f"    Sample entry: {hour_data[0]}")

if __name__ == '__main__':
    website_folder = sys.argv[1] if len(sys.argv) > 1 else 'example.com'
    data = test_basic_loading(website_folder)
    analyze_data_structure(data)