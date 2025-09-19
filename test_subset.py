#!/usr/bin/env python3
"""
Test converter on a small subset to debug issues
"""

import sys
from pathlib import Path
from fathom_to_umami_converter import FathomToUmamiConverter

def test_small_subset(website_folder='example.com'):
    """Test first 10 hours only"""
    converter = FathomToUmamiConverter(website_folder, 'output/test_subset.csv', verbose=True)
    
    # Get all timestamps
    all_timestamps = converter.get_all_timestamps()
    print(f"Total timestamps: {len(all_timestamps)}")
    
    # Test first 10
    test_timestamps = all_timestamps[:10]
    print(f"Testing first {len(test_timestamps)} timestamps")
    
    all_events = []
    
    for i, timestamp_str in enumerate(test_timestamps):
        print(f"\n--- Processing {i+1}/{len(test_timestamps)}: {timestamp_str} ---")
        try:
            hourly_events = converter.process_hour(timestamp_str)
            all_events.extend(hourly_events)
            print(f"Generated {len(hourly_events)} events")
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            break
    
    # Save results
    if all_events:
        converter.save_to_csv(all_events)
        print(f"\n✅ Successfully processed {len(test_timestamps)} hours")
        print(f"Generated {len(all_events)} total events")
    else:
        print("❌ No events generated")

if __name__ == '__main__':
    website_folder = sys.argv[1] if len(sys.argv) > 1 else 'example.com'
    test_small_subset(website_folder)