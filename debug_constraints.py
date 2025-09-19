#!/usr/bin/env python3
"""
Debug the constraint discrepancies
"""

from simple_ipf import load_hourly_data, get_marginal_totals

def debug_constraints():
    """Debug why constraints don't match"""
    target_timestamp = '2024-05-20 12:00:00'
    
    # Load raw data
    hourly_data = load_hourly_data('example.com', target_timestamp)
    
    print("Raw hourly data:")
    for key, rows in hourly_data.items():
        print(f"\n{key} ({len(rows)} entries):")
        for row in rows:
            print(f"  {row}")
    
    # Check totals
    print("\nTotals per dimension:")
    
    # Site total
    site_pageviews = sum(int(row['Pageviews']) for row in hourly_data.get('site', []))
    print(f"Site total pageviews: {site_pageviews}")
    
    # Pages total  
    pages_total = sum(int(row['Views']) for row in hourly_data.get('pages', []))
    print(f"Pages total views: {pages_total}")
    
    # Browsers total
    browsers_total = sum(int(row['Pageviews']) for row in hourly_data.get('browsers', []))
    print(f"Browsers total pageviews: {browsers_total}")
    
    # Countries total
    countries_total = sum(int(row['Pageviews']) for row in hourly_data.get('countries', []))
    print(f"Countries total pageviews: {countries_total}")
    
    # Devices total
    devices_total = sum(int(row['Pageviews']) for row in hourly_data.get('devices', []))
    print(f"Devices total pageviews: {devices_total}")
    
    # Referrers total
    referrers_total = sum(int(row['Views']) for row in hourly_data.get('referrers', []))
    print(f"Referrers total views: {referrers_total}")
    
    print(f"\nExpected: all totals should equal {site_pageviews}")
    
    # Check which totals match
    totals = [pages_total, browsers_total, countries_total, devices_total, referrers_total]
    for i, (name, total) in enumerate(zip(['pages', 'browsers', 'countries', 'devices', 'referrers'], totals)):
        match = "✓" if total == site_pageviews else "✗"
        print(f"  {name}: {total} {match}")

if __name__ == '__main__':
    debug_constraints()