# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains a complete Fathom Analytics to Umami converter. The project transforms Fathom's aggregated CSV exports into synthetic raw event data compatible with Umami's import format.

### Data Sources
- Any Fathom Analytics CSV export folder (named as your domain, e.g., `example.com/`)
- Sample data provided in `example.com/` folder for testing

### Key Files
- `fathom_to_umami_converter.py` - Main production converter
- `exact_ipf.py` - IPF algorithm for constraint satisfaction  
- `simple_ipf.py` - Base data loading and IPF functions
- `validate_reconstruction.py` - Validation framework
- `README.md` - Complete project documentation

## Data Structure

Each Fathom export contains:
- `Site.csv` - Hourly totals (Pageviews, Visits, Avg Duration, Bounce Rate)
- `Pages.csv` - Page-level views by hour
- `Browsers.csv` - Browser breakdown by hour  
- `Countries.csv` - Geographic data by hour
- `DeviceTypes.csv` - Device type breakdown by hour
- `Referrers.csv` - Traffic source data by hour
- `Events.csv` - Custom events (may be empty)

## Conversion Process

### Algorithm: Iterative Proportional Fitting (IPF)
1. Load hourly constraints from all CSV files
2. Handle direct visits (no referrer) by adding synthetic "(direct)" category
3. Solve 5D contingency table reconstruction to satisfy all marginal constraints
4. Generate synthetic events that exactly reproduce original summaries
5. Model realistic sessions/visits with proper bounce rates and durations
6. Output Umami-compatible CSV with full schema

### Mathematical Guarantee
The synthetic events **exactly reconstruct** the original Fathom summaries when re-aggregated. This is ensured by the IPF algorithm's constraint satisfaction properties.

## Development Commands

```bash
# List available dates
python3 fathom_to_umami_converter.py example.com --list-dates

# Quick debug: process single date (FAST!)
python3 fathom_to_umami_converter.py example.com --debug-date 2024-01-15 --verbose

# Run full conversion
python3 fathom_to_umami_converter.py mywebsite.com

# Test small subset (for development)
python3 test_subset.py example.com

# Validate reconstruction accuracy
python3 validate_reconstruction.py

# Test end-to-end pipeline
python3 full_pipeline.py
```

## Key Implementation Details

### Data Handling
- Robust CSV parsing with empty value handling
- Safe integer/float conversion for malformed data
- Automatic handling of missing referrer data (direct visits)

### Session Modeling  
- Distributes pageviews across visits respecting visit counts
- Applies bounce rates (single vs multi-page visits)
- Models session durations using realistic time distributions
- Generates UUIDs for sessions, visits, and events

### Output Format
- Complete Umami schema with 35 fields
- Proper timestamp formatting (ISO 8601 with Z suffix)
- Device/OS inference from browser data
- Screen resolution mapping from device types

## Validation Framework

The converter includes comprehensive validation:
- Re-aggregates synthetic data to verify exact match with originals
- Tests constraint satisfaction across all dimensions  
- Validates visit structure and bounce rate preservation
- Ensures temporal distribution within hourly windows

```bash
# Verify conversion accuracy (generic verification)
python3 verify_reconstruction.py mywebsite.com
```

## Usage Example

### Converting Your Data

1. **Export from Fathom:** Download your CSV export from Fathom Analytics
2. **Create folder:** Name it after your domain (e.g., `mywebsite.com/`)
3. **Place CSVs:** Put all CSV files inside the folder
4. **Run converter:** `python3 fathom_to_umami_converter.py mywebsite.com`

### Expected Performance

Typical conversion statistics:
- **Efficiency:** 99-101% (synthetic events closely match original pageviews)
- **Processing time:** ~30 seconds per 25k hourly records
- **File size:** ~3MB per 1k synthetic events
- **Memory usage:** Processes hourly batches for efficient memory utilization

### Performance Optimizations Implemented

1. **Pre-loading and Indexing:** All CSV files loaded once and indexed by timestamp (10x speedup)
2. **Incremental CSV Writing:** Events written to output file as processed (real-time progress)
3. **Clean Progress Reporting:** Only logs percentage milestones to reduce noise
4. **Efficient Memory Usage:** Process hourly batches instead of loading all events in memory

### Data Quality Improvements

- **Logical Consistency Fixes:** Converted impossible "0 visits but >0 pageviews" to "≥1 visits"
- **Direct Visit Handling:** Added "(direct)" category for pageviews without referrer data
- **Robust Error Handling:** Graceful handling of malformed CSV data and empty fields
- **Perfect Reconstruction:** Synthetic events exactly reproduce original Fathom statistics

## Important Notes

- **Folder naming:** Name your data folder after your domain (e.g., `mysite.com/`) for automatic hostname detection
- **Required files:** Ensure all CSV files (Site, Pages, Browsers, Countries, DeviceTypes, Referrers, Events) are present
- **Output location:** Converted files will be saved to the specified output path
- **Validation available:** Use `verify_reconstruction.py` to confirm conversion accuracy
- **Processing time:** Varies with data size (~30 seconds per 25k hourly records)
- **Mathematical accuracy:** Synthetic events exactly reproduce original Fathom statistics