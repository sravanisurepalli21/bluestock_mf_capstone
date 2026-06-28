"""
Day 1: Live NAV Fetch Script
Fetches live NAV data from mfapi.in API for specified schemes
"""

import requests
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import time

# Define paths
RAW_DATA_PATH = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")

# Create directories if they don't exist
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

# Scheme codes for our 5 key schemes + HDFC Top 100
SCHEMES = {
    "125497": "HDFC_Top_100_Direct",
    "119551": "SBI_Bluechip", 
    "120503": "ICICI_Bluechip",
    "118632": "Nippon_Large_Cap",
    "119092": "Axis_Bluechip",
    "120841": "Kotak_Bluechip"
}

def fetch_scheme_nav(scheme_code, scheme_name):
    """
    Fetch NAV data for a specific scheme from mfapi.in
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    
    print(f"\n📡 Fetching data for: {scheme_name} (Code: {scheme_code})")
    
    try:
        # Make API request
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Parse JSON
        data = response.json()
        
        # Check if we got valid data
        if 'meta' not in data or 'data' not in data:
            print(f"   ⚠️ Unexpected response format for {scheme_name}")
            return None
        
        # Extract scheme metadata
        meta = data['meta']
        
        # Convert NAV history to DataFrame
        nav_data = data['data']
        df = pd.DataFrame(nav_data)
        
        # Add scheme information to each row
        df['scheme_code'] = scheme_code
        df['scheme_name'] = scheme_name
        df['fund_house'] = meta.get('fund_house', 'N/A')
        df['scheme_type'] = meta.get('scheme_type', 'N/A')
        df['scheme_category'] = meta.get('scheme_category', 'N/A')
        
        # Convert date to datetime and NAV to float
        df['date_parsed'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        df['nav_float'] = pd.to_numeric(df['nav'], errors='coerce')
        
        # Get latest NAV
        latest_nav = df.iloc[0]  # API returns latest first
        print(f"   ✅ Success! Latest NAV: ₹{latest_nav['nav']} as of {latest_nav['date']}")
        print(f"   📊 Fund House: {meta.get('fund_house', 'N/A')}")
        print(f"   📁 Total history: {len(df)} records")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Network error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"   ❌ JSON parsing error: {str(e)}")
        return None
    except Exception as e:
        print(f"   ❌ Unexpected error: {str(e)}")
        return None

def validate_amfi_codes(fund_master_df, nav_data_dict):
    """
    Validate that all AMFI codes in fund master exist in NAV history
    """
    print("\n" + "="*80)
    print("🔍 AMFI CODE VALIDATION")
    print("="*80)
    
    # Assuming fund_master has a 'scheme_code' column
    if fund_master_df is not None and 'scheme_code' in fund_master_df.columns:
        master_codes = set(fund_master_df['scheme_code'].astype(str))
        nav_codes = set(nav_data_dict.keys())
        
        missing_in_nav = master_codes - nav_codes
        extra_in_nav = nav_codes - master_codes
        
        print(f"\n📊 Fund Master Codes: {len(master_codes)}")
        print(f"📊 NAV History Codes: {len(nav_codes)}")
        
        if missing_in_nav:
            print(f"\n⚠️ Codes in fund master but NOT in NAV history: {len(missing_in_nav)}")
            for code in list(missing_in_nav)[:10]:  # Show first 10
                print(f"   - {code}")
        else:
            print(f"\n✅ All fund master codes exist in NAV history!")
        
        if extra_in_nav:
            print(f"\nℹ️ Extra codes in NAV history (not in fund master): {len(extra_in_nav)}")
        
        return {
            'total_master_codes': len(master_codes),
            'total_nav_codes': len(nav_codes),
            'missing_count': len(missing_in_nav),
            'extra_count': len(extra_in_nav)
        }
    else:
        print("\n⚠️ Cannot validate: Fund master data not available or missing scheme_code column")
        return None

def main():
    """
    Main function to fetch NAV data for all schemes
    """
    print("🚀 STARTING LIVE NAV FETCH PROCESS")
    print("="*80)
    print("📡 Fetching data from mfapi.in API")
    print("💡 API is public - no authentication required [citation:2][citation:10]")
    
    all_data = {}
    
    # Fetch data for each scheme
    for code, name in SCHEMES.items():
        df = fetch_scheme_nav(code, name)
        if df is not None:
            all_data[code] = df
        
        # Be respectful to API - small delay between requests
        time.sleep(0.5)
    
    # Save individual scheme data
    print("\n" + "="*80)
    print("💾 SAVING DATA")
    print("="*80)
    
    for code, df in all_data.items():
        scheme_name = SCHEMES[code]
        filename = RAW_DATA_PATH / f"nav_{scheme_name}_{code}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved: {filename.name} ({len(df)} records)")
    
    # Create a combined dataset of latest NAVs
    latest_data = []
    for code, df in all_data.items():
        latest_row = df.iloc[0].to_dict()
        latest_data.append(latest_row)
    
    combined_latest = pd.DataFrame(latest_data)
    combined_latest.to_csv(PROCESSED_DATA_PATH / "latest_nav_all_schemes.csv", index=False)
    print(f"\n✅ Combined latest NAVs saved to: {PROCESSED_DATA_PATH}/latest_nav_all_schemes.csv")
    
    # Generate data quality report
    print("\n" + "="*80)
    print("📋 DATA QUALITY REPORT")
    print("="*80)
    
    for code, df in all_data.items():
        scheme_name = SCHEMES[code]
        print(f"\n📈 {scheme_name}:")
        print(f"   - Total NAV records: {len(df)}")
        print(f"   - Date range: {df['date'].iloc[-1]} to {df['date'].iloc[0]}")
        print(f"   - NAV range: ₹{df['nav_float'].min():.2f} - ₹{df['nav_float'].max():.2f}")
        print(f"   - Missing NAV values: {df['nav_float'].isnull().sum()}")
    
    # Summary
    print("\n" + "="*80)
    print("✨ FETCH COMPLETE")
    print("="*80)
    print(f"✅ Successfully fetched: {len(all_data)}/{len(SCHEMES)} schemes")
    print(f"💾 Data saved to: {RAW_DATA_PATH}")
    
    return all_data

if __name__ == "__main__":
    nav_data = main()
