import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def find_matching_yes_log(cdn_row, yes_df, used_yes_indices, time_window_seconds):
    """
    Helper function to find matching YES log for a given CDN row
    """
    # Define time window for matching
    time_start = cdn_row['dt_utc'] - timedelta(seconds=time_window_seconds)
    time_end = cdn_row['dt_utc'] + timedelta(seconds=time_window_seconds)
    
    # Filter YES logs within time window and matching URL
    potential_matches = yes_df[
        (yes_df['yes_t'] >= time_start) &
        (yes_df['yes_t'] <= time_end) &
        (yes_df['yes_Url'] == cdn_row['new_url']) &
        (~yes_df.index.isin(used_yes_indices))
    ]
    
    if len(potential_matches) > 0:
        # Calculate time differences
        time_diffs = (potential_matches['yes_t'] - cdn_row['dt_utc']).abs()
        closest_idx = time_diffs.idxmin()
        closest_match = potential_matches.loc[closest_idx]
        
        # Mark this YES log as used
        used_yes_indices.add(closest_idx)
        
        return closest_match
    
    return pd.Series({col: None for col in yes_df.columns})

def match_cdn_yes_logs(cdn_df, yes_df, time_window_seconds=2):
    """
    Match CDN logs with YES logs ensuring each YES log is matched only once.
    Retains all columns from both dataframes.
    """
    print("Converting timestamps...")
    # Convert timestamps to datetime objects
    cdn_df['dt_utc'] = pd.to_datetime(cdn_df['dt_utc'])
    yes_df['t'] = pd.to_datetime(yes_df['t'])
    
    # Sort both dataframes by timestamp
    print("Sorting dataframes...")
    cdn_df = cdn_df.sort_values('dt_utc')
    yes_df = yes_df.sort_values('t')
    
    # Create a copy of yes_df to track matches
    yes_df['matched'] = False
    
    # Prefix YES columns to avoid conflicts
    yes_columns = {col: f'yes_{col}' for col in yes_df.columns}
    yes_df = yes_df.rename(columns=yes_columns)
    
    # Initialize columns to store matching YES log information
    for col in yes_df.columns:
        cdn_df[col] = None
    
    # Create a set to track used YES indices
    used_yes_indices = set()
    
    print("Starting log matching process...")
    # Process in batches to show progress
    batch_size = 10000
    total_rows = len(cdn_df)
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = cdn_df.iloc[start_idx:end_idx]
        
        matches = []
        for _, row in batch.iterrows():
            match = find_matching_yes_log(row, yes_df, used_yes_indices, time_window_seconds)
            matches.append(match)
        
        cdn_df.iloc[start_idx:end_idx, cdn_df.columns.get_indexer(yes_df.columns)] = matches
        
        # Print progress
        progress = (end_idx / total_rows) * 100
        print(f"Progress: {progress:.1f}% ({end_idx:,}/{total_rows:,} rows)")
    
    # Calculate match statistics
    total_cdn_logs = len(cdn_df)
    matched_logs = cdn_df['yes_t'].notna().sum()
    unmatched_logs = total_cdn_logs - matched_logs
    
    print(f"\nMatching Statistics:")
    print(f"Total CDN logs: {total_cdn_logs:,}")
    print(f"Matched logs: {matched_logs:,}")
    print(f"Unmatched logs: {unmatched_logs:,}")
    print(f"Match rate: {(matched_logs/total_cdn_logs)*100:.2f}%")
    
    return cdn_df

if __name__ == "__main__":
    print("Starting log analysis...")
    
    try:
        # Load your data
        print("Loading CSV files...")
        cdn_logs = 'full_url_csv2.csv'
        yes_logs = 'sorted_xlsx.csv'

        cdn_df = pd.read_csv(cdn_logs, engine='python', encoding='latin1', on_bad_lines='skip')
        yes_df = pd.read_csv(yes_logs)
        
        print(f"Loaded CDN logs: {len(cdn_df):,} rows")
        print(f"Loaded YES logs: {len(yes_df):,} rows")

        # Perform the matching
        matched_df = match_cdn_yes_logs(cdn_df, yes_df)

        # Save the merged DataFrame
        output_file = 'matched_df.csv'
        print(f"\nSaving matched results to {output_file}...")
        matched_df.to_csv(output_file, index=False)
        print(f"Total rows in merged output: {len(matched_df):,}")
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        print("\nTraceback:")
        import traceback
        traceback.print_exc()