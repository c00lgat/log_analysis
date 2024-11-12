import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_timestamp(ts):
    """
    Clean and standardize timestamp format.
    Handle various timestamp formats and return a consistent datetime object.
    """
    try:
        if pd.isna(ts):
            return np.nan
        # Try parsing with milliseconds
        try:
            return pd.to_datetime(ts, format='%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            # Try parsing without milliseconds
            return pd.to_datetime(ts, format='%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f"Error parsing timestamp {ts}: {str(e)}")
        return np.nan

def clean_url(url):
    """
    Clean and standardize URL format.
    Remove trailing spaces, convert to lowercase, and ensure consistent formatting.
    """
    if pd.isna(url):
        return np.nan
    return str(url).strip().lower()

def verify_sorting(df, group_col='URL', timestamp_col='timestamp'):
    """
    Verify that the DataFrame is properly sorted by group and timestamp.
    Returns True if timestamps are monotonically increasing within each group, False otherwise.
    """
    # Check if the DataFrame is sorted by group_col and timestamp_col
    if not df.index.equals(df.sort_values([group_col, timestamp_col], ascending=[True, True]).index):
        logger.error("DataFrame is not sorted by URL and timestamp")
        return False

    # Check if timestamps are monotonically increasing within each group
    grouped = df.groupby(group_col)
    for name, group in grouped:
        if not group[timestamp_col].is_monotonic_increasing:
            logger.error(f"Timestamps not increasing in group {name}")
            # Output the problematic group
            print(f"Group '{name}' is not sorted correctly:")
            print(group[[timestamp_col]])
            return False
    return True

def prepare_dataframe(df, url_col, timestamp_col):
    """
    Prepare DataFrame for merging by cleaning and standardizing data.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        url_col (str): Name of the URL column
        timestamp_col (str): Name of the timestamp column
    
    Returns:
        pd.DataFrame: Cleaned and prepared DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    df_clean = df.copy()
    
    # Rename columns for consistency
    df_clean.rename(columns={
        url_col: 'URL',
        timestamp_col: 'timestamp'
    }, inplace=True)
    
    # Clean URLs and timestamps
    logger.info("Cleaning URLs and timestamps...")
    df_clean['URL'] = df_clean['URL'].apply(clean_url)
    df_clean['timestamp'] = df_clean['timestamp'].apply(clean_timestamp)
    
    # Drop rows with missing values in key columns
    initial_rows = len(df_clean)
    df_clean.dropna(subset=['URL', 'timestamp'], inplace=True)
    dropped_rows = initial_rows - len(df_clean)
    if dropped_rows > 0:
        logger.warning(f"Dropped {dropped_rows} rows with missing values")
    
    # Sort values and reset index
    df_clean.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)
    df_clean.reset_index(drop=True, inplace=True)
    
    return df_clean

def merge_csv_files(file1_path, file2_path, output_path):
    """
    Main function to merge two CSV files based on URL and timestamp.
    
    Args:
        file1_path (str): Path to the first CSV file (filtered_yes_no_304.csv)
        file2_path (str): Path to the second CSV file (full_url_csv2.csv)
        output_path (str): Path for the output merged CSV file
    """
    try:
        # Read CSV files
        logger.info("Reading CSV files...")
        yes_df = pd.read_csv(file1_path)
        cdn_df = pd.read_csv(file2_path)
        
        # Prepare both DataFrames
        logger.info("Preparing first DataFrame...")
        yes_df_clean = prepare_dataframe(yes_df, 'Url', 't')
        
        logger.info("Preparing second DataFrame...")
        cdn_df_clean = prepare_dataframe(cdn_df, 'new_url', 'dt_utc')
        
        # Handle duplicates in yes_df_clean
        duplicates = yes_df_clean[yes_df_clean.duplicated(subset=['URL', 'timestamp'], keep=False)]
        if not duplicates.empty:
            logger.warning("Duplicate timestamps found in 'yes_df_clean'. Adjusting timestamps slightly to ensure uniqueness.")
            yes_df_clean['timestamp'] += pd.to_timedelta(
                yes_df_clean.groupby(['URL', 'timestamp']).cumcount() * 1e-9, unit='s'
            )
        
        # Ensure time zones are consistent
        yes_df_clean['timestamp'] = yes_df_clean['timestamp'].dt.tz_localize(None)
        cdn_df_clean['timestamp'] = cdn_df_clean['timestamp'].dt.tz_localize(None)
        
        # Explicitly sort the DataFrames
        yes_df_clean.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)
        cdn_df_clean.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)
        
        # Reset index after sorting
        yes_df_clean.reset_index(drop=True, inplace=True)
        cdn_df_clean.reset_index(drop=True, inplace=True)
        
        # Verify sorting
        logger.info("Verifying DataFrame sorting...")
        if not verify_sorting(yes_df_clean):
            logger.error("First DataFrame timestamps are not monotonically increasing within each URL group")
            raise ValueError("First DataFrame must be sorted by URL and timestamp")
        
        if not verify_sorting(cdn_df_clean):
            logger.error("Second DataFrame timestamps are not monotonically increasing within each URL group")
            raise ValueError("Second DataFrame must be sorted by URL and timestamp")
        
        # Perform merge
        logger.info("Performing merge operation...")
        merged_df = pd.merge_asof(
            yes_df_clean,
            cdn_df_clean,
            on='timestamp',
            by='URL',
            tolerance=pd.Timedelta('1min'),
            direction='nearest',
            suffixes=('_yes', '_cdn')
        )
        
        # Log merge results
        logger.info(f"Merge complete. Input shapes: {yes_df_clean.shape}, {cdn_df_clean.shape}")
        logger.info(f"Output shape: {merged_df.shape}")
        
        # Save merged DataFrame
        logger.info(f"Saving merged data to {output_path}")
        merged_df.to_csv(output_path, index=False)
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Error during merge operation: {str(e)}")
        raise

def main():
    """
    Main execution function with error handling.
    """
    try:
        # File paths
        file1_path = 'filtered_yes_no_304.csv'
        file2_path = 'full_url_csv2.csv'
        output_path = 'final_boss_merged.csv'
        
        # Execute merge
        merged_df = merge_csv_files(file1_path, file2_path, output_path)
        
        # Print summary statistics
        print("\nMerge Summary:")
        print(f"Total rows in merged file: {len(merged_df)}")
        print(f"Unique URLs: {merged_df['URL'].nunique()}")
        print(f"Date range: {merged_df['timestamp'].min()} to {merged_df['timestamp'].max()}")
        
        # Additional verification
        print("\nMatching Statistics:")
        matched_rows = merged_df.dropna().shape[0]
        unmatched_rows = len(merged_df) - matched_rows
        print(f"Matched rows: {matched_rows}")
        print(f"Unmatched rows: {unmatched_rows}")
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()



Prompt:

Background:

I have two CSV files that I need to merge using pandas in Python. The goal is to match rows from both CSVs based on the 'URL' column and approximate timestamps (within a 1-minute tolerance). However, I'm encountering issues with data types, sorting, and merging, specifically getting a ValueError: left keys must be sorted error.

CSV File Details:

    First CSV File (filtered_yes_no_304.csv):
        Purpose: Contains request logs.
        Columns:
            'Url': The URL of the request (string).
            't': The timestamp of the request (string, may include milliseconds).
            'ReturnCode': Integer.
            'crc': String.
            'b': Integer.
            'stms': Integer.
        Sample Data:

    Url                                          t                       ReturnCode crc     b     stms
    http://example.com/page1.html 2023-09-11 18:29:50.776              200        abc123  100   50
    http://example.com/page2.html 2023-09-11 04:44:21.114              404        def456  200   60

Second CSV File (full_url_csv2.csv):

    Purpose: Contains response logs.
    Columns:
        'new_url': The URL of the response (string).
        'dt_utc': The timestamp of the response (string, may lack milliseconds).
        Additional columns (e.g., _cdn_token, edgeip, cliip, etc.).
    Sample Data:

        new_url                                   dt_utc                    statuscode
        http://example.com/page1.html 2023-09-11 18:29:51                 200
        http://example.com/page2.html 2023-09-11 04:44:22                 200

Objective:

    Merge the two CSV files into a single DataFrame.
    Match rows based on:
        'URL' values (from 'Url' in the first CSV and 'new_url' in the second CSV).
        Timestamps within a 1-minute tolerance (from 't' in the first CSV and 'dt_utc' in the second CSV).
    Handle differences in timestamp formats and ensure proper datetime parsing.
    Ensure DataFrames are correctly sorted for merge_asof.
    Address and fix the ValueError: left keys must be sorted error.

Issues Encountered:

    Getting ValueError: left keys must be sorted during the merge operation.
    Timestamps in the two CSV files may have different formats (e.g., presence or absence of milliseconds).
    DataFrames may not be properly sorted, or there may be issues with data types.

Requirements:

    Provide a complete, working Python code using pandas that:
        Reads both CSV files into DataFrames.
        Renames columns to have consistent names ('Url' and 'new_url' to 'URL'; 't' and 'dt_utc' to 'timestamp').
        Converts 'timestamp' columns to datetime64[ns] dtype, handling different formats.
        Ensures both DataFrames are sorted by 'URL' and 'timestamp' in ascending order.
        Merges the DataFrames using pd.merge_asof, with a 1-minute tolerance, matching on 'timestamp' and 'URL'.
        Saves the merged DataFrame to 'final_boss_merged.csv'.
        Includes error checking and handles any potential issues that might cause the ValueError.

Additional Information:

    I'm using Python 3.12 and pandas (version X.X.X).

    Here's the code I've tried (simplified):

    import pandas as pd

    yes_df = pd.read_csv('filtered_yes_no_304.csv')
    cdn_df = pd.read_csv('full_url_csv2.csv')

    yes_df.rename(columns={'Url': 'URL', 't': 'timestamp'}, inplace=True)
    cdn_df.rename(columns={'new_url': 'URL', 'dt_utc': 'timestamp'}, inplace=True)

    yes_df['timestamp'] = pd.to_datetime(yes_df['timestamp'])
    cdn_df['timestamp'] = pd.to_datetime(cdn_df['timestamp'])

    yes_df.sort_values(['URL', 'timestamp'], inplace=True)
    cdn_df.sort_values(['URL', 'timestamp'], inplace=True)

    merged_df = pd.merge_asof(
        yes_df,
        cdn_df,
        on='timestamp',
        by='URL',
        tolerance=pd.Timedelta('1min'),
        direction='nearest'
    )

    merged_df.to_csv('final_boss_merged.csv', index=False)

