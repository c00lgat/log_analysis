import pandas as pd
import numpy as np

# Load your data (same as before)
csv_file = 'RAW_CDN_Logs_12-9-2024.csv'
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(csv_file)

# Load the two XLSX files and concatenate them into one DataFrame
xlsx_df_a = pd.read_excel(xlsx_file_a)
xlsx_df_b = pd.read_excel(xlsx_file_b)
xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)

# Normalize column names
xlsx_df.columns = xlsx_df.columns.str.strip().str.lower()
csv_df.columns = csv_df.columns.str.strip().str.lower()

# Rename columns in XLSX DataFrame
xlsx_df.rename(columns={
    'returncode': 'statuscode',
    'crc': 'cachestatus',
    'b': 'bytes',
    'stms': 'transfertimemsec',
    't': 'dt_utc',
    'url': 'full_url',
}, inplace=True)

# Reconstruct the full URL in the CSV DataFrame
def reconstruct_url(row):
    proto = row['proto'].split('/')[0].lower() if pd.notnull(row['proto']) else 'http'
    reqhost = row['reqhost'].split(':')[0].lower() if pd.notnull(row['reqhost']) else ''
    reqpath = row['reqpath'] if pd.notnull(row['reqpath']) else ''
    if not reqpath.startswith('/'):
        reqpath = '/' + reqpath
    querystr = row['querystr'] if pd.notnull(row['querystr']) else ''
    url = f"{proto}://{reqhost}{reqpath}"
    if querystr:
        url += f"?{querystr}"
    return url.lower().rstrip('/')

csv_df['full_url'] = csv_df.apply(reconstruct_url, axis=1)

# Normalize URLs in both DataFrames
xlsx_df['full_url'] = xlsx_df['full_url'].str.lower().str.rstrip('/')
csv_df['full_url'] = csv_df['full_url'].str.lower().str.rstrip('/')

# Remove protocol from 'full_url's
xlsx_df['url_no_proto'] = xlsx_df['full_url'].str.replace(r'^https?://', '', regex=True)
csv_df['url_no_proto'] = csv_df['full_url'].str.replace(r'^https?://', '', regex=True)

# Remove 'www.' if present
xlsx_df['url_no_proto'] = xlsx_df['url_no_proto'].str.replace(r'^www\.', '', regex=True)
csv_df['url_no_proto'] = csv_df['url_no_proto'].str.replace(r'^www\.', '', regex=True)

# Convert time columns to datetime with the correct format
xlsx_df['dt_utc'] = pd.to_datetime(xlsx_df['dt_utc'], errors='coerce')
csv_df['dt_utc'] = pd.to_datetime(csv_df['dt_utc'], errors='coerce')

# Ensure both datetime columns are timezone-naive
xlsx_df['dt_utc'] = xlsx_df['dt_utc'].dt.tz_localize(None)
csv_df['dt_utc'] = csv_df['dt_utc'].dt.tz_localize(None)

# Create time windows (e.g., hourly)
xlsx_df['time_window'] = xlsx_df['dt_utc'].dt.floor('h')
csv_df['time_window'] = csv_df['dt_utc'].dt.floor('h')

# Ensure data types match
xlsx_df['statuscode'] = xlsx_df['statuscode'].astype(str)
csv_df['statuscode'] = csv_df['statuscode'].astype(str)

xlsx_df['bytes'] = xlsx_df['bytes'].astype(float)
csv_df['bytes'] = csv_df['bytes'].astype(float)

xlsx_df['transfertimemsec'] = xlsx_df['transfertimemsec'].astype(float)
csv_df['transfertimemsec'] = csv_df['transfertimemsec'].astype(float)

# Key columns for merging (excluding 'cachestatus')
key_columns = ['url_no_proto', 'statuscode', 'bytes', 'transfertimemsec', 'time_window']

# Merge DataFrames to find matches
merged_df = pd.merge(
    xlsx_df,
    csv_df,
    on=key_columns,
    how='inner',
    suffixes=('_xlsx', '_csv')
)
print(f"Number of rows after merging with corrected data: {len(merged_df)}")

# Save matched rows
if len(merged_df) > 0:
    merged_df.to_csv('matched_rows.csv', index=False)
    print("Matching complete. The matched rows have been saved to 'matched_rows.csv'.")

# Now, find unmatched rows in csv_df
# Perform a left merge with indicator
merged_with_indicator = pd.merge(
    csv_df,
    xlsx_df,
    on=key_columns,
    how='left',
    indicator=True
)

# Select unmatched rows (left_only)
unmatched_csv = merged_with_indicator[merged_with_indicator['_merge'] == 'left_only']

# Drop the merge indicator column
unmatched_csv.drop(columns=['_merge'], inplace=True)

# Save unmatched rows to a separate CSV file
unmatched_csv.to_csv('unmatched_csv_rows.csv', index=False)
print(f"Number of unmatched rows in csv_df: {len(unmatched_csv)}")
print("Unmatched CSV rows have been saved to 'unmatched_csv_rows.csv'.")
