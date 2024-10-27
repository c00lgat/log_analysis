import pandas as pd
import numpy as np

# Load your data
csv_file = 'RAW_CDN_Logs_12-9-2024.csv'         # Replace with your CSV file name
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'       # Replace with your first XLSX file name
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'       # Replace with your second XLSX file name

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(csv_file)

# Load the two XLSX files and concatenate them into one DataFrame
xlsx_df_a = pd.read_excel(xlsx_file_a)
xlsx_df_b = pd.read_excel(xlsx_file_b)

# Combine the two XLSX DataFrames
xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)

print(f"Number of rows in xlsx_df: {len(xlsx_df)}")
print(f"Number of rows in csv_df: {len(csv_df)}")


# Remove leading/trailing spaces and normalize to lowercase
xlsx_df.columns = xlsx_df.columns.str.strip().str.lower()
csv_df.columns = csv_df.columns.str.strip().str.lower()


# Optionally, normalize column names to lowercase to avoid case sensitivity issues
xlsx_df.columns = xlsx_df.columns.str.lower()
csv_df.columns = csv_df.columns.str.lower()

# Print the columns to check their names
print("Columns in xlsx_df:", xlsx_df.columns.tolist())
print("Columns in csv_df:", csv_df.columns.tolist())

# Rename columns in XLSX DataFrame to match CSV DataFrame columns
xlsx_df.rename(columns={
    'returncode': 'statuscode',         # From 'ReturnCode' to 'statuscode'
    'crc': 'cachestatus',
    'b': 'bytes',
    'stms': 'transfertimemsec',
    't': 'dt_utc',
    'url': 'full_url',                  # From 'Url' to 'full_url' to match reconstructed URL in CSV
}, inplace=True)

def reconstruct_url(row):
    # Clean up 'proto' by removing any protocol version suffix
    proto = row['proto'].split('/')[0].lower() if pd.notnull(row['proto']) else 'http'
    
    # Clean up 'reqhost' by removing port numbers
    reqhost = row['reqhost'].split(':')[0].lower() if pd.notnull(row['reqhost']) else ''
    
    # Ensure 'reqpath' starts with '/'
    reqpath = row['reqpath'] if pd.notnull(row['reqpath']) else ''
    if not reqpath.startswith('/'):
        reqpath = '/' + reqpath
    
    # Clean up 'querystr'
    querystr = row['querystr'] if pd.notnull(row['querystr']) else ''
    
    # Construct the URL
    url = f"{proto}://{reqhost}{reqpath}"
    if querystr:
        url += f"?{querystr}"
    return url.lower().rstrip('/')

csv_df['full_url'] = csv_df.apply(reconstruct_url, axis=1)


# Normalize URLs: convert to lowercase and remove trailing slashes
csv_df['full_url'] = csv_df['full_url'].str.lower().str.rstrip('/')
# Remove any 'www.' prefix and convert to lowercase
xlsx_df['full_url'] = xlsx_df['full_url'].str.lower().str.rstrip('/').str.replace('www.', '')

# Convert time columns to datetime
csv_df['dt_utc'] = pd.to_datetime(csv_df['dt_utc'], errors='coerce')
xlsx_df['dt_utc'] = pd.to_datetime(xlsx_df['dt_utc'], errors='coerce')

# Normalize numeric columns to float
csv_df['bytes'] = pd.to_numeric(csv_df['bytes'], errors='coerce')
xlsx_df['bytes'] = pd.to_numeric(xlsx_df['bytes'], errors='coerce')

csv_df['transfertimemsec'] = pd.to_numeric(csv_df['transfertimemsec'], errors='coerce')
xlsx_df['transfertimemsec'] = pd.to_numeric(xlsx_df['transfertimemsec'], errors='coerce')

# Adjust data types to ensure matching
csv_df['statuscode'] = csv_df['statuscode'].astype(str)
xlsx_df['statuscode'] = xlsx_df['statuscode'].astype(str)

csv_df['cachestatus'] = csv_df['cachestatus'].astype(str)
xlsx_df['cachestatus'] = xlsx_df['cachestatus'].astype(str)

# Create time windows (e.g., round to nearest minute)
# csv_df['time_window'] = csv_df['dt_utc'].dt.floor('5min')
# xlsx_df['time_window'] = xlsx_df['dt_utc'].dt.floor('5min')

# Use an hourly time window
csv_df['time_window'] = csv_df['dt_utc'].dt.floor('H')
xlsx_df['time_window'] = xlsx_df['dt_utc'].dt.floor('H')



#List of key columns used for merging
key_columns = ['url_no_proto', 'statuscode', 'cachestatus', 'bytes', 'transfertimemsec', 'time_window']

# Adjust data types if necessary
xlsx_df['statuscode'] = xlsx_df['statuscode'].astype(str)
csv_df['statuscode'] = csv_df['statuscode'].astype(str)

xlsx_df['cachestatus'] = xlsx_df['cachestatus'].astype(str)
csv_df['cachestatus'] = csv_df['cachestatus'].astype(str)


# Check for missing values in xlsx_df
print("Missing values in xlsx_df key columns:")
print(xlsx_df[key_columns].isnull().sum())

# Check for missing values in csv_df
print("Missing values in csv_df key columns:")
print(csv_df[key_columns].isnull().sum())

# Display data types in xlsx_df
print("Data types in xlsx_df key columns:")
print(xlsx_df[key_columns].dtypes)

# Display data types in csv_df
print("Data types in csv_df key columns:")
print(csv_df[key_columns].dtypes)


# Unique full URLs
xlsx_urls = set(xlsx_df['full_url'].unique())
csv_urls = set(csv_df['full_url'].unique())

# Intersection of URLs
common_urls = xlsx_urls.intersection(csv_urls)
print(f"Number of unique URLs in xlsx_df: {len(xlsx_urls)}")
print(f"Number of unique URLs in csv_df: {len(csv_urls)}")
print(f"Number of common URLs: {len(common_urls)}")

# Unique status codes
xlsx_status_codes = set(xlsx_df['statuscode'].unique())
csv_status_codes = set(csv_df['statuscode'].unique())
common_status_codes = xlsx_status_codes.intersection(csv_status_codes)
print(f"Common status codes: {common_status_codes}")

# Unique cache statuses
xlsx_cache_statuses = set(xlsx_df['cachestatus'].unique())
csv_cache_statuses = set(csv_df['cachestatus'].unique())
common_cache_statuses = xlsx_cache_statuses.intersection(csv_cache_statuses)
print(f"Common cache statuses: {common_cache_statuses}")

print("Unique cache statuses in xlsx_df:")
print(xlsx_df['cachestatus'].unique())

print("Unique cache statuses in csv_df:")
print(csv_df['cachestatus'].unique())

xlsx_df['cachestatus'] = xlsx_df['cachestatus'].str.upper().str.strip()
csv_df['cachestatus'] = csv_df['cachestatus'].str.upper().str.strip()

xlsx_cache_statuses = set(xlsx_df['cachestatus'].unique())
csv_cache_statuses = set(csv_df['cachestatus'].unique())
common_cache_statuses = xlsx_cache_statuses.intersection(csv_cache_statuses)
print(f"Common cache statuses after normalization: {common_cache_statuses}")


print("Sample data from xlsx_df:")
print(xlsx_df.head())

print("Sample data from csv_df:")
print(csv_df.head())


# Check for NaT (Not a Time) in dt_utc columns
print(f"NaT values in xlsx_df['dt_utc']: {xlsx_df['dt_utc'].isnull().sum()}")
print(f"NaT values in csv_df['dt_utc']: {csv_df['dt_utc'].isnull().sum()}")


# Check the range of time_window values
print("Time window range in xlsx_df:")
print(xlsx_df['time_window'].min(), "to", xlsx_df['time_window'].max())

print("Time window range in csv_df:")
print(csv_df['time_window'].min(), "to", csv_df['time_window'].max())

# Remove protocol from 'full_url's
xlsx_df['url_no_proto'] = xlsx_df['full_url'].str.replace(r'^https?://', '', regex=True)
csv_df['url_no_proto'] = csv_df['full_url'].str.replace(r'^https?://', '', regex=True)

# Also remove 'www.' if present
xlsx_df['url_no_proto'] = xlsx_df['url_no_proto'].str.replace(r'^www\.', '', regex=True)
csv_df['url_no_proto'] = csv_df['url_no_proto'].str.replace(r'^www\.', '', regex=True)

xlsx_urls = set(xlsx_df['url_no_proto'].unique())
csv_urls = set(csv_df['url_no_proto'].unique())
common_urls = xlsx_urls.intersection(csv_urls)
print(f"Number of unique URLs without protocol in xlsx_df: {len(xlsx_urls)}")
print(f"Number of unique URLs without protocol in csv_df: {len(csv_urls)}")
print(f"Number of common URLs without protocol: {len(common_urls)}")

print("Available columns in csv_df:", csv_df.columns.tolist())

print("Unique 'cp' values in csv_df:")
print(csv_df['cp'].unique())

# Use 'cp' as 'cachestatus' in csv_df
csv_df.rename(columns={'cp': 'cachestatus'}, inplace=True)

# Normalize 'cachestatus'
csv_df['cachestatus'] = csv_df['cachestatus'].str.upper().str.strip()

print("Unique cache statuses in csv_df after using 'cp':")
print(csv_df['cachestatus'].unique())

xlsx_cache_statuses = set(xlsx_df['cachestatus'].unique())
csv_cache_statuses = set(csv_df['cachestatus'].unique())
common_cache_statuses = xlsx_cache_statuses.intersection(csv_cache_statuses)
print(f"Common cache statuses after adjustment: {common_cache_statuses}")

print("Data type of xlsx_df['dt_utc']:", xlsx_df['dt_utc'].dtype)
print("First few 'dt_utc' values in xlsx_df:")
print(xlsx_df['dt_utc'].head())

xlsx_df['dt_utc'] = pd.to_datetime(xlsx_df['dt_utc'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

xlsx_df['time_window'] = xlsx_df['dt_utc'].dt.floor('H')
csv_df['time_window'] = csv_df['dt_utc'].dt.floor('H')

print("Time window range in xlsx_df:")
print(xlsx_df['time_window'].min(), "to", xlsx_df['time_window'].max())

print("Time window range in csv_df:")
print(csv_df['time_window'].min(), "to", csv_df['time_window'].max())



# Merge without time_window
merged_df_no_time = pd.merge(
    xlsx_df,
    csv_df,
    on=['full_url', 'statuscode', 'cachestatus', 'bytes', 'transfertimemsec'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)
print(f"Number of rows after merging without time_window: {len(merged_df_no_time)}")

# Merge on fewer columns
merged_df_less_columns = pd.merge(
    xlsx_df,
    csv_df,
    on=['full_url', 'statuscode', 'cachestatus'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)
print(f"Number of rows after merging on fewer columns: {len(merged_df_less_columns)}")


print("Sample full URLs from xlsx_df:")
print(xlsx_df['full_url'].head(10))

print("Sample full URLs from csv_df:")
print(csv_df['full_url'].head(10))

# Remove query parameters from URLs for matching
xlsx_df['url_no_query'] = xlsx_df['full_url'].str.split('?').str[0]
csv_df['url_no_query'] = csv_df['full_url'].str.split('?').str[0]

# Remove trailing slashes
xlsx_df['url_no_query'] = xlsx_df['url_no_query'].str.rstrip('/')
csv_df['url_no_query'] = csv_df['url_no_query'].str.rstrip('/')

# Merge on 'url_no_query' instead of 'full_url'
merged_df_url_no_query = pd.merge(
    xlsx_df,
    csv_df,
    on=['url_no_query', 'statuscode', 'cachestatus', 'bytes', 'transfertimemsec', 'time_window'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)
print(f"Number of rows after merging on 'url_no_query': {len(merged_df_url_no_query)}")



# List of string columns to strip
string_columns = ['full_url', 'statuscode', 'cachestatus']

for col in string_columns:
    xlsx_df[col] = xlsx_df[col].str.strip()
    csv_df[col] = csv_df[col].str.strip()


# Ensure both datetime columns are timezone-naive
xlsx_df['dt_utc'] = xlsx_df['dt_utc'].dt.tz_localize(None)
csv_df['dt_utc'] = csv_df['dt_utc'].dt.tz_localize(None)

merged_df = pd.merge(
    xlsx_df,
    csv_df,
    on=key_columns,
    how='inner',
    suffixes=('_xlsx', '_csv')
)
print(f"Number of rows after merging with corrected data: {len(merged_df)}")


# Final attempt to merge after adjustments
final_merged_df = pd.merge(
    xlsx_df,
    csv_df,
    on=['full_url', 'statuscode', 'cachestatus', 'bytes', 'transfertimemsec', 'time_window'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)

print(f"Number of rows after final merging: {len(final_merged_df)}")

# Save the final matched DataFrame
final_output_file = 'matched_rows_final.csv'
final_merged_df.to_csv(final_output_file, index=False)
print(f"Final matching complete. The results have been saved to '{final_output_file}'.")

print(f"Number of rows in xlsx_df: {len(xlsx_df)}")
print(f"Number of rows in csv_df: {len(csv_df)}")


# Merge DataFrames on common fields, including time window
merged_df = pd.merge(
    xlsx_df,
    csv_df,
    on=['full_url', 'statuscode', 'cachestatus', 'bytes', 'transfertimemsec', 'time_window'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)

# Define tolerances
bytes_tolerance = 5000
stms_tolerance = 500

# Merge on non-numeric fields
partial_merged_df = pd.merge(
    xlsx_df,
    csv_df,
    on=['url_no_proto', 'statuscode', 'cachestatus', 'time_window'],
    how='inner',
    suffixes=('_xlsx', '_csv')
)

# Apply tolerances
matched_df = partial_merged_df[
    (np.abs(partial_merged_df['bytes_xlsx'] - partial_merged_df['bytes_csv']) <= bytes_tolerance) &
    (np.abs(partial_merged_df['transfertimemsec_xlsx'] - partial_merged_df['transfertimemsec_csv']) <= stms_tolerance)
]
print(f"Number of rows after applying tolerances: {len(matched_df)}")

# Save the matched DataFrame to a new CSV file
output_file = 'matched_rows.csv'  # Specify your desired output file name
matched_df.to_csv(output_file, index=False)

print(f"Matching complete. The results have been saved to '{output_file}'.")
