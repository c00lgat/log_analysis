import pandas as pd
import numpy as np

# Load your data
cdn_logs = 'full_url_csv.csv'
yes_logs = 'sorted_xlsx.csv'

# Load the CSV file into a DataFrame
cdn_df = pd.read_csv(cdn_logs, encoding='latin1', on_bad_lines='skip')
yes_df = pd.read_csv(yes_logs)

print("First few entries in 't' column:")
print(yes_df['t'].head())

print("Data type of 't' column in 'yes_df':", yes_df['t'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)

yes_df['t'] = pd.to_datetime(yes_df['t'])
cdn_df['dt_utc'] = pd.to_datetime(cdn_df['dt_utc'])

print("Data type of 't' column in 'yes_df':", yes_df['t'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)

yes_df.rename(columns={'t': 'dt_utc'}, inplace=True)

print("First few entries in 't' column:")
print(yes_df['dt_utc'].head())

print("Data type of 'dt_utc' column in 'yes_df':", yes_df['dt_utc'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)

print("First few entries in 't' column:")
print(yes_df['dt_utc'].head())

# Normalize column names
yes_df.columns = yes_df.columns.str.strip().str.lower()
cdn_df.columns = cdn_df.columns.str.strip().str.lower()

yes_df['dt_utc'] = yes_df['dt_utc'].dt.floor('s')

print("First few entries in 't' column:")
print(yes_df['dt_utc'].head())


# # Ensure datetime columns are timezone-naive
yes_df['dt_utc'] = yes_df['dt_utc'].dt.tz_localize(None)
cdn_df['dt_utc'] = cdn_df['dt_utc'].dt.tz_localize(None)

yes_df = yes_df.sort_values('dt_utc').reset_index(drop=True)
cdn_df = cdn_df.sort_values('dt_utc').reset_index(drop=True)

merged_df = pd.merge_asof(
    cdn_df,
    yes_df,
    on='dt_utc',
    tolerance=pd.Timedelta(seconds=1),
    direction='nearest',
    suffixes=('_cdn', '_yes')
)

# Save the merged DataFrame
merged_df.to_csv('merged_logs.csv', index=False)
print(f"Total rows in merged output: {len(merged_df)}")

# null_count = yes_df['dt_utc'].isna().sum()
# print(f"Number of null values in 'dt_utc' of yes_df: {null_count}")
# # Perform merge_asof with a tolerance of up to 1 second
# merged_df = pd.merge_asof(
#     cdn_df,
#     yes_df,
#     on='dt_utc',
#     tolerance=pd.Timedelta(seconds=1),
#     direction='nearest',
#     suffixes=('_csv', '_xlsx')
# )

# # Verify columns in merged_df
# print("Columns in merged_df:")
# print(merged_df.columns.tolist())

# # Save the merged DataFrame
# merged_df.to_csv('merged_logs.csv', index=False)
# print(f"Number of matched rows: {len(merged_df)}")

