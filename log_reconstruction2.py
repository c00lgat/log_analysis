import pandas as pd
import numpy as np

# Load your data
cdn_logs = 'full_url_csv.csv'
yes_logs = 'sorted_xlsx.csv'

# Load the CSV file into a DataFrame
cdn_df = pd.read_csv(cdn_logs, encoding='latin1', on_bad_lines='skip')
yes_df = pd.read_csv(yes_logs)
print("Data type of 't' column in 'yes_df':", yes_df['t'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)

yes_df['t'] = pd.to_datetime(yes_df['t'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
cdn_df['dt_utc'] = pd.to_datetime(cdn_df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

print("Data type of 't' column in 'yes_df':", yes_df['t'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)

yes_df.rename(columns={'t': 'dt_utc'}, inplace=True)


print("Data type of 'dt_utc' column in 'yes_df':", yes_df['dt_utc'].dtype)
print("Data type of 'dt_utc' column in 'cdn_df':", cdn_df['dt_utc'].dtype)
# # Normalize column names
# yes_df.columns = yes_df.columns.str.strip().str.lower()
# cdn_df.columns = cdn_df.columns.str.strip().str.lower()

# print("First few entries in 't' column:")
# print(yes_df['t'].head())

# print("Columns before renaming:")
# print(yes_df.columns.tolist())

# if 'dt_utc' in yes_df.columns:
#     yes_df.drop(columns=['dt_utc'], inplace=True)


# yes_df.rename(columns={'t': 'dt_utc'}, inplace=True)
# # After renaming
# print("Columns after renaming:")
# print(yes_df.columns.tolist())

# yes_df['dt_utc'] = pd.to_datetime(yes_df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
# print(type(yes_df['dt_utc']))

# yes_df['dt_utc'] = pd.to_datetime(yes_df['dt_utc'].dt.floor('s'))
# cdn_df['dt_utc'] = pd.to_datetime(cdn_df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

# duplicate_columns = yes_df.columns[yes_df.columns.duplicated()]
# print("Duplicate columns:")
# print(duplicate_columns)

# print("First few entries in 'dt_utc' after renaming:")
# print(yes_df['dt_utc'].head())

# # Ensure datetime columns are timezone-naive
# yes_df['dt_utc'] = yes_df['dt_utc'].dt.tz_localize(None)
# cdn_df['dt_utc'] = cdn_df['dt_utc'].dt.tz_localize(None)

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

