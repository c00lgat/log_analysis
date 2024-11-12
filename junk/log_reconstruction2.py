import pandas as pd
import numpy as np

# Load your data
cdn_logs = 'full_url_csv2.csv'
yes_logs = 'sorted_xlsx.csv'

# Load the CSV file into a DataFrame
cdn_df = pd.read_csv(cdn_logs, engine='python', encoding='latin1', on_bad_lines='skip')
yes_df = pd.read_csv(yes_logs)

# Normalize column names
yes_df.columns = yes_df.columns.str.strip().str.lower()
cdn_df.columns = cdn_df.columns.str.strip().str.lower()


# Rename 'url' column in yes_df to 'new_url' to match cdn_df
yes_df.rename(columns={'url': 'new_url'}, inplace=True)

# Convert 'dt_utc' and 't' columns to datetime
cdn_df['dt_utc'] = pd.to_datetime(cdn_df['dt_utc'])
yes_df['t'] = pd.to_datetime(yes_df['t'])

# Remove timezone information
cdn_df['dt_utc'] = cdn_df['dt_utc'].dt.tz_localize(None)
yes_df['t'] = yes_df['t'].dt.tz_localize(None)

# Truncate datetime to remove milliseconds
cdn_df['dt_utc_truncated'] = cdn_df['dt_utc'].dt.floor('s')
yes_df['t_truncated'] = yes_df['t'].dt.floor('s')

# Sort DataFrames by the truncated datetime
cdn_df = cdn_df.sort_values('dt_utc_truncated')
yes_df = yes_df.sort_values('t_truncated')

# Create a key by combining 'new_url' and 'dt_utc_truncated'
cdn_df['merge_key'] = cdn_df['new_url'] + '__' + cdn_df['dt_utc_truncated'].astype(str)
yes_df['merge_key'] = yes_df['new_url'] + '__' + yes_df['t_truncated'].astype(str)

cdn_df['footprint_key'] = cdn_df['new_url'].astype(str) + '__' \
                        + cdn_df['dt_utc_truncated'].astype(str) + '__' \
                        + cdn_df['edgeip'].astype(str) + '__' \
                        + cdn_df['cliip'].astype(str) + '__' \
                        + cdn_df['reqid'].astype(str) + '__' \
                        + cdn_df['xforwardedfor'].astype(str)

cdn_duplicates = cdn_df['footprint_key'].duplicated().sum()
print(f"Number of duplicate footprint keys in cdn_df: {cdn_duplicates}")

yes_duplicates = yes_df['merge_key'].duplicated().sum()
print(f"Number of duplicate merge keys in yes_df: {yes_duplicates}")

unique_cdn_keys = cdn_df['merge_key'].nunique()
unique_cdn_footprint_keys = cdn_df['footprint_key'].nunique()
unique_yes_keys = yes_df['merge_key'].nunique()

print(f"Unique merge keys in cdn_df: {unique_cdn_keys}")
print(f"Unique merge keys in yes_df: {unique_yes_keys}")


# Merge using pd.merge_asof with a tolerance
# merged_df = pd.merge_asof(
#     cdn_df,
#     yes_df,
#     left_on='dt_utc_truncated',
#     right_on='t_truncated',
#     by='new_url',
#     tolerance=pd.Timedelta(seconds=1),
#     direction='nearest',
#     suffixes=('_cdn', '_yes')
# )

merged_df = pd.merge(
    cdn_df,
    yes_df,
    on=['merge_key'],
    suffixes=('_cdn', '_yes'),
    how='left'  # Use 'left' to keep all rows from cdn_df
)

# Save the merged DataFrame
merged_df.to_csv('merged_logs.csv', index=False)
print(f"Total rows in merged output: {len(merged_df)}")



# Count the number of matched and unmatched rows
matched_rows = merged_df['t'].notnull().sum()
unmatched_rows = len(merged_df) - matched_rows
match_rate = (matched_rows / len(merged_df)) * 100

print(f"Number of matched rows: {matched_rows}")
print(f"Number of unmatched rows: {unmatched_rows}")
print(f"Match rate: {match_rate:.2f}%")





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

