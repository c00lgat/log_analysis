import pandas as pd

cdn_df = pd.read_csv('full_url_cdn_logs.csv')
yes_df = pd.read_csv('sorted_xlsx.csv')

# Rename columns for consistency
yes_df.rename(columns={'Url': 'URL', 't': 'timestamp'}, inplace=True)
cdn_df.rename(columns={'new_url': 'URL', 'dt_utc': 'timestamp'}, inplace=True)

# Convert 'URL' columns to string
yes_df['URL'] = yes_df['URL'].astype(str)
cdn_df['URL'] = cdn_df['URL'].astype(str)

# Convert 'timestamp' columns to datetime
yes_df['timestamp'] = pd.to_datetime(yes_df['timestamp'], errors='coerce')
cdn_df['timestamp'] = pd.to_datetime(cdn_df['timestamp'], errors='coerce')

# Drop rows with missing 'URL' or 'timestamp'
# yes_df.dropna(subset=['URL', 'timestamp'], inplace=True)
# cdn_df.dropna(subset=['URL', 'timestamp'], inplace=True)

# Remove duplicates
# yes_df.drop_duplicates(subset=['URL', 'timestamp'], inplace=True)
# cdn_df.drop_duplicates(subset=['URL', 'timestamp'], inplace=True)

# Sort both DataFrames by 'URL' and 'timestamp' in ascending order
yes_df.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)
cdn_df.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)

# Reset index after sorting
yes_df.reset_index(drop=True, inplace=True)
cdn_df.reset_index(drop=True, inplace=True)

# Verify the sorting
print("First few rows of yes_df after sorting:")
print(yes_df[['URL', 'timestamp']].head(10))

print("\nFirst few rows of cdn_df after sorting:")
print(cdn_df[['URL', 'timestamp']].head(10))


yes_df.to_csv('url_time_sorted_yes.csv', index=False)
print(f"Number of output rows: {len(yes_df)}")

cdn_df.to_csv('url_time_sorted_cdn.csv', index=False)
print(f"Number of output rows: {len(cdn_df)}")

