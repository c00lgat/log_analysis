import pandas as pd

yes_df = pd.read_csv('url_time_sorted_yes.csv')
cdn_df = pd.read_csv('url_time_sorted_cdn.csv')

# Convert 'URL' columns to string
yes_df['URL'] = yes_df['URL'].astype(str)
cdn_df['URL'] = cdn_df['URL'].astype(str)

# Convert 'timestamp' columns to datetime
yes_df['timestamp'] = pd.to_datetime(yes_df['timestamp'], errors='coerce')
cdn_df['timestamp'] = pd.to_datetime(cdn_df['timestamp'], errors='coerce')

# Sort both DataFrames by 'URL' and 'timestamp' in ascending order
yes_df.sort_values(['timestamp', 'URL'], ascending=[True, True], inplace=True)
cdn_df.sort_values(['timestamp', 'URL'], ascending=[True, True], inplace=True)

# Reset index after sorting
yes_df.reset_index(drop=True, inplace=True)
cdn_df.reset_index(drop=True, inplace=True)

# merged_df = pd.merge_asof(
#     cdn_df, 
#     yes_df, 
#     on='timestamp', 
#     by='URL', 
#     tolerance=pd.Timedelta('1 minute'), 
#     direction='nearest', 
#     suffixes=('_yes', '_cdn')
# )



merged_df.to_csv('final_fusion.csv', index=False)
print(f"Number of output rows: {len(merged_df)}")