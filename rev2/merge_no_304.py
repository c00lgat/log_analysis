import pandas as pd
import numpy as np


yes_df = pd.read_csv('url_time_sorted_yes_no_304.csv')
cdn_df = pd.read_csv('url_time_sorted_cdn.csv')

# Convert 'timestamp' columns to datetime
yes_df['timestamp'] = pd.to_datetime(yes_df['timestamp'], errors='coerce')
cdn_df['timestamp'] = pd.to_datetime(cdn_df['timestamp'], errors='coerce')

yes_df.sort_values(['timestamp', 'URL'], ascending=[True, True], inplace=True)
cdn_df.sort_values(['timestamp', 'URL'], ascending=[True, True], inplace=True)

merged_df = pd.merge_asof(
    cdn_df, 
    yes_df, 
    on='timestamp', 
    by='URL', 
    tolerance=pd.Timedelta('1 minute'), 
    direction='nearest', 
    suffixes=('_yes', '_cdn')
)


merged_df.to_csv('mergymerge.csv', index=False)
print(f"Number of output rows: {len(merged_df)}")
