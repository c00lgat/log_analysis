import pandas as pd

cdn_df = pd.read_csv('full_url_cdn_logs.csv')
yes_df = pd.read_csv('sorted_xlsx.csv')


# Rename columns for consistency
yes_df.rename(columns={'Url': 'URL', 't': 'timestamp'}, inplace=True)
cdn_df.rename(columns={'new_url': 'URL', 'statuscode': 'ReturnCode', 'dt_utc': 'timestamp'}, inplace=True)



# Sort both DataFrames by 'URL' and 'timestamp' in ascending order
yes_df.sort_values(['ReturnCode', 'URL'], ascending=[True, True], inplace=True)
cdn_df.sort_values(['ReturnCode', 'URL'], ascending=[True, True], inplace=True)

# Convert 'URL' columns to string
yes_df['URL'] = yes_df['URL'].astype(str)
cdn_df['URL'] = cdn_df['URL'].astype(str)

# Convert 'URL' columns to string
yes_df['ReturnCode'] = yes_df['ReturnCode'].astype(str)
cdn_df['ReturnCode'] = cdn_df['ReturnCode'].astype(str)

# Sort both DataFrames by 'URL' and 'timestamp' in ascending order
yes_df.sort_values(['ReturnCode', 'URL'], inplace=True)
cdn_df.sort_values(['ReturnCode', 'URL'], inplace=True)

print(f"Unique status codes in CDN logs: {cdn_df.ReturnCode.unique()}")

cdn_df = cdn_df[cdn_df['ReturnCode'] == '304']
yes_df = yes_df[yes_df['ReturnCode'] == '304']

cdn_df.to_csv('cdn_304_only.csv', index=False)
print(f"Number of output rows: {len(cdn_df)}")

yes_df.to_csv('yes_304_only.csv', index=False)
print(f"Number of output rows: {len(yes_df)}")
# yes_df.to_csv('http_sort_yes.csv', index=False)
# print(f"Number of output rows: {len(yes_df)}")

# cdn_df.to_csv('http_sort_cdn.csv', index=False)
# print(f"Number of output rows: {len(cdn_df)}")

