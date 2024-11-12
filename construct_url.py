import pandas as pd
import numpy as np

# Reconstruct the full URL in the CSV DataFrame
def reconstruct_url(row):
    url = 'http://fykswkmjb.filerobot.com/'
    reqpath = row['reqpath'] #if pd.notnull(row['reqpath']) else ''
    
    if row['querystr'] != '-':
        querystr = row['querystr'] #if pd.notnull(row['querystr']) else ''
    else:
        querystr = ''

    url = url + reqpath + querystr
    return url

# Load your data
csv_file = 'fixed_cdn_logs.csv'
csv_df = pd.read_csv(csv_file)
csv_df.columns = csv_df.columns.str.strip().str.lower()

# new_url = (
#     'http://fykswkmjb.filerobot.com/' +
#     csv_df['reqpath'] +
#     np.where(csv_df['querystr'] != '-', csv_df['querystr'], '')
# )

csv_df2 = csv_df.assign(
    new_url=lambda x:
     'http://fykswkmjb.filerobot.com/' + 
     x.reqpath + 
     np.where(x.querystr != '-', '?' + x.querystr, ''))

# csv_df['full_url'] = csv_df.apply(reconstruct_url, axis=1)

csv_df2.to_csv('full_url_cdn_logs.csv', index=False)
print(f"Number of output rows: {len(csv_df)}")
