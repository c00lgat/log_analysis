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
csv_file = 'sorted_csv.csv'
csv_df = pd.read_csv(csv_file)

csv_df.columns = csv_df.columns.str.strip().str.lower()

csv_df['full_url'] = csv_df.apply(reconstruct_url, axis=1)

csv_df.to_csv('full_url_csv.csv', index=False)
print(f"Number of output rows: {len(csv_df)}")
