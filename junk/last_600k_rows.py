import pandas as pd
import numpy as np


csv_file = 'RAW_CDN_Logs_12-9-2024.csv'

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(csv_file, encoding='latin1', on_bad_lines='skip')

csv_df['dt_utc'] = pd.to_datetime(csv_df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

csv_df = csv_df.sort_values('dt_utc').reset_index(drop=True)

last_rows = csv_df.tail(630000)

last_rows.to_csv('last_rows.csv', index=False)
print(f"Number of matched rows: {len(csv_df)}")