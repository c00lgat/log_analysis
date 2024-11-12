import pandas as pd
import numpy as np

# Load your data
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'

# Load the two XLSX files and concatenate them into one DataFrame
xlsx_df_a = pd.read_excel(xlsx_file_a)
xlsx_df_b = pd.read_excel(xlsx_file_b)
xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)

xlsx_df['dt_utc'] = pd.to_datetime(xlsx_df['t'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
xlsx_df = xlsx_df.sort_values('dt_utc').reset_index(drop=True)

xlsx_df.to_csv('sorted_xlsx.csv', index=False)
print(f"Number of matched rows: {len(xlsx_df)}")