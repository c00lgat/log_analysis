import pandas as pd
import numpy as np

# Load your data
csv_file = 'RAW_CDN_Logs_12-9-2024.csv'
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(csv_file, encoding='latin1', on_bad_lines='skip')

# Load the two XLSX files and concatenate them into one DataFrame
xlsx_df_a = pd.read_excel(xlsx_file_a)
xlsx_df_b = pd.read_excel(xlsx_file_b)
xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)

print("Data type of 't' column:", xlsx_df['t'].dtype)
print("Data type of 'dt_utc' column csv:", csv_df['dt_utc'].dtype)

csv_df['dt_utc'] = pd.to_datetime(csv_df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

print("Data type of 't' column:", xlsx_df['t'].dtype)
print("Data type of 'dt_utc' column csv:", csv_df['dt_utc'].dtype)

xlsx_df = xlsx_df.sort_values('t').reset_index(drop=True)
csv_df = csv_df.sort_values('dt_utc').reset_index(drop=True)

xlsx_df.to_csv('sorted_xlsx.csv', index=False)
print(f"Number of matched rows: {len(xlsx_df)}")

csv_df.to_csv('sorted_csv.csv', index=False)
print(f"Number of matched rows: {len(csv_df)}")