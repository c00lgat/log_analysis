import pandas as pd
import numpy as np


# Load your data
csv_file = 'RAW_CDN_Logs_12-9-2024.csv'
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(csv_file)

# Load the two XLSX files and concatenate them into one DataFrame
xlsx_df_a = pd.read_excel(xlsx_file_a)
xlsx_df_b = pd.read_excel(xlsx_file_b)
xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)

# Normalize column names
xlsx_df.columns = xlsx_df.columns.str.strip().str.lower()
csv_df.columns = csv_df.columns.str.strip().str.lower()

print(f'Number of rows in CDN Logs: {len(csv_df)}')
print(f'Number of rows in Yes Logs: {len(xlsx_df)}')

#print(csv_df['cliip'].unique())
#print(csv_df['cliip'].nunique())