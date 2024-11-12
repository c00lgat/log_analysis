import pandas as pd
import numpy as np


# Load your data
csv_file = 'RAW_CDN_Logs_12-9-2024.csv'
xlsx_file_a = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'
xlsx_file_b = '2024-09-11-UTC_Time_Logs_Part-B.xlsx'
filtered_cdn = 'RAW_CDN_Logs_12-9-2024.csv'
yes = 'merged_yes_cdn_no_304.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(yes)

yes_bytes = df['b'].sum()
cdn_bytes = df['bytes'].sum()

print(f'Yes bytes: {yes_bytes:,}')
print(f'CDN bytes: {cdn_bytes:,}')
print(f"Number of output rows: {len(df)}")
# df_yes = pd.read_csv(yes)
# cdn_duplicates = cdn_df['merge_key'].duplicated().sum()
# print(f"Number of duplicate merge keys in cdn_df: {cdn_duplicates}")
# print(df['ReturnCode'].value_counts()[304])

# yes_duplicates = yes_df['merge_key'].duplicated().sum()
# print(f"Number of duplicate merge keys in yes_df: {yes_duplicates}")

# non_null_count = yes_df['Url'].count()
# print(f"Number of non-null values in 'non_null_count': {non_null_count}")


# total_yes_bytes = df_yes['b'].sum()
# print(f"Total in 'bytes' column in YES logs: {total_yes_bytes:,d}")

# total_cdn_bytes = df['bytes'].sum()
# total_cdn_totalbytes = df['totalBytes'].sum()

# print(f"Total in 'bytes' column in CDN logs: {total_cdn_bytes:,d}")
# print(f"Total in 'totalbytes' column in CDN logs: {total_cdn_totalbytes:,d}")


# unique_keys = df['statuscode'].nunique()
# print(f"Number of unique values in 'cdn': {unique_keys}")
# print(f"Unique status codes in YES logs{df_yes.ReturnCode.unique()}")
# non_empty_mask = df['t'].notnull() & (df['t'] != '')
# non_empty_count = non_empty_mask.sum()
# print(f"Number of non-empty cells in 't': {non_empty_count}")


# Load the two XLSX files and concatenate them into one DataFrame
# xlsx_df_a = pd.read_excel(xlsx_file_a)
# xlsx_df_b = pd.read_excel(xlsx_file_b)
# xlsx_df = pd.concat([xlsx_df_a, xlsx_df_b], ignore_index=True)


# print(len(pd.read_csv(csv_file, index_col=0, nrows=0).columns.tolist()))
# Normalize column names
# xlsx_df.columns = xlsx_df.columns.str.strip().str.lower()
# csv_df.columns = csv_df.columns.str.strip().str.lower()

# print(f'Number of rows in CDN Logs: {len(csv_df)}')
# print(f'Number of rows in Yes Logs: {len(xlsx_df)}')

#print(csv_df['cliip'].unique())
#print(csv_df['cliip'].nunique())