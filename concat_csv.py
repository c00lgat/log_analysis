import pandas as pd

csv_df_1 = pd.read_csv('first_part.csv')
csv_df_2 = pd.read_csv('second_part.csv')
csv_df_3 = pd.read_csv('third_part.csv')

concat_csv = pd.concat([csv_df_1, csv_df_2, csv_df_3])

concat_csv.to_csv('fixed_cdn_logs.csv', index=False)