import pandas as pd
import numpy as np

yes_logs = 'sorted_xlsx.csv'

df = pd.read_csv(yes_logs)

filtered_df = df[df['ReturnCode'] != 304]

filtered_df.to_csv('filtered_yes_no_304.csv', index=False)
print(f"Number of output rows: {len(filtered_df)}")