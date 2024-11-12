import pandas as pd
import numpy as np


df = pd.read_csv('url_time_sorted_yes.csv')
print(f"Number of output rows: {len(df)}")

df = df[df['ReturnCode'] != 304]

df.to_csv('url_time_sorted_yes_no_304.csv', index=False)
print(f"Number of output rows: {len(df)}")
