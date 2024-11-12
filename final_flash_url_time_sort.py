import pandas as pd

final_df = pd.read_csv('final_fusion.csv')

final_df['URL'] = final_df['URL'].astype(str)
final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], errors='coerce')

final_df.sort_values(['URL', 'timestamp'], ascending=[True, True], inplace=True)

final_df.reset_index(drop=True, inplace=True)

final_df.to_csv('final_flash_vegito.csv', index=False)
print(f"Number of output rows: {len(final_df)}")