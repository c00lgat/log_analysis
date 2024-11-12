import pandas as pd


final_df = pd.read_csv('final_flash_vegito.csv')

# final_df['ReturnCode'] = final_df['ReturnCode'].astype(str).str.strip()
final_df_no_304 = final_df[final_df['ReturnCode'] != 304]

final_df_no_304.to_csv('final_flash_vegito_no_304.csv', index=False)
print(f"Number of output rows: {len(final_df_no_304)}")