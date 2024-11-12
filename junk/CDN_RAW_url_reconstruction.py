import pandas as pd

# Step 1: Read the CSV file
csv_file = '2024-09-11-UTC_Time_Logs_Part-A.xlsx'  # Replace with your actual CSV file name
df = pd.read_csv(csv_file)

# Step 2: Convert 'dt_utc' column to datetime
df['dt_utc'] = pd.to_datetime(df['dt_utc'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

# Step 3: Drop rows with invalid or missing 'dt_utc' values (optional)
# df.dropna(subset=['dt_utc'], inplace=True)

# Step 4: Sort the DataFrame chronologically
df_sorted = df.sort_values(by='dt_utc')

# # Step 5: Reset the index (optional)
# df_sorted.reset_index(drop=True, inplace=True)

# Step 6: Output the sorted DataFrame to a new CSV file
output_file = 'sorted_RAW_CDN_Logs_12-9-2024.csv'  # You can name this file as you like
df_sorted.to_csv(output_file, index=False)

print(f"Sorted CSV file has been saved to '{output_file}'.")
