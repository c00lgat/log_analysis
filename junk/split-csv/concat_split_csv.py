import pandas as pd

# List of CSV files to concatenate
csv_files = ['first_part.csv', 'second_part.csv', 'third_part.csv']

# Read each CSV file into a DataFrame
dfs = [pd.read_csv(f) for f in csv_files]

# Concatenate the DataFrames
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a new CSV file
combined_df.to_csv('combined.csv', index=False)

# Read the combined CSV file
combined_df = pd.read_csv('combined.csv')

# Check the number of rows
print(f"Total rows (excluding header): {len(combined_df)}")