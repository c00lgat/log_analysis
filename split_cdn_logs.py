import pandas as pd

# Read the CSV file into a DataFrame
csv_df = pd.read_csv('sorted_csv.csv')

# Calculate the total number of rows
total_rows = len(csv_df)

# Define the size of each part (e.g., 500,000 rows)
part_size = 558187

# Ensure that the DataFrame has enough rows
if total_rows < part_size * 3:
    raise ValueError(f"The CSV file must have at least {part_size * 3} rows.")

# Split the DataFrame into three parts
first_part = csv_df.iloc[:part_size]
second_part = csv_df.iloc[part_size:part_size*2]
third_part = csv_df.iloc[part_size*2:part_size*3]

# Save each part to a separate CSV file
first_part.to_csv('first_part.csv', index=False)
second_part.to_csv('second_part.csv', index=False)
third_part.to_csv('third_part.csv', index=False)
