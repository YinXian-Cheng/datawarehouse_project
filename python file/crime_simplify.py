import pandas as pd

# Load the CSV file
file_path = 'crime_simplified1.csv'
df = pd.read_csv(file_path)

# Drop rows where the index is a multiple of 2
df_filtered = df.iloc[~(df.index % 2 == 0)]

# Save the modified CSV file
output_file_path = 'crime_simplified2.csv'
df_filtered.to_csv(output_file_path, index=False)

print(f"Filtered CSV saved to {output_file_path}")
