# Import required libraries
import glob    # Used to find files matching a pattern (e.g. all part-* files)
import json    # Used to read and parse the schemas.json file
import os      # Used to create directories and manipulate file paths
import pandas as pd  # Used to read CSV files and add headers


# Step 1: Load the schema (column names) from schemas.json

# Define the path to the schemas.json file inside retail_db
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RETAIL_DB = os.path.join(BASE_DIR, "retail_db")
schema_file_path = os.path.join(RETAIL_DB, "schemas.json")

# Open and parse the JSON file into a Python dictionary
with open(schema_file_path, "r") as f:
    schemas = json.load(f)

print("schemas.json loaded successfully.\n")


# Step 2: Process each dataset defined in the schema

# Iterate over each dataset in the schema
for dataset_name, columns_info in schemas.items():

    print(f"Processing dataset: '{dataset_name}'...")

    # Step 2a: Extract and sort column names by their position

    # Each entry in columns_info has: column_name, data_type, column_position
    # We sort by 'column_position' to ensure the correct order of columns
    sorted_columns = sorted(columns_info, key=lambda col: col["column_position"])

    # Extract just the column names in the correct order
    column_names = [col["column_name"] for col in sorted_columns]

    print(f"   Columns found: {column_names}")

    # Step 2b: Find all data files for this dataset

    # Use glob to find all files matching the pattern inside the dataset folder
    # for example the 'retail_db/customers/part-*' will match 'retail_db/customers/part-00000'
  
    file_pattern = f"retail_db/{dataset_name}/part-*"
    matching_files = glob.glob(file_pattern)

    # If no files are found, skip this dataset and warn
    if not matching_files:
        print(f"No files found for '{dataset_name}', skipping.\n")
        continue

    # Step 2c: Read each file, add headers, and save to new_dataset

    for file_path in matching_files:

        # Read the CSV file without a header (header=None)
        # and assign the column names we extracted from the schema
      
        df = pd.read_csv(file_path, header=None, names=column_names)

        # Build the output path by replacing 'retail_db' with 'new_dataset'
        # like 'retail_db/customers/part-00000' → 'new_dataset/customers/part-00000'
        output_path = file_path.replace("retail_db", "new_dataset", 1)

        # Create the output directory if it doesn't already exist
        # os.makedirs with exist_ok=True avoids errors if the folder already exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save the DataFrame as a CSV file with the header included
        # index=False prevents pandas from writing row numbers into the file
        df.to_csv(output_path, index=False)

        print(f"Saved: {output_path}")

    print()  # Blank line between datasets for readability

print("All datasets processed! Files saved in the 'new_dataset' folder.")
