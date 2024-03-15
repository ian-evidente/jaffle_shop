import os
import json
import pandas as pd


def get_columns(dataset: str) -> pd.DataFrame:
    columns_data = []

    # Get the path of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Adjust the path to the catalog.json file
    catalog_path = os.path.join(script_dir, '../target/catalog.json')

    # Read the catalog.json file
    with open(catalog_path, 'r') as f:
        catalog_data = json.load(f)

    # Iterate through nodes to find the specified model
    for node_name, node_data in catalog_data.get('nodes', {}).items():
        if node_name.endswith(dataset):
            # Extract columns for the found model
            for col_name in node_data.get('columns', {}).keys():
                columns_data.append({'model_name': dataset, 'column_name': col_name})
            break  # Stop iteration once the model is found

    return pd.DataFrame(columns_data)


# Example usage:
model_name = "customers"
df = get_columns(dataset=model_name)
print(df)
