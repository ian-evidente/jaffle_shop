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


def get_model_depends_on(model: str) -> pd.DataFrame:
    dependencies = []

    # Get the path of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Adjust the path to the manifest.json file
    manifest_path = os.path.join(script_dir, '../target/manifest.json')

    # Read manifest.json file
    with open(manifest_path, 'r') as f:
        manifest_data = json.load(f)

    # Iterate over nodes in the manifest
    for node_name, node_data in manifest_data.get('nodes', {}).items():
        # Check if the node matches the specified model
        if node_name.endswith(model):
            # Get dependencies for the specified model
            model_dependencies = node_data.get('depends_on', {})
            # Iterate over the nodes in dependencies
            for node in model_dependencies.get('nodes', []):
                # Extract the node type from the node name
                node_type = node.split('.')[0]
                dependencies.append({'model_name': model, 'depends_on': node.split('.')[-1], 'node_type': node_type})
            break  # Stop searching once the model is found

    return pd.DataFrame(dependencies)

