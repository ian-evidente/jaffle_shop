import os
import json
import argparse
import pandas as pd


class DbtColumnMapper:
    def __init__(self, artifact_path: str = None):
        self.artifact_path = artifact_path or '../target'
        self.catalog_path = os.path.join(self.artifact_path, 'catalog.json')
        self.manifest_path = os.path.join(self.artifact_path, 'manifest.json')

    def get_columns(self, dataset: str) -> pd.DataFrame:
        columns_data = []

        # Read the catalog.json file
        with open(self.catalog_path, 'r') as f:
            catalog_data = json.load(f)

        # Iterate through nodes to find the specified model
        for node_name, node_data in catalog_data.get('nodes', {}).items():
            if node_name.endswith(dataset):
                # Extract columns for the found model
                for col_name in node_data.get('columns', {}).keys():
                    columns_data.append({'model_name': dataset, 'column_name': col_name})
                break  # Stop iteration once the model is found

        return pd.DataFrame(columns_data)

    def get_model_depends_on(self, model: str) -> pd.DataFrame:
        dependencies = []

        # Read manifest.json file
        with open(self.manifest_path, 'r') as f:
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
                    dependencies.append(
                        {'model_name': model, 'depends_on': node.split('.')[-1], 'node_type': node_type})
                break  # Stop searching once the model is found

        return pd.DataFrame(dependencies)

    def get_model_compiled_code(self, model: str) -> str:
        # Read manifest.json file
        with open(self.manifest_path, 'r') as f:
            manifest_data = json.load(f)

        compiled_code = ""
        # Iterate over nodes in the manifest
        for node_name, node_data in manifest_data.get('nodes', {}).items():
            # Check if the node matches the specified model
            if node_name.endswith(model):
                # Retrieve the compiled code for the specified model
                compiled_code = node_data.get('compiled_code', '')
                break

        return compiled_code


def main():
    parser = argparse.ArgumentParser(description="dbt Column Mapper")
    parser.add_argument("-s", "--select", type=str, help="Specify the model name")
    args = parser.parse_args()
    model = args.select

    if model:
        dbt_mapper = DbtColumnMapper()
        columns_df = dbt_mapper.get_columns(model)
        depends_on_df = dbt_mapper.get_model_depends_on(model)
        compiled_code = dbt_mapper.get_model_compiled_code(model)
        print("Columns:")
        print(columns_df)
        print("\nDependencies:")
        print(depends_on_df)
        print("\nCompiled Code:")
        print(compiled_code)
    else:
        print("Please specify the model name using the -s/--model option.")


if __name__ == "__main__":
    main()
