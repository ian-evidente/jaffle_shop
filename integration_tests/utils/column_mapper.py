import os
import json
import argparse
import pandas as pd
import re


class DbtColumnMapper:
    def __init__(self, artifact_path: str = None):
        self.artifact_path = artifact_path or '../target'
        self.catalog_path = os.path.join(self.artifact_path, 'catalog.json')
        self.manifest_path = os.path.join(self.artifact_path, 'manifest.json')

    def get_columns(self, datasets: list[str]) -> pd.DataFrame:
        columns_data = []

        with open(self.catalog_path, 'r') as f:
            catalog_data = json.load(f)

        for dataset in datasets:
            for node_name, node_data in catalog_data.get('nodes', {}).items():
                if node_name.endswith(dataset):
                    for col_name in node_data.get('columns', {}).keys():
                        columns_data.append({'model_name': dataset, 'column_name': col_name})
                    break

        return pd.DataFrame(columns_data)

    def get_model_dependencies(self, model: str) -> pd.DataFrame:
        dependencies = []

        with open(self.manifest_path, 'r') as f:
            manifest_data = json.load(f)

        for node_name, node_data in manifest_data.get('nodes', {}).items():
            if node_name.endswith(model):
                model_dependencies = node_data.get('depends_on', {})
                for node in model_dependencies.get('nodes', []):
                    node_type = node.split('.')[0]
                    dependencies.append({'model_name': model, 'depends_on': node.split('.')[-1], 'node_type': node_type})
                break

        return pd.DataFrame(dependencies)

    def get_compiled_code(self, model: str) -> str:
        with open(self.manifest_path, 'r') as f:
            manifest_data = json.load(f)

        compiled_code = ""
        for node_name, node_data in manifest_data.get('nodes', {}).items():
            if node_name.endswith(model):
                compiled_code = node_data.get('compiled_code', '')
                break

        return compiled_code

    @staticmethod
    def replace_final_select_columns(sql_query: str, columns: list) -> str:
        last_select_match = re.finditer(r'select\s+(?:(?!\bselect\b).)*$', sql_query, re.IGNORECASE | re.MULTILINE)
        last_select_indices = [match.span() for match in last_select_match]
        if last_select_indices:
            last_select_start, last_select_end = last_select_indices[-1]
            last_select_statement = sql_query[last_select_start:last_select_end]
            modified_last_select = re.sub(r'\*', ', '.join(columns), last_select_statement)
            modified_sql_query = sql_query[:last_select_start] + modified_last_select + sql_query[last_select_end:]
            return modified_sql_query
        else:
            return sql_query

    @staticmethod
    def replace_cte_select_columns(cte_query: str, cte_table: str, columns: list) -> str:
        pattern = rf'(\b[a-z_]*\b\s+as\s+\(\s*select\s+)(\*)\s+(from\s+{cte_table}\s*\))'
        modified_query = re.sub(pattern, rf'\1{", ".join(columns)} \3', cte_query, flags=re.IGNORECASE)
        return modified_query

    def reformat_compiled_code(self, model: str) -> str:
        compiled_code = self.get_compiled_code(model=model)
        reformatted = compiled_code.replace('`', '').replace('"', '')
        pattern = r'\b\w+[-]\w+\.\w+\.\w+\b'
        reformatted = re.sub(pattern, lambda x: x.group().split('.')[-1], reformatted)

        model_columns = self.get_columns(datasets=[model])['column_name'].tolist()
        reformatted = self.replace_final_select_columns(sql_query=reformatted, columns=model_columns)

        deps = self.get_model_dependencies(model=model)['depends_on'].tolist()
        for d in deps:
            d_columns = self.get_columns(datasets=[d])['column_name'].tolist()
            reformatted = self.replace_cte_select_columns(cte_query=reformatted, cte_table=d, columns=d_columns)

        return reformatted


def main():
    parser = argparse.ArgumentParser(description="dbt Column Mapper")
    parser.add_argument("-s", "--select", type=str, help="Specify the model name")
    args = parser.parse_args()
    model = args.select

    if model:
        dbt_mapper = DbtColumnMapper()
        columns_df = dbt_mapper.get_columns([model])
        depends_on_df = dbt_mapper.get_model_dependencies(model)
        reformatted_code = dbt_mapper.reformat_compiled_code(model)
        print("Columns:")
        print(columns_df)
        print("\nDependencies:")
        print(depends_on_df)
        print("\nReformatted Compiled Code:")
        print(reformatted_code)
    else:
        print("Please specify the model name using the -s/--model option.")


if __name__ == "__main__":
    main()
