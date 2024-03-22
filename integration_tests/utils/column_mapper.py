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

    def get_node_columns(self, node: str) -> pd.DataFrame:
        columns_data = []

        with open(self.catalog_path, 'r') as f:
            catalog_data = json.load(f)

        for node_name, node_data in catalog_data.get('nodes', {}).items():
            if node_name.endswith(f'.{node}'):
                for col_name in node_data.get('columns', {}).keys():
                    columns_data.append({'node_name': node, 'column_name': col_name})
                break

        for node_name, node_data in catalog_data.get('sources', {}).items():
            if node_name.endswith(f'.{node}'):
                for col_name in node_data.get('columns', {}).keys():
                    columns_data.append({'node_name': node, 'column_name': col_name})
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
                    dependencies.append(
                        {'model_name': model, 'dependency': node.split('.')[-1], 'type': node_type}
                    )
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
        last_select_match = re.finditer(r"select\s+(?:(?!\bselect\b).)*$", sql_query, re.IGNORECASE | re.MULTILINE)
        last_select_indices = [match.span() for match in last_select_match]
        if last_select_indices:
            last_select_start, last_select_end = last_select_indices[-1]
            last_select_statement = sql_query[last_select_start:last_select_end]
            modified_last_select = re.sub(r"\*", ", ".join(columns), last_select_statement)
            modified_sql_query = sql_query[:last_select_start] + modified_last_select + sql_query[last_select_end:]
            return modified_sql_query
        else:
            return sql_query

    @staticmethod
    def replace_cte_select_columns(cte_query: str, cte_table: str, columns: list) -> str:
        pattern = rf"(\b[a-z_]*\b\s+as\s+\(\s*select\s+)(\*)\s+(from\s+{cte_table}\s*\))"
        modified_query = re.sub(pattern, rf"\1{', '.join(columns)} \3", cte_query, flags=re.IGNORECASE)
        return modified_query

    def reformat_compiled_code(self, model: str) -> str:
        compiled_code = self.get_compiled_code(model=model)
        lowered = compiled_code.lower()
        flattened = ' '.join(lowered.split())
        quotes_removed = flattened.replace('`', '').replace('"', '')
        ref_replaced = re.sub(r"\b\w+-\w+\.\w+\.\w+\b", lambda x: x.group().split(".")[-1], quotes_removed)
        model_columns = self.get_node_columns(node=model)['column_name'].tolist()
        reformatted = self.replace_final_select_columns(sql_query=ref_replaced, columns=model_columns)
        deps = self.get_model_dependencies(model=model)['dependency'].tolist()
        for d in deps:
            d_columns = self.get_node_columns(node=d)['column_name'].tolist()
            reformatted = self.replace_cte_select_columns(cte_query=reformatted, cte_table=d, columns=d_columns)

        return reformatted

    @staticmethod
    def get_cte_definitions(sql_query: str) -> dict:
        cte_names = re.findall(r"(?:(?<=with )|(?<=\), ))(.+?)(?= as \()", sql_query)
        cte_definitions = re.findall(r"(?<= as \( )(.+?)(?= \))", sql_query)
        cte_info = dict(zip(cte_names, cte_definitions))

        return cte_info

    def get_cte_dependencies(self, sql_query: str) -> pd.DataFrame:
        cte_info = self.get_cte_definitions(sql_query=sql_query)
        return_list = []
        for cte, definition in cte_info.items():
            deps = re.findall(r"(?<= from | join )(.+?)(?=$| group | where | on | join | inner | left | right | full "
                              r"| outer | cross )", definition)
            for dep in deps:
                return_list.append({'cte_name': cte, 'dependency': dep, 'type': 'cte'})

        cte_list = []
        for r in return_list:
            cte_list.append(r['cte_name'])

        for r in return_list:
            if r['dependency'] not in cte_list:
                r['type'] = 'model'

        dependencies_df = pd.DataFrame(return_list)
        return dependencies_df

    def get_cte_columns_info(self, sql_query: str) -> pd.DataFrame:
        cte_info = self.get_cte_definitions(sql_query=sql_query)
        columns_list = []
        for cte, definition in cte_info.items():
            column_definitions = re.findall(r'(?<=select )(.+?)(?= from )', definition)[0].split(', ')
            for cd in column_definitions:
                column_name = re.findall(r'(?:(?<=^)|(?<= )|(?<=\.))\w+(?=$)', cd)[0]
                column_source = re.findall(r'\w+?(?=\.)', cd)
                try:
                    column_source = column_source[0]
                except IndexError:
                    column_source = 'UNKNOWN'

                columns_list.append({
                    'cte_name': cte,
                    'column_definition': cd,
                    'column_name': column_name,
                    'column_source': column_source
                })
        columns_df = pd.DataFrame(columns_list)

        return columns_df


def main():
    parser = argparse.ArgumentParser(description="dbt Column Mapper")
    parser.add_argument("-s", "--select", type=str, help="Specify the model name")
    args = parser.parse_args()
    model = args.select or 'orders'

    if model:
        dbt_mapper = DbtColumnMapper()
        columns_df = dbt_mapper.get_node_columns(model)
        depends_on_df = dbt_mapper.get_model_dependencies(model)
        reformatted_code = dbt_mapper.reformat_compiled_code(model)
        cte_definitions = dbt_mapper.get_cte_definitions(reformatted_code)
        cte_dependencies_df = dbt_mapper.get_cte_dependencies(reformatted_code)
        cte_columns_df = dbt_mapper.get_cte_columns_info(reformatted_code)

        print("Model Columns:")
        print(columns_df.to_string(index=False, justify='right'))
        print("\nModel Dependencies:")
        print(depends_on_df.to_string(index=False, justify='right'))
        print("\nCTE Definitions:")
        for k, v in cte_definitions.items():
            print(f'{k}: {v}')
        print("\nCTE Dependencies:")
        print(cte_dependencies_df.to_string(index=False, justify='right'))
        print("\nCTE Columns:")
        print(cte_columns_df.to_string(index=False, justify='right'))
    else:
        print("Please specify the model name using the -s/--model option.")


if __name__ == "__main__":
    main()
