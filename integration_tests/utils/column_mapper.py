import os
import json
import argparse
import re
import pandas as pd
from pandasql import sqldf


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
                    columns_data.append({'node': node, 'type': 'model', 'column': col_name})
                break

        for node_name, node_data in catalog_data.get('sources', {}).items():
            if node_name.endswith(f'.{node}'):
                for col_name in node_data.get('columns', {}).keys():
                    columns_data.append({'node': node, 'type': 'source', 'column': col_name})
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
                        {'model': model, 'dependency': node.split('.')[-1], 'type': node_type}
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
        model_columns = self.get_node_columns(node=model)['column'].tolist()
        reformatted = self.replace_final_select_columns(sql_query=ref_replaced, columns=model_columns)
        deps = self.get_model_dependencies(model=model)['dependency'].tolist()
        for d in deps:
            d_columns = self.get_node_columns(node=d)['column'].tolist()
            reformatted = self.replace_cte_select_columns(cte_query=reformatted, cte_table=d, columns=d_columns)

        return reformatted

    @staticmethod
    def get_cte_definitions(sql_query: str) -> dict:
        cte_names = re.findall(r"(?:(?<=with )|(?<=\), ))(.+?)(?= as \()", sql_query)
        cte_definitions = re.findall(r"(?<= as \( )(.+?)(?= \))", sql_query)
        cte_info = dict(zip(cte_names, cte_definitions))

        return cte_info

    def get_cte_dependencies(self, model: str) -> pd.DataFrame:
        sql_query = self.reformat_compiled_code(model=model)
        cte_info = self.get_cte_definitions(sql_query=sql_query)
        return_list = []
        for cte, definition in cte_info.items():
            deps = re.findall(r"(?<= from | join )(.+?)(?=$| group | where | on | join | inner | left | right | full "
                              r"| outer | cross )", definition)
            for dep in deps:
                return_list.append({'cte': cte, 'dependency': dep, 'type': 'cte'})

        cte_list = []
        for r in return_list:
            cte_list.append(r['cte'])

        for r in return_list:
            if r['dependency'] not in cte_list:
                r['type'] = 'model'

        model_deps = self.get_model_dependencies(model=model)
        cte_deps = pd.DataFrame(return_list)
        query = """
            SELECT
                cte_deps.cte,
                cte_deps.dependency,
                COALESCE(model_deps.type, cte_deps.type) as type
            FROM cte_deps
            LEFT JOIN model_deps
                ON cte_deps.dependency = model_deps.dependency
                AND model_deps.dependency = 'source'
        """
        final_deps = sqldf(query, locals())
        return final_deps

    def get_cte_columns_info(self, model: str) -> pd.DataFrame:
        sql_query = self.reformat_compiled_code(model=model)
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
                    'model': model,
                    'cte': cte,
                    'column': column_name,
                    'source': column_source
                })

        cte_columns = pd.DataFrame(columns_list)
        cte_deps = self.get_cte_dependencies(model=model)

        query = """
            SELECT cte, COUNT(distinct dependency) as deps
            FROM cte_deps
            GROUP BY cte
            HAVING COUNT(distinct dependency) = 1
        """
        single_dep = sqldf(query, locals())

        query = """
            SELECT cte_deps.*
            FROM cte_deps
            JOIN single_dep
                ON cte_deps.cte = single_dep.cte
        """
        cte_deps2 = sqldf(query, locals())

        query = """
            SELECT 
                cte_columns.model,
                cte_columns.cte,
                cte_columns.column,
                COALESCE(cte_deps2.dependency, cte_columns.source) as source
            FROM cte_columns
            LEFT JOIN cte_deps2
                ON cte_columns.cte = cte_deps2.cte
        """
        cte_columns2 = sqldf(query, locals())

        return cte_columns2


def main():
    parser = argparse.ArgumentParser(description="dbt Column Mapper")
    parser.add_argument("-s", "--select", type=str, help="Specify the model name")
    args = parser.parse_args()
    model = args.select or 'customers'

    if model:
        dbt_mapper = DbtColumnMapper()

        reformatted_code = dbt_mapper.reformat_compiled_code(model)
        print("\nReformatted Model:")
        print(reformatted_code)

        node_columns_df = dbt_mapper.get_node_columns(model)
        print("\nModel Columns:")
        print(node_columns_df.to_string(index=False, justify='right'))

        model_dependencies_df = dbt_mapper.get_model_dependencies(model)
        print("\nModel Dependencies:")
        print(model_dependencies_df.to_string(index=False, justify='right'))

        cte_dependencies_df = dbt_mapper.get_cte_dependencies(model)
        print("\nCTE Dependencies:")
        print(cte_dependencies_df.to_string(index=False, justify='right'))

        cte_def_dict = dbt_mapper.get_cte_definitions(reformatted_code)
        print("\nCTE Definitions:")
        for cte, definition in cte_def_dict.items():
            print(f"{cte}: {definition}")

        cte_columns_info_df = dbt_mapper.get_cte_columns_info(model)
        print("\nCTE Columns:")
        print(cte_columns_info_df.to_string(index=False, justify='right'))

        node_columns_df.to_csv('node_columns.csv', index=False)
        model_dependencies_df.to_csv('model_dependencies.csv', index=False)
        cte_dependencies_df.to_csv('cte_dependencies.csv', index=False)
        cte_columns_info_df.to_csv('cte_columns_info.csv', index=False)

    else:
        print("Please specify the model name using the -s/--model option.")


if __name__ == "__main__":
    main()
