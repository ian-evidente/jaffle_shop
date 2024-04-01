import os
import json
import argparse
import re
import pandas as pd
from pandasql import sqldf


class DbtColumnMapper:
    def __init__(self, artifact_path: str = None):
        self.artifact_path = artifact_path or '../../target'
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
        modified_query = re.sub(pattern, rf"\1{', '.join(columns)} \3", cte_query, flags=re.IGNORECASE)
        return modified_query

    def reformat_compiled_code(self, model: str) -> str:
        compiled_code = self.get_compiled_code(model=model)
        no_comments = re.sub(r'--.*|/\*.*?\*/', '', compiled_code, flags=re.DOTALL)
        no_quotes = no_comments.replace('`', '').replace('"', '')
        lowered = no_quotes.lower()
        flattened = ' '.join(lowered.split())
        ref_replaced = re.sub(r'\b\w+-\w+\.\w+\.\w+\b', lambda x: x.group().split('.')[-1], flattened)
        model_columns = self.get_node_columns(node=model)['column'].tolist()
        reformatted = self.replace_final_select_columns(sql_query=ref_replaced, columns=model_columns)
        deps = self.get_model_dependencies(model=model)['dependency'].tolist()
        for d in deps:
            d_columns = self.get_node_columns(node=d)['column'].tolist()
            reformatted = self.replace_cte_select_columns(cte_query=reformatted, cte_table=d, columns=d_columns)

        return reformatted

    @staticmethod
    def get_cte_definitions(sql_query: str) -> dict:
        cte_names = re.findall(r'(?:(?<=with )|(?<=\), ))(.+?)(?= as \()', sql_query, re.IGNORECASE)
        cte_definitions = re.findall(r'(?<= as \( )(.+?)(?= \))', sql_query, re.IGNORECASE)
        cte_info = dict(zip(cte_names, cte_definitions))

        return cte_info

    def get_cte_dependencies(self, model: str) -> pd.DataFrame:
        sql_query = self.reformat_compiled_code(model=model)
        cte_info = self.get_cte_definitions(sql_query=sql_query)
        return_list = []
        for cte, definition in cte_info.items():
            deps = re.findall(
                r'(?<= from | join )(.+?)(?=$| group | where | on | join | inner | left | right | full | outer | cross )',
                definition, re.IGNORECASE
            )
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

    def get_cte_columns_source_info(self, model: str) -> pd.DataFrame:
        sql_query = self.reformat_compiled_code(model=model)
        cte_info = self.get_cte_definitions(sql_query=sql_query)
        columns_list = []
        for cte, definition in cte_info.items():
            column_sql = re.findall(r'(?<=select )(.+?)(?= from )', definition, re.IGNORECASE)[0].split(', ')
            for cs in column_sql:
                # Capture final column name
                column = re.findall(r'(?:(?<=^)|(?<= )|(?<=\.))\w+(?=$)', cs, re.IGNORECASE)[0]

                # Capture source CTE of column
                source = re.findall(r'\w+?(?=\.)', cs, re.IGNORECASE)
                try:
                    source = source[0]
                except IndexError:
                    source = 'UNKNOWN'

                # Capture original column name from source CTE
                # v1: No CASE statements
                source_column = re.findall(
                    r'(?:(?<=^)|(?<=\.)|(?<=\()|(?<=, ))(?<! when )(?<! then )(\'?\w+\'?[^0()])(?:(?=\))|(?=$)|(?= as )|(?=, ))(?! end)',
                    cs, re.IGNORECASE
                )
                try:
                    source_column = source_column[0]
                except IndexError:
                    # v2: With CASE statements
                    source_column = re.findall(
                        r'(?:(?<= when )|(?<= then )|(?<= else ))(\'?\w+\'?[^0][^()])(?:(?= when )|(?= else )|(?= end ))',
                        cs, re.IGNORECASE
                    )
                    try:
                        source_column = source_column[0]
                    except IndexError:
                        source_column = 'UNKNOWN'

                columns_list.append({
                    'model': model,
                    'cte': cte,
                    'column': column,
                    'source': source,
                    'source_column': source_column,
                    'column_sql': cs
                })

        cte_columns = pd.DataFrame(columns_list)
        cte_deps = self.get_cte_dependencies(model=model)
        cte_deps_2 = sqldf(
            """
            SELECT cte_deps.*
            FROM cte_deps
            JOIN (
                SELECT cte, COUNT(distinct dependency) as deps
                FROM cte_deps
                GROUP BY cte
                HAVING COUNT(distinct dependency) = 1
            ) single_dep
                ON cte_deps.cte = single_dep.cte
            """,
            locals()
        )
        cte_columns_2 = sqldf(
            """
            SELECT 
                cte_columns.model,
                cte_columns.cte,
                cte_columns.column,
                COALESCE(cte_deps_2.dependency, cte_columns.source) as source,
                cte_columns.source_column,
                cte_columns.column_sql
            FROM cte_columns
            LEFT JOIN cte_deps_2
                ON cte_columns.cte = cte_deps_2.cte
            """,
            locals()
        )

        # Map remaining UNKNOWN records using columns existing in CTE's dependencies
        unknown_source = sqldf(
            """
            SELECT *
            FROM cte_columns_2
            WHERE source = 'UNKNOWN' 
            """,
            locals()
        )

        if not unknown_source.empty:
            unknown_cte_list = []
            for index, row in unknown_source.iterrows():
                unknown_cte_list.append(row['cte'])

            for cte in unknown_cte_list:
                unknown_cte_deps = sqldf(
                    f"""
                    SELECT *
                    FROM cte_deps
                    WHERE cte = '{cte}'
                    """,
                    locals()
                )
                unknown_cte_deps_list = []
                for index, row in unknown_cte_deps.iterrows():
                    unknown_cte_deps_list.append(row['dependency'])
                    unknown_cte_deps_string = ', '.join("'" + item + "'" for item in unknown_cte_deps_list)
                    unknown_cte_deps_columns = sqldf(
                        f"""
                        SELECT DISTINCT cte, column
                        FROM cte_columns_2
                        WHERE cte IN ({unknown_cte_deps_string})
                        """,
                        locals()
                    )

            cte_columns_final = sqldf(
                """
                    SELECT
                        cte_columns_2.model,
                        cte_columns_2.cte,
                        cte_columns_2.column,
                        COALESCE(unknown_cte_deps_columns.cte, cte_columns_2.source) as source,
                        cte_columns_2.source_column,
                        cte_columns_2.column_sql
                    FROM cte_columns_2
                    LEFT JOIN unknown_cte_deps_columns
                        ON cte_columns_2.source_column = unknown_cte_deps_columns.column
                        AND cte_columns_2.source = 'UNKNOWN'
                """,
                locals()
            )
        else:
            cte_columns_final = cte_columns_2

        return cte_columns_final


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

        cte_columns_info_df = dbt_mapper.get_cte_columns_source_info(model)
        print("\nCTE Columns Source Info:")
        print(cte_columns_info_df.to_string(index=False, justify='right'))

        output_dir = os.path.abspath(os.path.join(os.getcwd(), 'output'))
        os.makedirs(output_dir, exist_ok=True)
        node_columns_df.to_csv(os.path.join(output_dir, 'node_columns.csv'), index=False)
        model_dependencies_df.to_csv(os.path.join(output_dir, 'model_dependencies.csv'), index=False)
        cte_dependencies_df.to_csv(os.path.join(output_dir, 'cte_dependencies.csv'), index=False)
        cte_columns_info_df.to_csv(os.path.join(output_dir, 'cte_columns_source_info.csv'), index=False)

        # 1.) Get column names of selected model
        # 2.) Get all dependencies of selected model until the ultimate source(s) reached
        # 3.) Get column names of each dependency


    else:
        print("Please specify the model name using the -s or --select option.")


if __name__ == "__main__":
    main()
