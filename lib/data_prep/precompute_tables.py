import yaml
from tqdm import tqdm
import sys
sys.path.insert(0, '../data_prep')
from sqlite_utils import select_statement_to_df, df_to_table

precomputed_queries_file = 'precompute_queries.yaml'

def load_queries():
    with open(precomputed_queries_file, 'r') as file:
        queries = yaml.safe_load(file)
    return queries

queries = load_queries()

def precompute_table(table_name):
    sql = queries[table_name]['sql']
    df = select_statement_to_df(sql)
    table_name = 'precomputed_'+table_name.replace('_query', '')
    df_to_table(df, table_name, replace_append='replace', verbose=True)

def precompute_tables():
    for table_name in tqdm(queries.keys()):
        precompute_table(table_name)
