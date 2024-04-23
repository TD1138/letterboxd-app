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

def precompute_tables():
    queries = load_queries()
    for q in tqdm(queries):
        sql = queries[q]['sql']
        df = select_statement_to_df(sql)
        table_name = 'precomputed_'+q.replace('_query', '')
        df_to_table(df, table_name, replace_append='replace', verbose=True)