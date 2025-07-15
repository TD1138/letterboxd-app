import os
import json
import pandas as pd
import sqlite3 as sql
from datetime import datetime
import dotenv
dotenv.load_dotenv(override=True)

def set_working_db(db_name):
    dotenv.load_dotenv(override=True)
    local_db_path = os.getenv('PROJECT_PATH') + '/db/' + db_name
    dotenv.set_key(dotenv.find_dotenv(), 'WORKING_DB', local_db_path)

def db_info(db_path=None):
    '''
    prints out all of the columns of every table in db
    '''
    dotenv.load_dotenv(override=True)
    if not db_path: db_path=os.getenv('WORKING_DB')
    if db_exists(db_path):
        db_conn = sql.connect(db_path)
        tables = db_conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        for table_name in tables:
            table_name = table_name[0] # tables is a list of single item tuples
            table = pd.read_sql_query("SELECT * from {} LIMIT 0".format(table_name), db_conn)
            print(table_name)
            for col in table.columns:
                print('\t' + col)
        db_conn.close()
    else:
        print('This database has no tables yet!')

def get_list_of_tables():
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    tables = db_conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    db_conn.close()
    tables = [table[0] for table in tables]
    return tables

def print_table_head(table_name, head=10):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    print(pd.read_sql_query("SELECT * from {} LIMIT {}".format(table_name, head), db_conn))
    db_conn.close()

def db_table_samples(head=10):
    for t in get_list_of_tables():
        print(t, ':')
        print_table_head(t, head=head)

def db_exists(db_path):
    try:
        db_conn = sql.connect(db_path)
        tables = db_conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        table_count = len(tables)
        db_conn.close()
        if table_count == 0:
            return False
        else:
            return True
    except:
        return False

def df_to_table(df, table_name, replace_append='replace', verbose=False):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    try:
        df.columns = [col.upper() for col in df.columns]
        df.to_sql(table_name, db_conn, if_exists=replace_append, index=False)
        db_conn.close()
        if verbose: print('dataframe succesfully added to the database {} with the table name {}'.format(os.getenv('WORKING_DB'), table_name))
    except:
        db_conn.close()
        print('Error - dataframe couldn\'t be added to the database table {}'.format(table_name))

def drop_table(table_name, verbose=False):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    try:
        drop_statement = 'DROP TABLE {}'.format(table_name)
        db_conn.cursor().execute(drop_statement)
        db_conn.close()
        if verbose: print('Table {} dropped successfully'.format(table_name))
    except:
        db_conn.close()
        print('Error in dropping the table')

def table_to_df(table_name):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    select_statement = 'SELECT * FROM {}'.format(table_name)
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    return df

def db_basic_setup(db_name, overwrite=False, verbose=False):
    if db_name[-3:] != '.db':
        print('db name must be a valid name ending in \'.db\'')
        return
    dotenv.load_dotenv(override=True)
    db_path = os.path.join(os.getenv('PROJECT_PATH'), 'db', db_name)
    if db_exists(db_path) and overwrite:
        os.remove(db_path)
    with open('db_schema.json', 'r') as schema:
        db_schema = json.load(schema)
    if not db_exists(db_path):
        db_conn = sql.connect(db_path)
        for k in db_schema:
            column_details = db_schema[k]
            statement_list = ['CREATE TABLE IF NOT EXISTS {} ('.format(k)]
            for col in column_details:
                statement_list.append(' '.join(col)+',')
            statement_list.append(');')
            create_statement = ' '.join(statement_list).replace('( ', '(').replace(', )', ')')
            db_conn.cursor().execute(create_statement)
        db_conn.close()
        if verbose: db_info()
    else:
        print('database already exists - set overwrite to true if you wish to delete and rebuild')
        return

def get_from_table(table_name, film_id, item=None):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    select_statement = 'SELECT * FROM {} WHERE FILM_ID = "{}"'.format(table_name, film_id)
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    try:
        output = df.to_dict(orient='records')
        if len(df) == 0:
            output = {}    
        elif len(df) == 1:
            output = output[0]
    except:
        output = {}
    if item:
        output = output.get(item, '')
    return output

def insert_record_into_table(record, table_name, logging=True, log_reason='UPDATE', append=False):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    table_columns = pd.read_sql_query("SELECT * from {} LIMIT 0".format(table_name), db_conn).columns
    record_columns = record.keys()
    missing_from_record = [x for x in table_columns if x not in record_columns]
    if len(missing_from_record) > 0:
        print('There are fields missing from the record so the table cannot be updated ({})'.format(missing_from_record))
        return
    if append:
        append_str = 'INSERT'
    else:
        append_str = 'REPLACE'
    insert_statement = '{} INTO {} ({}) VALUES({})'.format(append_str, table_name, ','.join(table_columns), ','.join(['?']*len(table_columns)))
    insert_tuple =  tuple([record.get(x) for x in table_columns])
    db_conn.cursor().execute(insert_statement, insert_tuple)
    db_conn.commit()
    db_conn.close()
    if logging:
        log_update(record, table_name, log_reason=log_reason)

def delete_records(table_name, film_id, primary_key='FILM_ID'):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    c = db_conn.cursor()
    try:
        delete_statement = 'DELETE FROM {} WHERE {} = "{}"'.format(table_name, primary_key, film_id)
        c.execute(delete_statement)
        db_conn.commit()
    except sql.Error as error:
        print("Error executing update statement:", error)
    db_conn.close()

def update_record(table_name, column_name, column_value, film_id, primary_key='FILM_ID', logging=True, log_reason='UPDATE'):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    c = db_conn.cursor()
    try:
        update_time = datetime.now()
        c.execute('UPDATE {} SET {} = ? WHERE {} = ?'.format(table_name, column_name, primary_key), (column_value, film_id))
        c.execute('UPDATE {} SET {} = ? WHERE {} = ?'.format(table_name, 'CREATED_AT', primary_key), (update_time, film_id))
        db_conn.commit()
        if logging:
            record = {
                'FILM_ID': film_id,
                column_name: column_value,
                'CREATED_AT': update_time
                }
            log_update(record, table_name, log_reason=log_reason)
        db_conn.close()
    except sql.Error as error:
        print("Error executing update statement:", error)
        db_conn.close()

def replace_record(table_name, record, film_id, primary_key='FILM_ID', log_reason='UPDATE'):
    delete_records(table_name, film_id, primary_key)
    insert_record_into_table(record, table_name, log_reason=log_reason)

def select_statement_to_df(select_statement):
    dotenv.load_dotenv(override=True)
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    return df

def get_film_ids_from_select_statement(select_statement):
    sql_df = select_statement_to_df(select_statement)
    try:
        sql_film_ids = list(sql_df['FILM_ID'].values)
        return sql_film_ids
    except:
        print('select statement must output "FILM_ID" column')

def get_person_ids_from_select_statement(select_statement):
    sql_df = select_statement_to_df(select_statement)
    try:
        sql_person_ids = list(sql_df['PERSON_ID'].values)
        return sql_person_ids
    except:
        print('select statement must output "PERSON_ID" column')

def update_ingestion_table(film_id):
    ingestion_record = {
        'FILM_ID': film_id,
        'CREATED_AT': datetime.now()
    }
    replace_record('INGESTED', ingestion_record, film_id)

def log_update(record, table_name, log_reason='UPDATE'):
    for k, v in record.items():
        if k not in ['FILM_ID', 'CREATED_AT']:
            log_record = {
                'FILM_ID': record['FILM_ID'],
                'TABLE_NAME': table_name,
                'COLUMN_NAME': k,
                'COLUMN_VALUE': v,
                'CREATED_AT': record['CREATED_AT'],
                'LOG_REASON': log_reason
                }
            insert_record_into_table(log_record, 'LOGGING', logging=False, append=True)