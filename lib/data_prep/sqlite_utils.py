import os
import json
import pandas as pd
import sqlite3 as sql
import time
import dotenv
dotenv.load_dotenv()

def set_working_db(db_name):
    local_db_path = os.getenv('PROJECT_PATH') + '/db/' + db_name
    dotenv.set_key(dotenv.find_dotenv(), 'WORKING_DB', local_db_path)

def db_info(db_path=None):
    '''
    prints out all of the columns of every table in db
    '''
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

def db_exists(db_path):
    db_conn = sql.connect(db_path)
    tables = db_conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    table_count = len(tables)
    db_conn.close()
    if table_count == 0:
        return False
    else:
        return True

def df_to_table(df, table_name, replace_append='replace', verbose=False):
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    try:
        df.columns = [col.upper() for col in df.columns]
        df.to_sql(table_name, db_conn, if_exists=replace_append, index=False)
        db_conn.close()
        if verbose: print('dataframe succesfully added to the database {} with the table name {}'.format(os.getenv('WORKING_DB'), table_name))
    except:
        db_conn.close()
        print('Error - dataframe couldn\'t be added to the database')

def drop_table(table_name, verbose=False):
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
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    select_statement = 'SELECT * FROM {}'.format(table_name)
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    return df

def db_basic_setup(db_name, overwrite=False, verbose=False):
    if db_name[-3:] != '.db':
        print('db name must be a valid name ending in \'.db\'')
        return
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
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    select_statement = 'SELECT * FROM {} WHERE FILM_ID = "{}"'.format(table_name, film_id)
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    output = df.to_dict(orient='records')[0]
    if item:
        output = output.get(item, '')
    return output

def insert_record_into_table(record, table_name):
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    table_columns = pd.read_sql_query("SELECT * from {} LIMIT 0".format(table_name), db_conn).columns
    record_columns = record.keys()
    missing_from_record = [x for x in table_columns if x not in record_columns]
    if len(missing_from_record) > 0:
        print('There are fields missing from the record so the table cannot be updated ({})'.format(missing_from_record))
        return
    insert_statement = 'INSERT INTO {}({}) VALUES({})'.format(table_name, ','.join(table_columns), ','.join(['?']*len(table_columns)))
    insert_tuple =  tuple([record.get(x) for x in table_columns])
    db_conn.cursor().execute(insert_statement, insert_tuple)
    db_conn.commit()
    db_conn.close()

def update_record(table_name, column_name, column_value, film_id):
    db_conn = sql.connect(os.getenv('WORKING_DB'))
    update_statement = 'UPDATE {} SET {} = ? WHERE FILM_ID = ?;'.format(table_name, column_name)
    update_tuple = (column_value, film_id)
    db_conn.cursor().execute(update_statement, update_tuple)
    db_conn.commit()
    db_conn.close()