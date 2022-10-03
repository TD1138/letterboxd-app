import sqlite3 as sql
from dotenv import load_dotenv

load_dotenv()

db_name = 'lb-film.db'
film_db_path = os.path.join(data_loc, db_name)

def db_info(db_path):
    '''
    prints out all of the columns of every table in db
    c : cursor object
    conn : database connection object
    '''
    db_conn = sql.connect(db_path)
    connection_cursor = db_conn.cursor()
    tables = connection_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    if len(tables) == 0:
        print('This database has no tables yet!')
    else:
        for table_name in tables:
            table_name = table_name[0] # tables is a list of single item tuples
            table = pd.read_sql_query("SELECT * from {} LIMIT 0".format(table_name), db_conn)
            print(table_name)
            for col in table.columns:
                print('\t' + col)
    db_conn.close()

def df_to_table(df, db_path, table_name, replace_append='replace'):
    db_conn = sql.connect(db_path)
    try:
        df.columns = [col.upper() for col in df.columns]
        df.to_sql(table_name, db_conn, if_exists=replace_append, index=False)
        db_conn.close()
        print('dataframe succesfully added to the database {} with the table name {}'.format(db_path, table_name))
    except:
        db_conn.close()
        print('Error - dataframe couldn\'t be added to the database')

def drop_table(db_path, table_name):
    db_conn = sql.connect(db_path)
    try:
        drop_statement = 'DROP TABLE {}'.format(table_name)
        db_conn.cursor().execute(drop_statement)
        db_conn.close()
        return print('Table {} dropped successfully'.format(table_name))
    except:
        db_conn.close()
        return print('Error in dropping the table')

def table_to_df(db_path, table_name):
    db_conn = sql.connect(db_path)
    select_statement = 'SELECT * FROM {}'.format(table_name)
    df = pd.read_sql(select_statement, db_conn)
    db_conn.close()
    return df