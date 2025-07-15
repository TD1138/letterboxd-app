from sqlite_utils import table_to_df

from dotenv import load_dotenv

load_dotenv()

def convert_uri_to_id(letterboxd_uri):
    return 'f_'+letterboxd_uri.replace('https://boxd.it/', '').zfill(5)

def get_ingested_films():
    ingested_df = table_to_df('INGESTED')
    ingested_film_list = ingested_df['FILM_ID'].values
    return ingested_film_list

def read_all_films(latest_export_file_loc):
    watched_df = pd.read_csv(os.path.join(latest_export_file_loc, 'watched.csv'))
    watched_films = [convert_uri_to_id(x) for x in watched_df['Letterboxd URI'].values]
    watchlist_df = pd.read_csv(os.path.join(latest_export_file_loc, 'watchlist.csv'))
    watchlist_films = [convert_uri_to_id(x) for x in watchlist_df['Letterboxd URI'].values]
    all_films = watched_films + watchlist_films
    return all_films

def get_new_films(latest_export_file_loc):
    ingested_film_list = get_ingested_films()
    all_films_latest_export = read_all_films(latest_export_file_loc)
    new_films = [x for x in all_films_latest_export if x not in ingest_film_list]
    return new_films

