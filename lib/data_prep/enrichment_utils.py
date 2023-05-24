from random import sample
from tqdm import tqdm
from sqlite_utils import table_to_df
from export_utils import exportfile_to_df, convert_uri_to_id
from tmdb_utils import get_tmbd_metadata
# from selenium_utils import return_logged_in_page_source
from justwatch_utils import update_streaming_info
from letterboxd_utils import update_all_letterboxd_info
from update_utils import update_ingestion_table
from dotenv import load_dotenv

load_dotenv()

def get_all_films():
    watched_df = exportfile_to_df('watched.csv')
    watched_film_ids = [convert_uri_to_id(x) for x in watched_df['LETTERBOXD_URI'].values]
    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_film_ids = [convert_uri_to_id(x) for x in watchlist_df['LETTERBOXD_URI'].values]
    all_film_ids = watched_film_ids + watchlist_film_ids
    return all_film_ids

def get_ingested_films(error_type=None, shuffle=True):
    valid_error_types = ['LETTERBOXD_ERROR', 'METADATA_ERROR', 'STREAMING_ERROR', 'TOTAL_INGESTION_ERRORS']
    try:
        ingested_df = table_to_df('INGESTED')
        if error_type:
            if error_type in valid_error_types:
                ingested_df = ingested_df[ingested_df[error_type] > 0]
            else:
                return print('error_type parameter, if passed, must be one of {}'.format(', '.join(valid_error_types)))
        ingested_film_ids = ingested_df['FILM_ID'].values
        if shuffle: ingested_film_ids = sample(list(ingested_film_ids), len(ingested_film_ids))
    except:
        ingested_film_ids = []
    return ingested_film_ids

def get_new_films():
    ingested_film_ids = get_ingested_films()
    all_film_ids = get_all_films()
    new_film_ids = [x for x in all_film_ids if x not in ingested_film_ids]
    new_film_ids = sample(new_film_ids, len(new_film_ids))
    return new_film_ids

def ingest_film(film_id):
    try:
        update_all_letterboxd_info(film_id)
    except Exception as e:
        print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))
    try:
        get_tmbd_metadata(film_id)
    except Exception as e:
        print('Update of film metadata info for {} failed ({})'.format(film_id, e))
    try:
        update_streaming_info(film_id)
    except Exception as e:
        print('Update of streaming info for {} failed ({})'.format(film_id, e))
    update_ingestion_table(film_id)

def ingest_new_films():
    films_to_ingest = get_new_films()
    print('In total, there are {} new films to ingest'.format(len(films_to_ingest)))
    for film_id in tqdm(films_to_ingest):
        ingest_film(film_id)
