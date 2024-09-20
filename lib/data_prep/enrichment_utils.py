from random import sample
from tqdm import tqdm
from export_utils import exportfile_to_df, convert_uri_to_id
from sqlite_utils import table_to_df, update_ingestion_table, get_person_ids_from_select_statement
from letterboxd_utils import update_all_letterboxd_info, download_poster
from tmdb_utils import update_tmbd_metadata, get_person_metadata
from justwatch_utils import update_streaming_info
from dotenv import load_dotenv

load_dotenv(override=True)

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

def ingest_film(film_id, log_reason='INGESTION', verbose=False):
    try:
        update_all_letterboxd_info(film_id, log_reason=log_reason, verbose=verbose)
    except Exception as e:
        print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))
    try:
        download_poster(film_id)
    except Exception as e:
        print('DOwnload of poster for {} failed ({})'.format(film_id, e))
    try:
        update_tmbd_metadata(film_id, log_reason=log_reason, verbose=verbose)
    except Exception as e:
        print('Update of film metadata info for {} failed ({})'.format(film_id, e))
    try:
        update_streaming_info(film_id, log_reason=log_reason, verbose=verbose)
    except Exception as e:
        print('Update of streaming info for {} failed ({})'.format(film_id, e))
    update_ingestion_table(film_id)

def ingest_films(films_to_ingest, log_reason='INGESTION'):
    for film_id in tqdm(films_to_ingest):
        ingest_film(film_id, log_reason=log_reason)

def ingest_new_films(film_limit=100):
    load_dotenv(override=True)
    total_films_to_ingest = get_new_films()
    films_to_ingest = total_films_to_ingest[:film_limit]
    print('In total, there are {} new films to ingest - ingesting {}'.format(len(total_films_to_ingest), len(films_to_ingest)))
    ingest_films(films_to_ingest)

def get_new_people():
    new_person_ids = get_person_ids_from_select_statement(ranked_person_id_query)
    return new_person_ids

def ingest_person(person_id):
    try:
        get_person_metadata(person_id)
    except Exception as e:
        print('Update of Person metadata for {} failed ({})'.format(person_id, e))

def ingest_people(people_to_ingest):
    for person_id in tqdm(people_to_ingest):
        ingest_person(person_id)

def ingest_new_people(person_ids=None, people_limit=500):
    load_dotenv(override=True)
    if person_ids:
        total_people_to_ingest = person_ids
        people_to_ingest = total_people_to_ingest[:people_limit]
    else:
        total_people_to_ingest = get_new_people()
        people_to_ingest = total_people_to_ingest[:people_limit]
    print('In total, there are {} new people to ingest - ingesting {}'.format(len(total_people_to_ingest), len(people_to_ingest)))
    ingest_people(people_to_ingest)

ranked_person_id_query = """

WITH FILM_PERSON_INFO AS (      
   
	SELECT
   	
     a.FILM_ID
   	,b.FILM_WATCH_COUNT
   	,c.PERSON_ID
   	
   FROM ALL_RELEASED_FILMS a
   LEFT JOIN FILM_LETTERBOXD_STATS b
   ON a.FILM_ID = b.FILM_ID
   LEFT JOIN FILM_CAST c
   ON a.FILM_ID = c.FILM_ID
   
   UNION ALL 
   
   SELECT
   	
     a.FILM_ID
   	,b.FILM_WATCH_COUNT
   	,c.PERSON_ID
   	
   FROM ALL_RELEASED_FILMS a
   LEFT JOIN FILM_LETTERBOXD_STATS b
   ON a.FILM_ID = b.FILM_ID
   LEFT JOIN FILM_CREW c
   ON a.FILM_ID = c.FILM_ID
   WHERE c.JOB = 'Director'
   
   )
   
   SELECT 
   
   	 a.PERSON_ID
   	,b.PERSON_NAME
   	,SUM(a.FILM_WATCH_COUNT) AS TOTAL_WATCHES
   	
   	FROM FILM_PERSON_INFO a
   	LEFT JOIN PERSON_INFO b
   	ON a.PERSON_ID = b.PERSON_ID
   	WHERE a.PERSON_ID > 0
    AND b.PERSON_NAME IS NULL
   	GROUP BY a.PERSON_ID, a.FILM_ID
   	ORDER BY TOTAL_WATCHES DESC
   
 """