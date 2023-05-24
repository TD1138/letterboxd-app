from datetime import datetime
from tqdm import tqdm
from sqlite_utils import replace_record, get_film_ids_from_select_statement
from enrichment_utils import update_streaming_info
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import update_letterboxd_stats
import dotenv
dotenv.load_dotenv()

def update_oldest_letterboxd_stats_records(film_ids=None, film_limit=100, dryrun=False):
    if film_ids:
        letterboxd_stats_to_update = film_ids
    else:
        letterboxd_stats_to_update = get_film_ids_from_select_statement(letterboxd_stats_select_statement)[:film_limit]
    print('In total, we are going to update the oldest {} records for letterboxd stats'.format(len(letterboxd_stats_to_update)))
    if dryrun:
        print(letterboxd_stats_to_update[:10])
        return
    for film_id in tqdm(letterboxd_stats_to_update):
        try:
            update_letterboxd_stats(film_id)
        except Exception as e:
            print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))

def update_oldest_tmdb_metadata_records(film_ids=None, film_limit=100, dryrun=False):
    if film_ids:
        tmdb_metadata_to_update = film_ids
    else:
        tmdb_metadata_to_update = get_film_ids_from_select_statement(tmdb_metadata_select_statement)[:film_limit]
    print('In total, we are going to update the oldest {} records for tmdb metadata'.format(len(tmdb_metadata_to_update)))
    if dryrun:
        print(tmdb_metadata_to_update[:10])
        return
    for film_id in tqdm(tmdb_metadata_to_update):
        try:
            update_tmbd_metadata(film_id)
        except Exception as e:
            print('Update of TMDB Metadata for {} failed ({})'.format(film_id, e))
        
def update_oldest_streaming_records(film_ids=None, film_limit=100, dryrun=False):
    if film_ids:
        streaming_to_update = film_ids
    else:
        streaming_to_update = get_film_ids_from_select_statement(streaming_select_statement)[:film_limit]
    print('In total, we are going to update the oldest {} records for streaming'.format(len(streaming_to_update)))
    if dryrun:
        print(streaming_to_update[:10])
        return
    for film_id in tqdm(streaming_to_update):
        try:
            update_streaming_info(film_id)
        except Exception as e:
            print('Update of streaming info for {} failed ({})'.format(film_id, e))   

def update_oldest_records(film_ids=None, film_limit=100, dryrun=False):
    update_oldest_letterboxd_stats_records(film_ids=film_ids, film_limit=film_limit, dryrun=dryrun)
    update_oldest_tmdb_metadata_records(film_ids=film_ids, film_limit=film_limit, dryrun=dryrun)
    update_oldest_streaming_records(film_ids=film_ids, film_limit=film_limit*10, dryrun=dryrun)

letterboxd_stats_select_statement = ("""

SELECT
	 FILM_ID
	,COALESCE(julianday('now') - julianday(CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE
	
FROM FILM_LETTERBOXD_STATS

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")

tmdb_metadata_select_statement = ("""

SELECT
	 FILM_ID
	,COALESCE(julianday('now') - julianday(CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE
	
FROM FILM_TMDB_STATS

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")

streaming_select_statement = ("""

SELECT
	 FILM_ID
	,COALESCE(julianday('now') - julianday(CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE
	
FROM (SELECT FILM_ID, MAX(CREATED_AT) AS CREATED_AT FROM FILM_STREAMING_SERVICES GROUP BY FILM_ID)

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")