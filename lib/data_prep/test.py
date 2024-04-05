from tqdm import tqdm
from export_utils import refresh_core_tables
from enrichment_utils import get_all_films, update_all_letterboxd_info, ingest_film, ingest_new_people
from sqlite_utils import get_from_table, get_film_ids_from_select_statement, select_statement_to_df
from tmdb_utils import update_tmdb_stats, update_tmbd_metadata, update_person_metadata
from error_utils import correct_tmdb_metadata_errors, correct_all_errors, correct_letterboxd_stats_errors
from update_utils import update_oldest_records, update_streaming_records, update_tmdb_metadata_records, update_recent_films, update_upcoming_films, update_most_popular_records, update_letterboxd_stats, update_letterboxd_top_250
from algo_utils import run_algo
from selenium_utils import download_letterboxd_zip
from justwatch_utils import update_streaming_info
from letterboxd_utils import get_letterboxd_top_250
import sys

import warnings
warnings.filterwarnings("ignore")

select_statement = ("""

WITH DAYS_SINCE_LAST_STREAMING_UPDATE AS (

SELECT FILM_ID, ROUND(AVG(DAYS_SINCE_LAST_UPDATE), 0) AS DAYS_SINCE_LAST_UPDATE 
FROM ( SELECT FILM_ID, ROUND(COALESCE(julianday('now') - julianday(CREATED_AT), 99), 0) AS DAYS_SINCE_LAST_UPDATE FROM FILM_STREAMING_SERVICES )
GROUP BY FILM_ID

)

SELECT a.FILM_ID
FROM FILM_ALGO_SCORE a
LEFT JOIN DAYS_SINCE_LAST_STREAMING_UPDATE b
ON a.FILM_ID = b.FILM_ID
ORDER BY COALESCE(a.ALGO_SCORE, 0.01) * COALESCE(b.DAYS_SINCE_LAST_UPDATE, 365) DESC

""")

# people_to_ingest = list(select_statement_to_df(select_statement)['PERSON_ID'].values)
# films_to_ingest = get_film_ids_from_select_statement(select_statement)
# # films_to_ingest = get_film_ids_from_select_statement("SELECT * FROM FILM_RELEASE_INFO WHERE FILM_STATUS = 'Ended' OR FILM_STATUS = 'Returning Series'")
# # films_to_ingest = get_all_films()
# # films_to_ingest = ['f_0hK9G'] # OVERRIDE TO DEBUG SPECIFIC FILMS
# ingestion_limit = len(films_to_ingest)
# # ingestion_limit = 10
# print('In total, there are {} films left to update letterboxd info for:'.format(len(films_to_ingest)))
# for film_id in tqdm(films_to_ingest[:ingestion_limit]):
# 	print('\n', film_id)
# 	try:
# 		ingest_film(film_id)
# 	    # update_tmbd_metadata(film_id)
# 		# update_all_letterboxd_info(film_id)
# 	except Exception as e:
# 	    print('UPDATE FAILED - {}'.format(e))

# update_oldest_records(film_limit=10, dryrun=False)
# refresh_core_tables()

# ingest_film('f_01Ogu', verbose=True)

# update_streaming_records(get_film_ids_from_select_statement(select_statement), film_limit=20)

# update_oldest_tmdb_metadata_records(['f_0rePK'])

# correct_all_errors(film_ids=None, refresh=False, dryrun=False, film_limit=999)
# correct_all_errors(film_ids=['f_0fBkw'])

# ingest_new_people(people_to_ingest, 10000)

# update_letterboxd_top_250()#model_type='decision_tree')
# update_recent_films(film_limit=500)
# update_upcoming_films(film_limit=500)

# update_most_popular_records(film_limit=1000)

# update_tmdb_metadata_records(['f_0idpe'], verbose=True)

# download_letterboxd_zip(hide_actions=False)

# update_streaming_info('f_0i7Q4', verbose=True)

# update_tmbd_metadata('f_0mUqi', verbose=True)

# update_letterboxd_stats('f_012Ci', verbose=True)
# correct_all_errors()

# correct_tmdb_metadata_errors()
# update_tmbd_metadata('f_0mkbG', verbose=True)

# refresh_core_tables()

# update_person_metadata(1, verbose=True)
# update_person_metadata(2, verbose=True)

# get_letterboxd_top_250()

download_letterboxd_zip(hide_actions=False)