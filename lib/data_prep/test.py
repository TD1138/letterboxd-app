from tqdm import tqdm
from export_utils import refresh_core_tables
from enrichment_utils import get_all_films, update_all_letterboxd_info, ingest_film, ingest_new_people
from sqlite_utils import get_from_table, get_film_ids_from_select_statement, select_statement_to_df
from tmdb_utils import get_tmbd_metadata, update_tmdb_stats
from error_utils import correct_tmdb_metadata_errors, correct_all_errors
from update_utils import update_oldest_records, update_oldest_streaming_records, update_oldest_tmdb_metadata_records
from algo_utils import run_algo
import sys

import warnings
warnings.filterwarnings("ignore")

select_statement = ("""

SELECT
     PERSON_ID
    ,SUM(FILM_WATCH_COUNT) AS TOTAL_FILM_WATCH_COUNT
FROM (
    SELECT a.PERSON_ID, b.FILM_WATCH_COUNT
    FROM FILM_CREW a
    LEFT JOIN FILM_LETTERBOXD_STATS b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN PERSON_INFO c
    ON a.PERSON_ID = c.PERSON_ID
    WHERE c.PERSON_NAME IS NULL
    UNION ALL
    SELECT a.PERSON_ID, b.FILM_WATCH_COUNT
    FROM FILM_CAST a
    LEFT JOIN FILM_LETTERBOXD_STATS b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN PERSON_INFO c
    ON a.PERSON_ID = c.PERSON_ID
    WHERE c.PERSON_NAME IS NULL
    )
GROUP BY PERSON_ID
ORDER BY TOTAL_FILM_WATCH_COUNT DESC

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
# 	    # get_tmbd_metadata(film_id)
# 		# update_all_letterboxd_info(film_id)
# 	except Exception as e:
# 	    print('UPDATE FAILED - {}'.format(e))

# update_oldest_records(film_limit=10, dryrun=False)
# refresh_core_tables()

ingest_film('f_0pAgy', verbose=True)
# update_oldest_streaming_records(films_to_ingest, 5000)
# update_oldest_tmdb_metadata_records(['f_0rePK'])

# correct_all_errors(film_ids=None, refresh=False, dryrun=False, film_limit=999)
# correct_all_errors(film_ids=['f_0fBkw'])

# ingest_new_people(people_to_ingest, 10000)

# run_algo(model_type='linear_regression')