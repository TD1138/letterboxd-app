from tqdm import tqdm
from export_utils import refresh_core_tables
from enrichment_utils import get_all_films, update_all_letterboxd_info, ingest_film, ingest_new_people
from sqlite_utils import get_from_table, get_film_ids_from_select_statement
from tmdb_utils import get_tmbd_metadata, update_tmdb_stats
from error_utils import correct_tmdb_metadata_errors, correct_all_errors
from update_utils import update_oldest_records, update_oldest_streaming_records
from algo_utils import run_algo
import sys

import warnings
warnings.filterwarnings("ignore")

select_statement = ("""

SELECT *
FROM FILMS_AVAILABLE_TO_STREAM

""")

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

# ingest_film('f_01Wgi')
# update_oldest_streaming_records(films_to_ingest, 5000)

# correct_all_errors(film_ids=None, refresh=False, dryrun=False, film_limit=999)
# correct_all_errors(film_ids=['f_0fBkw'])

# ingest_new_people(10000)

run_algo()