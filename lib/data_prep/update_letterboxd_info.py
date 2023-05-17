from tqdm import tqdm
from enrichment_utils import get_ext_ids_plus_content_type, get_all_films, get_film_ids_from_select_statement, update_all_letterboxd_info, ingest_film, get_metadata_from_letterboxd, update_letterboxd_stats
from sqlite_utils import get_from_table
from tmdb_utils import get_tmbd_metadata, update_tmdb_stats
from error_utils import correct_tmdb_metadata_errors, correct_all_errors
from update_utils import update_oldest_records

select_statement = ("""

SELECT

	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_RELEASED_FILMS a

LEFT JOIN FILM_KEYWORDS b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.KEYWORD_ID IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")
# films_to_ingest = get_film_ids_from_select_statement(select_statement)
# films_to_ingest = get_film_ids_from_select_statement("SELECT * FROM FILM_RELEASE_INFO WHERE FILM_STATUS = 'Ended' OR FILM_STATUS = 'Returning Series'")
# films_to_ingest = get_all_films()
# films_to_ingest = ['f_0hK9G'] # OVERRIDE TO DEBUG SPECIFIC FILMS
# ingestion_limit = len(films_to_ingest)
# ingestion_limit = 10
# print('In total, there are {} films left to update letterboxd info for:'.format(len(films_to_ingest)))
# for film_id in tqdm(films_to_ingest[:ingestion_limit]):
	# print('\n', film_id)
	# try:
		# ingest_film(film_id)
	    # get_tmbd_metadata(film_id)
		# update_all_letterboxd_info(film_id)
	# except Exception as e:
	    # print('UPDATE FAILED - {}'.format(e))

update_oldest_records(film_limit=10, dryrun=False)

# ingest_film('f_0lrky')