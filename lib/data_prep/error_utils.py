from tqdm import tqdm
from sqlite_utils import get_film_ids_from_select_statement
from enrichment_utils import ingest_film
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import get_ext_ids_plus_content_type
import dotenv
dotenv.load_dotenv()

def correct_ext_ids_plus_content_type_errors(film_ids=None, refresh=False, dryrun=False):
    if film_ids:
        films_to_correct = film_ids
    else:
        imdb_films_to_correct = get_film_ids_from_select_statement(imdb_id_select_statement)
        tmdb_films_to_correct = get_film_ids_from_select_statement(tmdb_id_select_statement)
        content_type_films_to_correct = get_film_ids_from_select_statement(content_type_select_statement)
        films_to_correct = list(set(          \
            imdb_films_to_correct             \
            + tmdb_films_to_correct           \
            + content_type_films_to_correct   \
            ))
    total_films = len(films_to_correct)
    if dryrun:
        print(films_to_correct[:10])
        return
    elif total_films == 0:
        return
    print('There are {} films to correct external ids & content type for:'.format(total_films))
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id)
            else:
                get_ext_ids_plus_content_type(film_id)
        except:
            errors += 1
    successful_films = total_films - errors
    print('Corrected tmdb metadata for {} films ({:.2%})'.format(successful_films, successful_films/total_films))

def correct_tmdb_metadata_errors(film_ids=None, refresh=False, dryrun=False):
    if film_ids:
        films_to_correct = film_ids
    else:
        financials_films_to_correct = get_film_ids_from_select_statement(financials_select_statement)
        tmdb_stats_films_to_correct = get_film_ids_from_select_statement(tmdb_stats_select_statement)
        release_info_films_to_correct = get_film_ids_from_select_statement(release_info_select_statement)
        keyword_films_to_correct = get_film_ids_from_select_statement(keyword_select_statement)    
        cast_films_to_correct = get_film_ids_from_select_statement(cast_select_statement)
        films_to_correct = list(set(          \
            financials_films_to_correct       \
            + tmdb_stats_films_to_correct     \
            + release_info_films_to_correct   \
            + keyword_films_to_correct        \
            + cast_films_to_correct           \
            ))
    total_films = len(films_to_correct)
    if dryrun:
        print(films_to_correct[:10])
        return
    elif total_films == 0:
        return
    print('There are {} films to correct tmdb metadata for:'.format(total_films))
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id)
            else:
                update_tmbd_metadata(film_id)
        except:
            errors += 1
    successful_films = total_films - errors
    print('Corrected tmdb metadata for {} films ({:.2%})'.format(successful_films, successful_films/total_films))

def correct_all_errors(film_ids=None, refresh=False, dryrun=False):
    correct_ext_ids_plus_content_type_errors(film_ids, refresh=refresh, dryrun=dryrun)
    correct_tmdb_metadata_errors(film_ids, refresh=refresh, dryrun=dryrun)

imdb_id_select_statement = ("""

SELECT

	  a.FILM_ID
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN IMDB_ID b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.IMDB_ID IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")

tmdb_id_select_statement = ("""

SELECT

	  a.FILM_ID
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN TMDB_ID b
ON a.FILM_ID = b.FILM_ID

WHERE b.TMDB_ID IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND b.VALID = 1

""")

content_type_select_statement = ("""

SELECT

	  a.FILM_ID
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN CONTENT_TYPE b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.CONTENT_TYPE IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")

financials_select_statement = ( """

SELECT

	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_RELEASED_FILMS a

LEFT JOIN FILM_FINANCIALS b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.FILM_BUDGET IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")

tmdb_stats_select_statement = ( """

SELECT

	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_RELEASED_FILMS a

LEFT JOIN FILM_TMDB_STATS b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.FILM_POPULARITY IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")

release_info_select_statement = ("""

SELECT

	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN FILM_RELEASE_INFO b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.FILM_RELEASE_DATE IS NULL
AND DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

""")

keyword_select_statement = ("""

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

cast_select_statement = ("""

SELECT FILM_ID, MAX(CAST_ORDER) AS CAST_SIZE FROM (

SELECT

	 a.FILM_ID
	,b.CAST_ORDER
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_RELEASED_FILMS a

LEFT JOIN FILM_CAST b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE DAYS_SINCE_LAST_UPDATE > 7
AND c.VALID = 1

) GROUP BY FILM_ID
HAVING CAST_SIZE = 0

""")