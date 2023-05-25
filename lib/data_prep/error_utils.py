from tqdm import tqdm
from sqlite_utils import get_film_ids_from_select_statement
from enrichment_utils import ingest_film
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import get_ext_ids_plus_content_type, get_metadata_from_letterboxd
from dotenv import load_dotenv

load_dotenv()

def correct_letterboxd_metadata_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        films_to_correct = film_ids
    else:
        title_films_to_correct = get_film_ids_from_select_statement(title_select_statement)
        year_films_to_correct = get_film_ids_from_select_statement(year_select_statement)
        genre_films_to_correct = get_film_ids_from_select_statement(genre_select_statement)
        total_films_to_correct = list(set(          \
            title_films_to_correct             \
            + year_films_to_correct           \
            + genre_films_to_correct   \
            ))
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} new films to correct letterboxd metadata for - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id)
            else:
                get_metadata_from_letterboxd(film_id)
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected letterboxd_metadata for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_ext_ids_plus_content_type_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        films_to_correct = film_ids
    else:
        imdb_films_to_correct = get_film_ids_from_select_statement(imdb_id_select_statement)
        tmdb_films_to_correct = get_film_ids_from_select_statement(tmdb_id_select_statement)
        content_type_films_to_correct = get_film_ids_from_select_statement(content_type_select_statement)
        total_films_to_correct = list(set(          \
            imdb_films_to_correct             \
            + tmdb_films_to_correct           \
            + content_type_films_to_correct   \
            ))
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} new films to correct external ids & content type for - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id)
            else:
                get_ext_ids_plus_content_type(film_id)
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected external ids & content type for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_tmdb_metadata_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        films_to_correct = film_ids
    else:
        financials_films_to_correct = get_film_ids_from_select_statement(financials_select_statement)
        tmdb_stats_films_to_correct = get_film_ids_from_select_statement(tmdb_stats_select_statement)
        release_info_films_to_correct = get_film_ids_from_select_statement(release_info_select_statement)
        keyword_films_to_correct = get_film_ids_from_select_statement(keyword_select_statement)    
        cast_films_to_correct = get_film_ids_from_select_statement(cast_select_statement)
        crew_films_to_correct = get_film_ids_from_select_statement(crew_select_statement)
        total_films_to_correct = list(set(          \
            financials_films_to_correct       \
            + tmdb_stats_films_to_correct     \
            + release_info_films_to_correct   \
            + keyword_films_to_correct        \
            + cast_films_to_correct           \
            + crew_films_to_correct           \
            ))
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} new films to correct tmdb metadata for - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id)
            else:
                update_tmbd_metadata(film_id)
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected tmdb metadata for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_all_errors(film_ids=None, refresh=False, dryrun=False, film_limit=100):
    correct_letterboxd_metadata_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_ext_ids_plus_content_type_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_tmdb_metadata_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit*10)

title_select_statement = ("""

SELECT
	  a.FILM_ID
	 ,COALESCE(julianday('now') - julianday(c.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN FILM_TITLE b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN FILM_URL_TITLE c
ON a.FILM_ID = c.FILM_ID

WHERE b.FILM_TITLE IS NULL
OR b.FILM_TITLE = ""
OR c.FILM_URL_TITLE IS NULL
OR c.FILM_URL_TITLE = ""
AND DAYS_SINCE_LAST_UPDATE > 3

""")

year_select_statement = """

SELECT
	  a.FILM_ID
	 ,b.FILM_YEAR
	 ,b.FILM_DECADE
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN FILM_YEAR b
ON a.FILM_ID = b.FILM_ID

WHERE b.FILM_ID IS NULL
OR b.FILM_YEAR = ""
OR b.FILM_YEAR IS NULL
AND DAYS_SINCE_LAST_UPDATE > 3

"""

genre_select_statement = """

SELECT
	  a.FILM_ID
	 ,b.FILM_GENRE
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN FILM_GENRE b
ON a.FILM_ID = b.FILM_ID

WHERE b.FILM_ID IS NULL
OR b.FILM_GENRE = ""
OR b.FILM_GENRE IS NULL
AND DAYS_SINCE_LAST_UPDATE > 3

"""

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
AND DAYS_SINCE_LAST_UPDATE > 3
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
AND DAYS_SINCE_LAST_UPDATE > 3
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

crew_select_statement = ("""

SELECT

	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_RELEASED_FILMS a

LEFT JOIN FILM_CREW b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

WHERE b.PERSON_ID IS NULL
AND c.VALID = 1
AND DAYS_SINCE_LAST_UPDATE > 7

""")