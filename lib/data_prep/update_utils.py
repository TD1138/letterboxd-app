from datetime import datetime
from tqdm import tqdm
from sqlite_utils import replace_record, get_film_ids_from_select_statement
from enrichment_utils import update_streaming_info, ingest_film, ingest_films
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import update_letterboxd_stats
from dotenv import load_dotenv

load_dotenv(override=True)

def reingest_records(film_ids, film_limit=100, dryrun=False, verbose=False):
    films_to_reingest = film_ids[:film_limit]
    if dryrun:
        print(films_to_reingest[:10])
        return
    for film_id in tqdm(films_to_reingest):
        try:
            ingest_film(film_id, verbose=verbose)
        except Exception as e:
            print('Reingestion for {} failed ({})'.format(film_id, e))
            
def update_letterboxd_stats_records(film_ids, film_limit=100, dryrun=False, verbose=False):
    letterboxd_stats_to_update = film_ids[:film_limit]
    if dryrun:
        print(letterboxd_stats_to_update[:10])
        return
    for film_id in tqdm(letterboxd_stats_to_update):
        try:
            update_letterboxd_stats(film_id, verbose=verbose)
        except Exception as e:
            print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))

def update_tmdb_metadata_records(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    tmdb_metadata_to_update = film_ids[:film_limit]
    if dryrun:
        print(tmdb_metadata_to_update[:10])
        return
    for film_id in tqdm(tmdb_metadata_to_update):
        try:
            update_tmbd_metadata(film_id, verbose=verbose)
        except Exception as e:
            print('Update of TMDB Metadata for {} failed ({})'.format(film_id, e))
        
def update_streaming_records(film_ids, film_limit=100, dryrun=False, verbose=False):
    streaming_to_update = film_ids[:film_limit]
    if dryrun:
        print(streaming_to_update[:10])
        return
    for film_id in tqdm(streaming_to_update):
        try:
            update_streaming_info(film_id, verbose=verbose)
        except Exception as e:
            print('Update of streaming info for {} failed ({})'.format(film_id, e))
            if '429' in str(e):
                print('Too Many Requests - Exit Update')
                break

def update_oldest_records(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        oldest_lb_records = film_ids
        oldest_tmdb_records = film_ids
        oldest_streaming_records = film_ids
    else:
        oldest_lb_records = get_film_ids_from_select_statement(oldest_letterboxd_stats_select_statement)
        oldest_tmdb_records = get_film_ids_from_select_statement(oldest_tmdb_metadata_select_statement)
        oldest_streaming_records = get_film_ids_from_select_statement(oldest_streaming_select_statement)
    
    print('In total, we are going to update the oldest {} records for letterboxd stats'.format(film_limit))
    update_letterboxd_stats_records(film_ids=oldest_lb_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    print('In total, we are going to update the oldest {} records for tmdb metadata'.format(film_limit))
    update_tmdb_metadata_records(film_ids=oldest_tmdb_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    print('In total, we are going to update the oldest {} records for streaming'.format(film_limit*10))
    update_streaming_records(film_ids=oldest_streaming_records, film_limit=film_limit*10, dryrun=dryrun, verbose=verbose)

def update_most_popular_records(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        lb_most_popular_records = film_ids    
    else:
        lb_most_popular_records = get_film_ids_from_select_statement(popular_letterboxd_stats_select_statement)
    print('In total, we are going to update the {} most popular record\'s letterboxd stats'.format(film_limit))
    update_letterboxd_stats_records(film_ids=lb_most_popular_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)

def update_letterboxd_top_250(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        lb_top_250_records = film_ids    
    else:
        lb_top_250_records = get_film_ids_from_select_statement(letterboxd_top_250_select_statement)
    print('In total, we are going to update the {} oldest updated records in the letterboxd top 250'.format(film_limit))
    update_letterboxd_stats_records(film_ids=lb_top_250_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)

def update_recent_films(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        recent_films_records = film_ids    
    else:
        recent_films_records = get_film_ids_from_select_statement(recent_films_select_statement)
    print('In total, we are going to update {} recent records for letterboxd stats, tmdb metadata, and streaming'.format(film_limit))
    update_letterboxd_stats_records(film_ids=recent_films_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    update_tmdb_metadata_records(film_ids=recent_films_records, film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    update_streaming_records(film_ids=recent_films_records, film_limit=film_limit*10, dryrun=dryrun, verbose=verbose)

def update_upcoming_films(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        upcoming_films_records = film_ids    
    else:
        upcoming_films_records = get_film_ids_from_select_statement(upcoming_films_select_statement)
    upcoming_films_records = upcoming_films_records[:film_limit]
    print('In total, we are going to update {} upcoming records for letterboxd stats, tmdb metadata, and streaming'.format(len(upcoming_films_records)))
    ingest_films(upcoming_films_records)

    

oldest_letterboxd_stats_select_statement = ("""

SELECT
	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE
	
FROM ALL_RELEASED_FILMS a
LEFT JOIN FILM_LETTERBOXD_STATS b
ON a.FILM_ID = b.FILM_ID

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")

oldest_tmdb_metadata_select_statement = ("""

SELECT
	 a.FILM_ID
	,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE
	
FROM ALL_RELEASED_FILMS a
LEFT JOIN FILM_TMDB_STATS b
ON a.FILM_ID = b.FILM_ID

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")

oldest_streaming_select_statement = ("""

WITH DAYS_SINCE_LAST_STREAMING_UPDATE AS (

SELECT FILM_ID, ROUND(AVG(DAYS_SINCE_LAST_UPDATE), 0) AS DAYS_SINCE_LAST_UPDATE 
FROM ( SELECT FILM_ID, ROUND(COALESCE(julianday('now') - julianday(CREATED_AT), 99), 0) AS DAYS_SINCE_LAST_UPDATE FROM FILM_STREAMING_SERVICES )
GROUP BY FILM_ID

)

SELECT a.FILM_ID
FROM FILM_ALGO_SCORE a
LEFT JOIN DAYS_SINCE_LAST_STREAMING_UPDATE b
ON a.FILM_ID = b.FILM_ID
ORDER BY COALESCE(a.ALGO_SCORE,0.01) * COALESCE(b.DAYS_SINCE_LAST_UPDATE, 365) DESC

""")

popular_letterboxd_stats_select_statement = ("""

WITH BASE_TABLE AS (

	SELECT
	
		a.FILM_ID
		,b.FILM_TITLE
		,c.FILM_WATCH_COUNT
		,ROUND(COALESCE(julianday('now') - julianday(c.CREATED_AT), 99), 0) AS DAYS_SINCE_LAST_UPDATE
	
	FROM ALL_FEATURE_FILMS a
	LEFT JOIN FILM_TITLE b
	ON a.FILM_ID = b.FILM_ID
	LEFT JOIN FILM_LETTERBOXD_STATS c
	ON a.FILM_ID = c.FILM_ID

	)
	
SELECT
	*
	,ROUND(POWER(FILM_WATCH_COUNT, 1) * POWER(DAYS_SINCE_LAST_UPDATE, 1.5)) AS SORT_KEY
FROM BASE_TABLE
ORDER BY SORT_KEY DESC

""")

letterboxd_top_250_select_statement = ("""

SELECT

	a.FILM_ID
	,b.FILM_TITLE
	,c.FILM_WATCH_COUNT
	,ROUND(COALESCE(julianday('now') - julianday(c.CREATED_AT), 99), 0) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FEATURE_FILMS a
LEFT JOIN FILM_TITLE b
ON a.FILM_ID = b.FILM_ID
LEFT JOIN FILM_LETTERBOXD_STATS c
ON a.FILM_ID = c.FILM_ID
INNER JOIN FILM_LETTERBOXD_TOP_250 d
ON a.FILM_ID = d.FILM_ID

WHERE DAYS_SINCE_LAST_UPDATE > 7

ORDER BY DAYS_SINCE_LAST_UPDATE DESC

""")

current_year = int(datetime.now().year)

recent_films_select_statement = ("""

SELECT
	a.FILM_ID
	
FROM ALL_FILMS a

LEFT JOIN FILM_YEAR b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN FILM_RELEASE_INFO c
ON a.FILM_ID = c.FILM_ID

LEFT JOIN FILM_LAST_UPDATED d
ON a.FILM_ID = d.FILM_ID

LEFT JOIN CONTENT_TYPE e
ON a.FILM_ID = e.FILM_ID

WHERE e.CONTENT_TYPE = 'movie'
AND COALESCE(c.FILM_STATUS, "None")  = "Released"
AND b.FILM_YEAR BETWEEN {} AND {}
AND d.MEAN_DAYS_SINCE_LAST_UPDATE > 7
                                 
ORDER BY d.MEAN_DAYS_SINCE_LAST_UPDATE DESC

""".format(current_year-1, current_year))

upcoming_films_select_statement = ("""

SELECT
                                   
	a.FILM_ID
	
FROM ALL_FILMS a

LEFT JOIN FILM_YEAR b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN FILM_RELEASE_INFO c
ON a.FILM_ID = c.FILM_ID

LEFT JOIN FILM_LAST_UPDATED d
ON a.FILM_ID = d.FILM_ID

LEFT JOIN CONTENT_TYPE e
ON a.FILM_ID = e.FILM_ID

WHERE e.CONTENT_TYPE = 'movie'
AND COALESCE(c.FILM_STATUS, "None") != "Released"
AND b.FILM_YEAR >= {}
AND d.MEAN_DAYS_SINCE_LAST_UPDATE > 7
                                 
ORDER BY d.MEAN_DAYS_SINCE_LAST_UPDATE DESC

""".format(current_year))