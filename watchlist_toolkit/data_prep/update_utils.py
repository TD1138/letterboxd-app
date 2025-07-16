from datetime import datetime
from tqdm import tqdm
from sqlite_utils import replace_record, get_film_ids_from_select_statement
from enrichment_utils import update_streaming_info, ingest_film, ingest_films
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import update_letterboxd_stats
from dotenv import load_dotenv
from watchlist_toolkit.utils.sql_loader import read_sql

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
            
def update_letterboxd_stats_records(film_ids, log_reason='UPDATE', film_limit=100, dryrun=False, verbose=False):
    letterboxd_stats_to_update = film_ids[:film_limit]
    if dryrun:
        print(letterboxd_stats_to_update[:10])
        return
    for film_id in tqdm(letterboxd_stats_to_update):
        try:
            update_letterboxd_stats(film_id, log_reason=log_reason, verbose=verbose)
        except Exception as e:
            print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))

def update_tmdb_metadata_records(film_ids=None, log_reason='UPDATE', film_limit=100, dryrun=False, verbose=False):
    tmdb_metadata_to_update = film_ids[:film_limit]
    if dryrun:
        print(tmdb_metadata_to_update[:10])
        return
    for film_id in tqdm(tmdb_metadata_to_update):
        try:
            update_tmbd_metadata(film_id, log_reason=log_reason, verbose=verbose)
        except Exception as e:
            print('Update of TMDB Metadata for {} failed ({})'.format(film_id, e))
        
def update_streaming_records(film_ids, log_reason='UPDATE', film_limit=100, dryrun=False, verbose=False):
    streaming_to_update = film_ids[:film_limit]
    if dryrun:
        print(streaming_to_update[:10])
        return
    for film_id in tqdm(streaming_to_update):
        try:
            update_streaming_info(film_id, log_reason=log_reason, verbose=verbose)
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
    update_letterboxd_stats_records(film_ids=oldest_lb_records, log_reason='UPDATE_OLDEST_RECORDS', film_limit=195, dryrun=dryrun, verbose=verbose)
    print('In total, we are going to update the oldest {} records for tmdb metadata'.format(film_limit))
    update_tmdb_metadata_records(film_ids=oldest_tmdb_records, log_reason='UPDATE_OLDEST_RECORDS', film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    print('In total, we are going to update the oldest {} records for streaming'.format(film_limit*10))
    update_streaming_records(film_ids=oldest_streaming_records, log_reason='UPDATE_OLDEST_RECORDS', film_limit=film_limit*10, dryrun=dryrun, verbose=verbose)

def update_most_popular_films(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        lb_most_popular_records = film_ids    
    else:
        lb_most_popular_records = get_film_ids_from_select_statement(popular_letterboxd_stats_select_statement)
    print('In total, we are going to update the {} most popular record\'s letterboxd stats'.format(film_limit))
    update_letterboxd_stats_records(film_ids=lb_most_popular_records, log_reason='UPDATE_POPULAR_FILMS', film_limit=film_limit, dryrun=dryrun, verbose=verbose)

def update_letterboxd_top_250(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        lb_top_250_records = film_ids    
    else:
        lb_top_250_records = get_film_ids_from_select_statement(letterboxd_top_250_select_statement)
    print('In total, we are going to update the {} oldest updated records in the letterboxd top 250'.format(film_limit))
    update_letterboxd_stats_records(film_ids=lb_top_250_records, log_reason='UPDATE_LBTOP250_FILMS', film_limit=film_limit, dryrun=dryrun, verbose=verbose)

def update_recent_films(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        recent_films_records = film_ids    
    else:
        recent_films_records = get_film_ids_from_select_statement(recent_films_select_statement)
    print('In total, we are going to update {} recent records for letterboxd stats, tmdb metadata, and streaming'.format(film_limit))
    update_letterboxd_stats_records(film_ids=recent_films_records, log_reason='UPDATE_RECENT_FILMS', film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    update_tmdb_metadata_records(film_ids=recent_films_records, log_reason='UPDATE_RECENT_FILMS', film_limit=film_limit, dryrun=dryrun, verbose=verbose)
    update_streaming_records(film_ids=recent_films_records, log_reason='UPDATE_RECENT_FILMS', film_limit=film_limit*10, dryrun=dryrun, verbose=verbose)

def update_upcoming_films(film_ids=None, film_limit=100, dryrun=False, verbose=False):
    load_dotenv(override=True)
    if film_ids:
        upcoming_films_records = film_ids    
    else:
        upcoming_films_records = get_film_ids_from_select_statement(upcoming_films_select_statement)
    upcoming_films_records = upcoming_films_records[:film_limit]
    print('In total, we are going to update {} upcoming records for letterboxd stats, tmdb metadata, and streaming'.format(len(upcoming_films_records)))
    ingest_films(upcoming_films_records, log_reason='UPDATE_UPCOMING_FILMS')

    

# External SQL files loaded via watchlist_toolkit.utils.sql_loader
current_year = int(datetime.now().year)

oldest_letterboxd_stats_select_statement = read_sql('oldest_letterboxd_stats_select_statement')
oldest_tmdb_metadata_select_statement = read_sql('oldest_tmdb_metadata_select_statement')
oldest_streaming_select_statement = read_sql('oldest_streaming_select_statement')
popular_letterboxd_stats_select_statement = read_sql('popular_letterboxd_stats_select_statement')
letterboxd_top_250_select_statement = read_sql('letterboxd_top_250_select_statement')
recent_films_select_statement = read_sql('recent_films_select_statement').format(current_year-1, current_year)
upcoming_films_select_statement = read_sql('upcoming_films_select_statement').format(current_year)