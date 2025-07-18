from tqdm import tqdm
from sqlite_utils import get_film_ids_from_select_statement
from enrichment_utils import ingest_film
from tmdb_utils import update_tmbd_metadata
from letterboxd_utils import get_ext_ids_plus_content_type, get_metadata_from_letterboxd, update_letterboxd_stats
from dotenv import load_dotenv
from watchlist_toolkit.utils.sql_loader import read_sql

load_dotenv(override=True)

def correct_letterboxd_stats_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
    else:
        letterboxd_stats_films_to_correct = get_film_ids_from_select_statement(letterboxd_stats_select_statement)
        total_films_to_correct = list(set(    \
            letterboxd_stats_films_to_correct \
            ))
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} new films to correct letterboxd stats for - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id, log_reason='CORRECTION_LB_STATS')
            else:
                update_letterboxd_stats(film_id, log_reason='CORRECTION_LB_STATS')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected letterboxd stats for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_letterboxd_metadata_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
    else:
        title_films_to_correct = get_film_ids_from_select_statement(title_select_statement)
        genre_films_to_correct = get_film_ids_from_select_statement(genre_select_statement)
        total_films_to_correct = list(set(          \
            title_films_to_correct             \
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
                ingest_film(film_id, log_reason='CORRECTION_LB_METADATA')
            else:
                get_metadata_from_letterboxd(film_id, log_reason='CORRECTION_LB_METADATA')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected letterboxd_metadata for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_ext_ids_plus_content_type_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
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
                ingest_film(film_id, log_reason='CORRECTION_EXT_IDS')
            else:
                get_ext_ids_plus_content_type(film_id, log_reason='CORRECTION_EXT_IDS')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected external ids & content type for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_tmdb_metadata_errors(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
    else:
        financials_films_to_correct = get_film_ids_from_select_statement(financials_select_statement)
        tmdb_stats_films_to_correct = get_film_ids_from_select_statement(tmdb_stats_select_statement)
        release_info_films_to_correct = get_film_ids_from_select_statement(release_info_select_statement)
        keyword_films_to_correct = get_film_ids_from_select_statement(keyword_select_statement)    
        cast_films_to_correct = get_film_ids_from_select_statement(cast_select_statement)
        crew_films_to_correct = get_film_ids_from_select_statement(crew_select_statement)
        runtime_films_to_correct = get_film_ids_from_select_statement(runtime_select_statement)
        total_films_to_correct = list(set(    \
            financials_films_to_correct       \
            + tmdb_stats_films_to_correct     \
            + release_info_films_to_correct   \
            + keyword_films_to_correct        \
            + cast_films_to_correct           \
            + crew_films_to_correct           \
            + runtime_films_to_correct        \
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
                ingest_film(film_id, log_reason='CORRECTION_TMDB_METADATA')
            else:
                update_tmbd_metadata(film_id, log_reason='CORRECTION_TMDB_METADATA')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected tmdb metadata for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_collection_name_mismatches(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
    else:
        collection_issue_films_to_correct = get_film_ids_from_select_statement(collection_issue_select_statement)
        total_films_to_correct = collection_issue_films_to_correct
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} films with collection name mismatches - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id, log_reason='CORRECTION_COLLECTION_NAME')
            else:
                update_tmbd_metadata(film_id, log_reason='CORRECTION_COLLECTION_NAME')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected collection names for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_release_date_mismatches(film_ids=None, refresh=False, dryrun=False, film_limit=1000):
    if film_ids:
        total_films_to_correct = film_ids
        films_to_correct = total_films_to_correct[:film_limit]
    else:
        release_date_issue_films_to_correct = get_film_ids_from_select_statement(release_date_issue_select_statement)
        total_films_to_correct = release_date_issue_films_to_correct
        films_to_correct = total_films_to_correct[:film_limit]
    print('In total, there are {} films with release date mismatches - correcting {}'.format(len(total_films_to_correct), len(films_to_correct)))
    if dryrun:
        print(films_to_correct[:10])
        return
    elif len(films_to_correct) == 0:
        return
    errors = 0
    for film_id in tqdm(films_to_correct):
        try:
            if refresh:
                ingest_film(film_id, log_reason='CORRECTION_RELEASE_DATE')
            else:
                update_tmbd_metadata(film_id, log_reason='CORRECTION_RELEASE_DATE')
        except:
            errors += 1
    successful_films = len(films_to_correct) - errors
    print('Corrected release date mismatches for {} films ({:.2%})'.format(successful_films, successful_films/len(films_to_correct)))

def correct_all_errors(film_ids=None, refresh=False, dryrun=False, film_limit=100):
    load_dotenv(override=True)
    correct_letterboxd_stats_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_letterboxd_metadata_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_ext_ids_plus_content_type_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_tmdb_metadata_errors(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit*10)
    correct_collection_name_mismatches(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)
    correct_release_date_mismatches(film_ids, refresh=refresh, dryrun=dryrun, film_limit=film_limit)

# Externalised SQL queries
title_select_statement = read_sql('title_select_statement')
genre_select_statement = read_sql('genre_select_statement')
imdb_id_select_statement = read_sql('imdb_id_select_statement')
tmdb_id_select_statement = read_sql('tmdb_id_select_statement')
content_type_select_statement = read_sql('content_type_select_statement')
financials_select_statement = read_sql('financials_select_statement')
letterboxd_stats_select_statement = read_sql('letterboxd_stats_select_statement')
tmdb_stats_select_statement = read_sql('tmdb_stats_select_statement')
release_info_select_statement = read_sql('release_info_select_statement')
keyword_select_statement = read_sql('keyword_select_statement')
cast_select_statement = read_sql('cast_select_statement')
crew_select_statement = read_sql('crew_select_statement')
runtime_select_statement = read_sql('runtime_select_statement')
collection_issue_select_statement = read_sql('collection_issue_select_statement')
release_date_issue_select_statement = read_sql('release_date_issue_select_statement')