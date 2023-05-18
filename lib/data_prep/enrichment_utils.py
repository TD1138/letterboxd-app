import os
import json
import re
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from random import sample
import time
from tqdm import tqdm
from sqlite_utils import table_to_df, get_from_table, insert_record_into_table, update_record, replace_record, df_to_table, delete_records, select_statement_to_df
from export_utils import exportfile_to_df, convert_uri_to_id
from tmdb_utils import get_tmbd_metadata, update_tmbd_metadata
from selenium_utils import return_logged_in_page_source
from justwatch import JustWatch
from dotenv import load_dotenv

load_dotenv()

def get_all_films():
    watched_df = exportfile_to_df('watched.csv')
    watched_film_ids = [convert_uri_to_id(x) for x in watched_df['LETTERBOXD_URI'].values]
    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_film_ids = [convert_uri_to_id(x) for x in watchlist_df['LETTERBOXD_URI'].values]
    all_film_ids = watched_film_ids + watchlist_film_ids
    return all_film_ids

def get_ingested_films(error_type=None, shuffle=True):
    valid_error_types = ['LETTERBOXD_ERROR', 'METADATA_ERROR', 'STREAMING_ERROR', 'TOTAL_INGESTION_ERRORS']
    try:
        ingested_df = table_to_df('INGESTED')
        if error_type:
            if error_type in valid_error_types:
                ingested_df = ingested_df[ingested_df[error_type] > 0]
            else:
                return print('error_type parameter, if passed, must be one of {}'.format(', '.join(valid_error_types)))
        ingested_film_ids = ingested_df['FILM_ID'].values
        if shuffle: ingested_film_ids = sample(list(ingested_film_ids), len(ingested_film_ids))
    except:
        ingested_film_ids = []
    return ingested_film_ids

def get_new_films():
    ingested_film_ids = get_ingested_films()
    all_film_ids = get_all_films()
    new_film_ids = [x for x in all_film_ids if x not in ingested_film_ids]
    new_film_ids = sample(new_film_ids, len(new_film_ids))
    return new_film_ids

def get_film_ids_from_select_statement(select_statement):
    sql_df = select_statement_to_df(select_statement)
    try:
        sql_film_ids = list(sql_df['FILM_ID'].values)
        return sql_film_ids
    except:
        print('select statement must output "FILM_ID" column')

def update_ingestion_table(film_id):
    ingestion_record = {
        'FILM_ID': film_id,
        'INGESTION_DATETIME':datetime.now()
    }
    replace_record('INGESTED', ingestion_record, film_id)

def get_ext_ids_plus_content_type(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    if r.status_code != 200:
        return
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    tmdb_valid = 1
    try:
        tmdb_url = soup.find('a', {'data-track-action': 'TMDb'}).get('href')
        if tmdb_url[27:32] == 'movie':
            tmdb_id = tmdb_url.replace('https://www.themoviedb.org/movie/', '').replace('/', '')
            content_type = 'movie'
        elif tmdb_url[27:29] == 'tv':
            tmdb_id = tmdb_url.replace('https://www.themoviedb.org/tv/', '').replace('/', '')
            content_type = 'tv'
        else:
            tmdb_id = None
            content_type = None
            tmdb_valid = 0
    except:
        tmdb_id = None
        content_type = None
        tmdb_valid = 0

    try:
        imdb_url = soup.find('a', {'data-track-action': 'IMDb'}).get('href')
        imdb_id = imdb_url.replace('http://www.imdb.com/title/', '').replace('/maindetails', '')
    except:
        imdb_id = None

    imdb_record = {
        'FILM_ID': film_id,
        'IMDB_ID': imdb_id,
        'CREATED_AT':datetime.now()
    }
    replace_record('IMDB_ID', imdb_record, film_id)
    tmdb_dict = {
        'FILM_ID': film_id,
        'TMDB_ID':tmdb_id,
        'CREATED_AT':datetime.now(),
        'VALID': tmdb_valid
    }
    replace_record('TMDB_ID', tmdb_dict, film_id)
    content_dict = {
        'FILM_ID': film_id,
        'CONTENT_TYPE':content_type,
        'CREATED_AT':datetime.now()
    }
    replace_record('CONTENT_TYPE', content_dict, film_id)

def get_metadata_from_letterboxd(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    # import ipdb; ipdb.set_trace()
    update_record("FILM_TITLE", "FILM_URL_TITLE", film, film_id)
    try:
        year = soup.find('small', {'class': 'number'}).text
    except:
        year = int(datetime.now().strftime('%Y')) + 2
    film_year_dict = {
        'FILM_ID': film_id,
        'FILM_YEAR':year,
        'FILM_DECADE': str(year)[:3]+'0s',
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_YEAR', film_year_dict, film_id)
    genre_list = [x.get('href').replace('/films/genre/', '').replace('/', '') for x in soup.findAll('a', {'class':'text-slug'}) if 'genre' in str(x.get('href'))]
    if not genre_list:
        genre_list = ['none']
    film_genre_dict = {
        'FILM_ID': film_id,
        'FILM_GENRE':genre_list[0],
        'ALL_FILM_GENRES': '/'.join(genre_list),
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_GENRE', film_genre_dict, film_id)

def get_letterboxd_top250_status(film_id):
    film_url_title = get_from_table('FILM_TITLE', film_id, 'FILM_URL_TITLE')
    r = requests.get('https://letterboxd.com/esi/film/{}/stats/'.format(film_url_title))
    soup = BeautifulSoup(r.content, 'lxml')
    try:
        top_250_status = int(soup.find('a', {'class': 'has-icon icon-top250 icon-16 tooltip'}).text)
    except:
        top_250_status = None
    return top_250_status

def get_letterboxd_rating(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    rating_dict = json.loads(soup.find('script', {'type':"application/ld+json"}).string.split('\n')[2]).get('aggregateRating')
    try:
        rating_mean = rating_dict.get('ratingValue')
    except:
        rating_mean = np.nan
    try:
        rating_count = rating_dict.get('ratingCount')
    except:
        rating_count = np.nan
    return rating_mean, rating_count

def get_letterboxd_metrics(film_id):
    film_url_title = get_from_table('FILM_TITLE', film_id, 'FILM_URL_TITLE')
    r = requests.get('https://letterboxd.com/film/{}/members/rated/.5-5/'.format(film_url_title))
    soup = BeautifulSoup(r.content, 'lxml')
    metrics_dict = {}
    for i in ['members', 'fans', 'likes', 'reviews', 'lists']:
        href_str = '/film/{}/{}/'.format(film_url_title, i)
        try:
            metric_string = soup.find('a', {'class': 'tooltip', 'href':href_str}).get('title')
            metric = int(metric_string[:metric_string.find('\xa0')].replace(',', ''))
        except:
            metric = 0
        metrics_dict[i] = metric
    return metrics_dict

def update_letterboxd_stats(film_id):
    rating_mean, rating_count = get_letterboxd_rating(film_id)
    metrics_dict = get_letterboxd_metrics(film_id)
    letterboxd_info_dict = {
        'FILM_ID': film_id,
        'FILM_WATCH_COUNT': metrics_dict['members'],
        'FILM_FAN_COUNT': metrics_dict['fans'],
        'FILM_LIKES_COUNT': metrics_dict['likes'],
        'FILM_REVIEW_COUNT': metrics_dict['reviews'],
        'FILM_LIST_COUNT': metrics_dict['lists'],
        'FILM_TOP_250': get_letterboxd_top250_status(film_id),
        'FILM_RATING': rating_mean,
        'FILM_RATING_COUNT': rating_count,
        'CREATED_AT': datetime.now()
    }
    insert_record_into_table(letterboxd_info_dict, 'FILM_LETTERBOXD_STATS')

def update_streaming_info(film_id):
    with open('my_streaming_services.json', 'r') as schema:
        my_streaming_services = json.load(schema)
    my_streaming_services_abbr = [x for x in set([x['provider_abbreviation'] for x in my_streaming_services]) if len(x) > 0]
    abbr_to_full_dict = {x['provider_abbreviation']:x['streaming_service'] for x in my_streaming_services if len(x['provider_abbreviation']) > 0}
    just_watch = JustWatch(country='GB')
    film_url_title = get_from_table('FILM_TITLE', film_id, 'FILM_URL_TITLE')
    film_release_year = get_from_table('FILM_YEAR', film_id, 'FILM_YEAR')
    # import ipdb; ipdb.set_trace()
    results = just_watch.search_for_item(query=film_url_title, release_year_from=film_release_year-1, release_year_until=film_release_year+1)
    delete_records('FILM_STREAMING_SERVICES', film_id)
    if len(results['items']) > 0:
        first_result = results['items'][0]
        if first_result.get('title') == get_from_table('FILM_TITLE', film_id, 'FILM_TITLE'):
            provider_abbreviations = list(set([x['package_short_name'] for x in first_result.get('offers', []) if x['monetization_type'] in ['flatrate', 'free', 'ads']]))
            valid_abbr = [x for x in provider_abbreviations if x in my_streaming_services_abbr]
            if len(valid_abbr) > 0:
                valid_full = [abbr_to_full_dict.get(x) for x in valid_abbr]
                film_streaming_services_df = pd.DataFrame(index=range(len(valid_abbr)))
                film_streaming_services_df['FILM_ID'] = film_id
                film_streaming_services_df['STREAMING_SERVICE_ABBR'] = valid_abbr
                film_streaming_services_df['STREAMING_SERVICE_FULL'] = valid_full
                film_streaming_services_df['CREATED_AT'] = datetime.now()
                df_to_table(film_streaming_services_df, 'FILM_STREAMING_SERVICES', replace_append='append', verbose=False)

def ingest_film(film_id):
    try:
        update_all_letterboxd_info(film_id)
    except Exception as e:
        print('Update of Letterboxd info for {} failed ({})'.format(film_id, e))
    try:
        get_tmbd_metadata(film_id)
    except Exception as e:
        print('Update of film metadata info for {} failed ({})'.format(film_id, e))
    try:
        update_streaming_info(film_id)
    except Exception as e:
        print('Update of streaming info for {} failed ({})'.format(film_id, e))
    update_ingestion_table(film_id)

def update_all_letterboxd_info(film_id):
    try:
        get_ext_ids_plus_content_type(film_id)
    except Exception as e:
        print('failed to get the TMDB ID for {} ({})'.format(film_id, e))
    try:
        get_metadata_from_letterboxd(film_id)
    except Exception as e:
        print('failed to get letterboxd metadata for {} ({})'.format(film_id, e))
    try:
        update_letterboxd_stats(film_id)
    except Exception as e:
        print('failed to get letterboxd stats for {} ({})'.format(film_id, e))
    update_ingestion_table(film_id)

def ingest_new_films():
    films_to_ingest = get_new_films()
    print('In total, there are {} new films to ingest'.format(len(films_to_ingest)))
    for film_id in tqdm(films_to_ingest):
        ingest_film(film_id)
