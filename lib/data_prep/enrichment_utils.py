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
from sqlite_utils import table_to_df, get_from_table, insert_record_into_table, update_record, df_to_table, delete_records, select_statement_to_df
from export_utils import exportfile_to_df, convert_uri_to_id
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
        sql_film_ids = sql_df['FILM_ID'].values
        return sql_film_ids
    except:
        print('select statement must output "FILM_ID" column')

def get_tmdb_id(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    if r.status_code != 200:
        return
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    tmdb_url = soup.find('a', {'data-track-action': 'TMDb'}).get('href')
    tmdb_id = tmdb_url.replace('https://www.themoviedb.org/movie/', '').replace('/', '')
    tmdb_dict = {
        'FILM_ID': film_id,
        'TMDB_ID':tmdb_id
    }
    insert_record_into_table(tmdb_dict, 'TMDB_ID')

def get_metadata_from_letterboxd(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    update_record("FILM_TITLE", "FILM_URL_TITLE", film, film_id)
    try:
        year = int(list(re.search(r'\((.*?)\)', soup.find('meta', {'property': 'og:title'}).get('content')).groups())[0])
    except:
        year = int(datetime.now().strftime('%Y')) + 2
    film_year_dict = {
        'FILM_ID': film_id,
        'FILM_YEAR':year,
        'FILM_DECADE': str(year)[:3]+'0s'
    }
    insert_record_into_table(film_year_dict, 'FILM_YEAR')
    genre_list = [x.get('href').replace('/films/genre/', '').replace('/', '') for x in soup.findAll('a', {'class':'text-slug'}) if 'genre' in str(x.get('href'))]
    if not genre_list:
        genre_list = ['none']
    film_genre_dict = {
        'FILM_ID': film_id,
        'FILM_GENRE':genre_list[0],
        'ALL_FILM_GENRES': '/'.join(genre_list)
    }
    insert_record_into_table(film_genre_dict, 'FILM_GENRE')

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
    }
    insert_record_into_table(letterboxd_info_dict, 'FILM_LETTERBOXD_STATS')

def update_film_metadata(film_id):
    # ping the API
    return

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
    delete_records('FILMS_AVAILABLE_TO_STREAM', film_id)
    delete_records('FILM_STREAMING_SERVICES', film_id)
    if len(results['items']) > 0:
        first_result = results['items'][0]
        if first_result.get('title') == get_from_table('FILM_TITLE', film_id, 'FILM_TITLE'):
            provider_abbreviations = list(set([x['package_short_name'] for x in first_result.get('offers', []) if x['monetization_type'] in ['flatrate', 'free', 'ads']]))
            valid_abbr = [x for x in provider_abbreviations if x in my_streaming_services_abbr]
            if len(valid_abbr) > 0:
                insert_record_into_table({'FILM_ID':film_id}, 'FILMS_AVAILABLE_TO_STREAM')
                valid_full = [abbr_to_full_dict.get(x) for x in valid_abbr]
                film_streaming_services_df = pd.DataFrame(index=range(len(valid_abbr)))
                film_streaming_services_df['FILM_ID'] = film_id
                film_streaming_services_df['STREAMING_SERVICE_ABBR'] = valid_abbr
                film_streaming_services_df['STREAMING_SERVICE_FULL'] = valid_full
                df_to_table(film_streaming_services_df, 'FILM_STREAMING_SERVICES', replace_append='append', verbose=True)
    current_ingestion_record = get_from_table('INGESTED', film_id)
    new_ingestion_record = ingestion_record(film_id, blank=True)
    for error_name in ['LETTERBOXD_ERROR', 'METADATA_ERROR']:
        new_ingestion_record.update_record(error_name, current_ingestion_record.get(error_name))
    new_ingestion_record.update_record('STREAMING_ERROR', 0)
    delete_records('INGESTED', film_id)
    insert_record_into_table(new_ingestion_record.dict, 'INGESTED')

class ingestion_record:
    def __init__(self, film_id, blank=False):
        if blank:
            self.dict = {
                'FILM_ID': film_id,
                'INGESTION_DATETIME':datetime.now(),
                'LETTERBOXD_ERROR':0,
                'METADATA_ERROR':0,
                'STREAMING_ERROR':0,
                'TOTAL_INGESTION_ERRORS': 0
                        }
        else:
            existing_record = get_from_table('INGESTED', film_id)
            self.dict = {
            'FILM_ID': film_id,
            'INGESTION_DATETIME': existing_record.get('INGESTION_DATETIME'),
            'LETTERBOXD_ERROR': existing_record.get('LETTERBOXD_ERROR', 0),
            'METADATA_ERROR': existing_record.get('METADATA_ERROR', 0),
            'STREAMING_ERROR': existing_record.get('STREAMING_ERROR', 0),
            'TOTAL_INGESTION_ERRORS': existing_record.get('TOTAL_INGESTION_ERRORS', 0)
                    }

    def recalculate_total_errors(self):
        self.dict['TOTAL_INGESTION_ERRORS'] = self.dict['LETTERBOXD_ERROR'] + \
                                              self.dict['METADATA_ERROR'] + \
                                              self.dict['STREAMING_ERROR']                                                 
    def update_record(self, key, value):
        self.dict[key] = value
        self.recalculate_total_errors()

def ingest_film(film_id):
    blank_ingestion_record = ingestion_record(film_id, blank=True)
    try:
        update_all_letterboxd_info(film_id)
    except:
        blank_ingestion_record.update_record('LETTERBOXD_ERROR', 1)
        print('Update of Letterboxd info for {} failed'.format(film_id))
    # try:
    #     update_film_metadata(film_id)
    # except:
        # blank_ingestion_record.update_record('METADATA_ERROR', 1)
        # print('Update of film metadata info for {} failed'.format(film_id))
    try:
        update_streaming_info(film_id)
    except:
        blank_ingestion_record.update_record('STREAMING_ERROR', 1)
        print('Update of streaming info for {} failed'.format(film_id))
    delete_records('INGESTED', film_id)
    insert_record_into_table(blank_ingestion_record.dict, 'INGESTED')

def update_all_letterboxd_info(film_id):
    existing_ingestion_record = ingestion_record(film_id)
    reingestion_errors = 0
    try:
        get_tmdb_id(film_id)
    except:
        print('failed to get the TMDB ID')
        reingestion_errors += 1
    try:
        get_metadata_from_letterboxd(film_id)
    except:
        print('failed to get letterboxd metadata')
        reingestion_errors += 1
    try:
        update_letterboxd_stats(film_id)
    except:
        print('failed to get letterboxd stats')
        reingestion_errors += 1
    reingestion_errors = min(reingestion_errors, 1)
    existing_ingestion_record.update_record('LETTERBOXD_ERROR', reingestion_errors)
    delete_records('INGESTED', film_id)
    insert_record_into_table(existing_ingestion_record.dict, 'INGESTED')

def update_film(film_id):
    existing_ingestion_record = ingestion_record(film_id)
    try:
        update_letterboxd_stats(film_id)
    except:
        existing_ingestion_record.update_record('LETTERBOXD_ERROR', 1)
        print('Update of Letterboxd info for {} failed'.format(film_id))
    try:
        update_streaming_info(film_id)
    except:
        existing_ingestion_record.update_record('STREAMING_ERROR', 1)
        print('Update of streaming info for {} failed'.format(film_id))
    delete_records('INGESTED', film_id)
    insert_record_into_table(existing_ingestion_record.dict, 'INGESTED')

def ingest_new_films():
    films_to_ingest = get_new_films()
    print('In total, there are {} new films to ingest'.format(len(films_to_ingest)))
    for film_id in tqdm(films_to_ingest):
        ingest_film(film_id)

def update_existing_films(film_limit=150):
    films_to_update = get_ingested_films(error_type=None, shuffle=False)[:film_limit]
    print('In total, we are going to update the oldest {} ingestion records'.format(len(films_to_update)))
    for film_id in tqdm(films_to_update):
        update_film(film_id)
