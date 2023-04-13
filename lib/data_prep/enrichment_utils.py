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
from sqlite_utils import table_to_df, get_from_table, insert_record_into_table, update_record, df_to_table, delete_records
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

def get_ingested_films(error_type=None):
    valid_error_types = ['LETTERBOXD_ERROR', 'METADATA_ERROR', 'STREAMING_ERROR', 'TOTAL_INGESTION_ERRORS']
    try:
        ingested_df = table_to_df('INGESTED')
        if error_type:
            if error_type in valid_error_types:
                ingested_df = ingested_df[ingested_df[error_type] > 0]
            else:
                return print('error_type parameter, if passed, must be one of {}'.format(', '.join(valid_error_types)))
        ingested_film_ids = ingested_df['FILM_ID'].values
        ingested_film_ids = sample(list(ingested_film_ids), len(ingested_film_ids))
    except:
        ingested_film_ids = []
    return ingested_film_ids

def get_new_films():
    ingested_film_ids = get_ingested_films()
    all_film_ids = get_all_films()
    new_film_ids = [x for x in all_film_ids if x not in ingested_film_ids]
    new_film_ids = sample(new_film_ids, len(new_film_ids))
    return new_film_ids

def update_letterboxd_info(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    try:
        year = int(list(re.search(r'\((.*?)\)', soup.find('meta', {'property': 'og:title'}).get('content')).groups())[0])
    except:
        year = int(datetime.now().strftime('%Y')) + 2
    genre_list = [x.get('href').replace('/films/genre/', '').replace('/', '') for x in soup.findAll('a', {'class':'text-slug'}) if 'genre' in str(x.get('href'))]
    if not genre_list:
        genre_list = ['none']
    rating_dict = json.loads(soup.find('script', {'type':"application/ld+json"}).string.split('\n')[2]).get('aggregateRating')
    try:
        rating_mean = rating_dict.get('ratingValue')
    except:
        rating_mean = np.nan
    try:
        rating_count = rating_dict.get('ratingCount')
    except:
        rating_count = np.nan
    r = requests.get('https://letterboxd.com/film/{}/members/rated/.5-5/'.format(film))
    # import ipdb; ipdb.set_trace()
    soup = BeautifulSoup(r.content, 'lxml')
    metrics_dict = {}
    for i in ['members', 'fans', 'likes', 'reviews', 'lists']:
        href_str = '/film/{}/{}/'.format(film, i)
        try:
            metric_string = soup.find('a', {'class': 'tooltip', 'href':href_str}).get('title')
            metric = int(metric_string[:metric_string.find('\xa0')].replace(',', ''))
        except:
            metric = 0
        metrics_dict[i] = metric
    r = requests.get('https://letterboxd.com/esi/film/{}/stats/'.format(film))
    soup = BeautifulSoup(r.content, 'lxml')
    try:
        top_ = int(soup.find('a', {'class': 'has-icon icon-top250 icon-16 tooltip'}).text)
    except:
        top_ = None
    letterboxd_info_dict = {
        'FILM_ID': film_id,
        'FILM_WATCH_COUNT': metrics_dict['members'],
        'FILM_FAN_COUNT': metrics_dict['fans'],
        'FILM_LIKES_COUNT': metrics_dict['likes'],
        'FILM_REVIEW_COUNT': metrics_dict['reviews'],
        'FILM_LIST_COUNT': metrics_dict['lists'],
        'FILM_TOP_250': top_,
        'FILM_RATING': rating_mean,
        'FILM_RATING_COUNT': rating_count,
    }
    insert_record_into_table(letterboxd_info_dict, 'FILM_LETTERBOXD_STATS')
    update_record("FILM_TITLE", "FILM_URL_TITLE", film, film_id)
    film_year_dict = {
        'FILM_ID': film_id,
        'FILM_YEAR':year,
        'FILM_DECADE': str(year)[:3]+'0s'
    }
    insert_record_into_table(film_year_dict, 'FILM_YEAR')
    film_genre_dict = {
        'FILM_ID': film_id,
        'FILM_GENRE':genre_list[0],
        'ALL_FILM_GENRES': '/'.join(genre_list)
    }
    insert_record_into_table(film_genre_dict, 'FILM_GENRE')

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

def ingest_film(film_id):
    ingestion_record = {
        'FILM_ID': film_id,
        'INGESTION_DATETIME':datetime.now(),
        'LETTERBOXD_ERROR':0,
        'METADATA_ERROR':0,
        'STREAMING_ERROR':0,
        'TOTAL_INGESTION_ERRORS': 0
                  }
    try:
        update_letterboxd_info(film_id)
    except:
        ingestion_record['TOTAL_INGESTION_ERRORS'] += 1
        ingestion_record['LETTERBOXD_ERROR'] = 1
        print('Update of Letterboxd info for {} failed'.format(film_id))
    # try:
    #     update_film_metadata(film_id)
    # except:
        # ingestion_record['TOTAL_INGESTION_ERRORS'] += 1
        # ingestion_record['METADATA_ERROR'] = 1
        # print('Update of film metadata info for {} failed'.format(film_id))
    try:
        update_streaming_info(film_id)
    except:
        ingestion_record['TOTAL_INGESTION_ERRORS'] += 1
        ingestion_record['STREAMING_ERROR'] = 1
        print('Update of streaming info for {} failed'.format(film_id))
    delete_records('INGESTED', film_id)
    insert_record_into_table(ingestion_record, 'INGESTED')