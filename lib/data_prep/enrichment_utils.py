import os
import json
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime as datetime
import time
from sqlite_utils import table_to_df, get_from_table, insert_record_into_table, update_record, df_to_table
from export_utils import exportfile_to_df, convert_uri_to_id
from selenium_utils import return_logged_in_page_source
from justwatch import JustWatch
from dotenv import load_dotenv

load_dotenv()

def get_ingested_films():
    try:
        ingested_df = table_to_df('INGESTED')
        ingested_film_list = ingested_df['FILM_ID'].values
    except:
        ingested_film_list = []
    return ingested_film_list

def read_all_films():
    watched_df = exportfile_to_df('watched.csv')
    watched_film_ids = [convert_uri_to_id(x) for x in watched_df['LETTERBOXD_URI'].values]
    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_film_ids = [convert_uri_to_id(x) for x in watchlist_df['LETTERBOXD_URI'].values]
    all_film_ids = watched_film_ids + watchlist_film_ids
    return all_film_ids

def get_new_films():
    ingested_film_list = get_ingested_films()
    all_films_latest_export = read_all_films()
    new_films = [x for x in all_films_latest_export if x not in ingested_film_list]
    return new_films

def get_letterboxd_info(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    year = int(list(re.search(r'\((.*?)\)', soup.find('meta', {'property': 'og:title'}).get('content')).groups())[0])
    rating_dict = json.loads(soup.find('script', {'type':"application/ld+json"}).string.split('\n')[2]).get('aggregateRating')
    rating_mean = rating_dict.get('ratingValue')
    rating_count = rating_dict.get('ratingCount')
    r = requests.get('https://letterboxd.com/film/{}/members/rated/.5-5/'.format(film))
    soup = BeautifulSoup(r.content, 'lxml')
    metrics_dict = {}
    for i in ['members', 'fans', 'likes', 'reviews', 'lists']:
        href_str = '/film/{}/{}/'.format(film, i)
        metric_string = soup.find('a', {'class': 'tooltip', 'href':href_str}).get('title')
        metric = int(metric_string[:metric_string.find('\xa0')].replace(',', ''))
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

def get_film_metadata(film_id):
    watched_df = exportfile_to_df('watched.csv')
    watched_df['FILM_ID'] = watched_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    df_to_table(watched_df[['FILM_ID']], 'WATCHED', replace_append='replace')
    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_df['FILM_ID'] = watchlist_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    watchlist_df.columns = ['ADDED_DATE', 'NAME', 'YEAR', 'LETTERBOXD_URI', 'FILM_ID']
    df_to_table(watchlist_df[['FILM_ID', 'ADDED_DATE']], 'WATCHLIST', replace_append='replace')
    all_films_df = pd.concat([watched_df, watchlist_df])
    all_films_df['FILM_URL_TITLE'] = ''
    title_df = all_films_df[['FILM_ID', 'NAME', 'FILM_URL_TITLE', 'LETTERBOXD_URI']]
    title_df.columns = ['FILM_ID', 'FILM_TITLE', 'FILM_URL_TITLE', 'LETTERBOXD_URL']
    df_to_table(title_df, 'FILM_TITLE', replace_append='replace')
    return

def get_streaming_info(film_id):
    film_url_title = get_from_table('FILM_TITLE', film_id, 'FILM_URL_TITLE')
    with open('my_streaming_services.json', 'r') as schema:
        my_streaming_services = json.load(schema)
    my_streaming_services_abbr = [x for x in set([x['provider_abbreviation'] for x in my_streaming_services]) if len(x) > 0]
    just_watch = JustWatch(country='GB')
    release_year = 1947
    results = just_watch.search_for_item(query=film_url_title, release_year_from=release_year-1, release_year_until=release_year+1)
    first_result = results['items'][0]
    provider_abbreviations = list(set([x['package_short_name'] for x in first_result['offers'] if x['monetization_type'] in ['flatrate', 'free', 'ads']]))
    valid_abbreviations = [x for x in provider_abbreviations if x in my_streaming_services_abbr]
    return valid_abbreviations

def ingest_film(film_id):
    get_letterboxd_info(film_id)
    # get_film_metadata(film_id)
    # get_streaming_info(film_id)
    return None