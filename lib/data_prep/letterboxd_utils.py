import numpy as np
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from sqlite_utils import get_from_table, insert_record_into_table, update_record, replace_record, update_ingestion_table

def get_metadata_from_letterboxd(film_id):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    film_url_dict = {
        'FILM_ID': film_id,
        'FILM_URL_TITLE': film,
        'CREATED_AT': datetime.now()
    }
    replace_record('FILM_URL_TITLE', film_url_dict, film_id)
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
    genre_list = [x.get('href').replace('/films/genre/', '').replace('/', '') for x in soup.findAll('a', {'class':'text-slug'}) if '/genre/' in str(x.get('href'))]
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
    film_url_title = get_from_table('FILM_URL_TITLE', film_id, 'FILM_URL_TITLE')
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
    film_url_title = get_from_table('FILM_URL_TITLE', film_id, 'FILM_URL_TITLE')
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