import numpy as np
import pandas as pd
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from sqlite_utils import get_from_table, insert_record_into_table, delete_records, replace_record, update_record, update_ingestion_table, df_to_table, table_to_df
from tmdbv3api import Person

def get_metadata_from_letterboxd(film_id, verbose=False):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)
    soup = BeautifulSoup(r.content, 'lxml')
    og_url = soup.find('meta', {'property': 'og:url'}).get('content')
    film = og_url.split('/')[-2]
    film_url_record = {
        'FILM_ID': film_id,
        'FILM_URL_TITLE': film,
        'CREATED_AT': datetime.now()
    }
    replace_record('FILM_URL_TITLE', film_url_record, film_id)
    if verbose: print(film_url_record)
    try:
        year = soup.find('small', {'class': 'number'}).text
    except:
        year = int(datetime.now().strftime('%Y')) + 2
    film_year_record = {
        'FILM_ID': film_id,
        'FILM_YEAR':year,
        'FILM_DECADE': str(year)[:3]+'0s',
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_YEAR', film_year_record, film_id)
    if verbose: print(film_year_record)
    genre_list = [x.get('href').replace('/films/genre/', '').replace('/', '') for x in soup.findAll('a', {'class':'text-slug'}) if '/genre/' in str(x.get('href'))]
    if not genre_list:
        genre_list = ['none']
    genre_record = {
        'FILM_ID': film_id,
        'FILM_GENRE':genre_list[0],
        'ALL_FILM_GENRES': '/'.join(genre_list),
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_GENRE', genre_record, film_id)
    if verbose: print(genre_record)

def get_cast_from_letterboxd(film_id, verbose=False):
    letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
    r = requests.get(letterboxd_url)
    redirected_url = r.url
    if letterboxd_url != redirected_url:
        r = requests.get(redirected_url)    
    soup = BeautifulSoup(r.content, 'lxml')
    cast = soup.find('div', {'id': 'tab-cast'}).findAll('a')
    cast_record = {
        'FILM_ID': [film_id]*len(cast),
        'PERSON_ID': [Person().search(x.text)[0].get('id') for x in cast],
        'CHARACTER': [x.get('title') for x in cast],
        'CAST_ORDER': range(len(cast)),
        'CREATED_AT': [datetime.now()]*len(cast)
        }
    delete_records('FILM_CAST', film_id)
    df_to_table(pd.DataFrame(cast_record), 'FILM_CAST', replace_append='append', verbose=False)
    if verbose: print(cast_record)

# def get_crew_from_letterboxd(film_id, verbose=False):
#     letterboxd_url = get_from_table('FILM_TITLE', film_id, 'LETTERBOXD_URL')
#     r = requests.get(letterboxd_url)
#     redirected_url = r.url
#     if letterboxd_url != redirected_url:
#         r = requests.get(redirected_url)    
#     soup = BeautifulSoup(r.content, 'lxml')
#     crew = soup.find('div', {'id': 'tab-cast'}).findAll('a')
#     cast_record = {
#         'FILM_ID': [film_id]*len(cast),
#         'PERSON_ID': [Person().search(x.text)[0].get('id') for x in cast],
#         'CHARACTER': [x.get('title') for x in cast],
#         'CAST_ORDER': range(len(cast)),
#         'CREATED_AT': [datetime.now()]*len(cast)
#         }
#     delete_records('FILM_CAST', film_id)
#     df_to_table(pd.DataFrame(cast_record), 'FILM_CAST', replace_append='append', verbose=False)
#     if verbose: print(cast_record)
    
def get_letterboxd_top_250():
    top_250_url_titles = []
    for p in [1, 2, 3]:
        r = requests.get('https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/page/{}/'.format(p))
        soup = BeautifulSoup(r.content, 'lxml')
        top_250_url_titles += [x.find('div').get('data-film-slug') for x in soup.findAll('li', {'class': 'poster-container numbered-list-item'})]
    top_250_url_titles_df = pd.DataFrame(top_250_url_titles, columns=['FILM_URL_TITLE'])
    top_250_url_titles_df['TOP_250_POSITION'] = range(1, 251, 1)
    film_url_title_df = table_to_df('FILM_URL_TITLE')
    top_250_url_titles_df_plus_filmid = top_250_url_titles_df.merge(film_url_title_df[['FILM_URL_TITLE', 'FILM_ID']], how='left', on='FILM_URL_TITLE')
    df_to_table(top_250_url_titles_df_plus_filmid[['FILM_ID', 'TOP_250_POSITION']], 'FILM_LETTERBOXD_TOP_250', replace_append='replace', verbose=False)

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
    initial_url = 'https://letterboxd.com/film/{}/members/rated/.5-5/'.format(film_url_title)
    r = requests.get(initial_url)
    redirected_url = r.url
    if initial_url != redirected_url:
        new_film_url_title = redirected_url.replace('https://letterboxd.com/film/', '').replace('/members/rated/.5-5/', '')
        update_record(table_name='FILM_URL_TITLE',
                      column_name='FILM_URL_TITLE',
                      column_value=new_film_url_title,
                      film_id=film_id)
        film_url_title = new_film_url_title
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

def update_letterboxd_stats(film_id, verbose=False):
    rating_mean, rating_count = get_letterboxd_rating(film_id)
    metrics_dict = get_letterboxd_metrics(film_id)
    letterboxd_stats_record = {
        'FILM_ID': film_id,
        'FILM_WATCH_COUNT': metrics_dict['members'],
        'FILM_FAN_COUNT': metrics_dict['fans'],
        'FILM_LIKES_COUNT': metrics_dict['likes'],
        'FILM_REVIEW_COUNT': metrics_dict['reviews'],
        'FILM_LIST_COUNT': metrics_dict['lists'],
        'FILM_RATING': rating_mean,
        'FILM_RATING_COUNT': rating_count,
        'CREATED_AT': datetime.now()
    }
    insert_record_into_table(letterboxd_stats_record, 'FILM_LETTERBOXD_STATS')
    if verbose: print(letterboxd_stats_record)

def get_ext_ids_plus_content_type(film_id, verbose=False):
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
    if verbose: print(imdb_record)

    tmdb_record = {
        'FILM_ID': film_id,
        'TMDB_ID':tmdb_id,
        'CREATED_AT':datetime.now(),
        'VALID': tmdb_valid
    }
    replace_record('TMDB_ID', tmdb_record, film_id)
    if verbose: print(tmdb_record)

    content_record = {
        'FILM_ID': film_id,
        'CONTENT_TYPE':content_type,
        'CREATED_AT':datetime.now()
    }
    replace_record('CONTENT_TYPE', content_record, film_id)
    if verbose: print(content_record)

def update_all_letterboxd_info(film_id, verbose=False):
    try:
        get_ext_ids_plus_content_type(film_id, verbose=verbose)
    except Exception as e:
        print('failed to get the TMDB ID for {} ({})'.format(film_id, e))
    try:
        get_metadata_from_letterboxd(film_id, verbose=verbose)
    except Exception as e:
        print('failed to get letterboxd metadata for {} ({})'.format(film_id, e))
    try:
        update_letterboxd_stats(film_id, verbose=verbose)
    except Exception as e:
        print('failed to get letterboxd stats for {} ({})'.format(film_id, e))
    update_ingestion_table(film_id)