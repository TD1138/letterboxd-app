import os
import pandas as pd
from tmdbv3api import TMDb
from tmdbv3api import Movie
from sqlite_utils import get_from_table, delete_records, insert_record_into_table, df_to_table
from dotenv import load_dotenv

load_dotenv()

tmdb_api_key = os.getenv('TMDB_API_KEY')

tmdb = TMDb()
tmdb.api_key = tmdb_api_key

required_attrs = [
                 'belongs_to_collection',
                #  'budget',
                #  'imdb_id',
                #  'original_language',
                #  'popularity',
                #  'release_date',
                #  'revenue',
                #  'runtime',
                #  'status',
                #  'vote_average',
                #  'vote_count',
                #  'keywords',
                 'casts'
                 ]

def get_movie_metadata(film_id, attributes=required_attrs):
    tmdb_id = get_from_table('TMDB_ID', film_id, 'TMDB_ID')
    movie = Movie()
    m = movie.details(tmdb_id)
    movie_metadata_dict = {'FILM_ID', film_id}
    for k in attributes:
        movie_metadata_dict[k] = m[k]
    return movie_metadata_dict

def update_financials(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    budget = movie_metadata_dict.get('budget', 0)
    revenue = movie_metadata_dict.get('revenue', 0)
    financials_record = {
        'FILM_ID': film_id,
        'FILM_BUDGET': budget,
        'FILM_REVENUE': revenue
    }
    delete_records('FILM_FINANCIALS', film_id)
    insert_record_into_table(financials_record, 'FILM_FINANCIALS')

def update_tmdb_stats(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    popularity = movie_metadata_dict.get('popularity', 0)
    vote_count = movie_metadata_dict.get('vote_count', 0)
    vote_average = movie_metadata_dict.get('vote_average', 0)
    tmdb_stats_record = {
        'FILM_ID': film_id,
        'FILM_POPULARITY': popularity,
        'FILM_VOTE_COUNT': vote_count,
        'FILM_VOTE_AVERAGE': vote_average
    }
    delete_records('FILM_TMDB_STATS', film_id)
    insert_record_into_table(tmdb_stats_record, 'FILM_TMDB_STATS')

def get_language(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    original_language = movie_metadata_dict.get('original_language', 'none')
    language_record = {
        'FILM_ID': film_id,
        'FILM_LANGUAGE': original_language
    }
    delete_records('FILM_LANGUAGE', film_id)
    insert_record_into_table(language_record, 'FILM_LANGUAGE')

def get_imdb_id(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    imdb_id = movie_metadata_dict.get('imdb_id')
    imdb_record = {
        'FILM_ID': film_id,
        'IMDB_ID': imdb_id
    }
    delete_records('IMDB_ID', film_id)
    insert_record_into_table(imdb_record, 'IMDB_ID')

def get_runtime(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    runtime = movie_metadata_dict.get('runtime')
    runtime_record = {
        'FILM_ID': film_id,
        'FILM_RUNTIME': runtime
    }
    delete_records('FILM_RUNTIME', film_id)
    insert_record_into_table(runtime_record, 'FILM_RUNTIME')

def update_release_info(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    release_date = movie_metadata_dict.get('release_date')
    status = movie_metadata_dict.get('status')
    release_info_record = {
        'FILM_ID': film_id,
        'FILM_RELEASE_DATE': release_date,
        'FILM_STATUS': status
    }
    delete_records('FILM_RELEASE_INFO', film_id)
    insert_record_into_table(release_info_record, 'FILM_RELEASE_INFO')    

def update_keywords(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    keywords = movie_metadata_dict.get('keywords').get('keywords')
    keyword_df = pd.DataFrame({'FILM_ID': [film_id]*len(keywords), 'KEYWORD_ID':[x.get('id') for x in keywords], 'KEYWORD':[x.get('name') for x in keywords]})
    delete_records('FILM_KEYWORDS', film_id)
    df_to_table(keyword_df, 'FILM_KEYWORDS', replace_append='append', verbose=False)
    
def update_cast(movie_metadata_dict):
    film_id = movie_metadata_dict.get('film_id')
    cast = movie_metadata_dict.get('casts').get('cast')
    cast_df = pd.DataFrame({'FILM_ID': [film_id]*len(cast),
                            'PERSON_ID':[x.get('id') for x in cast],
                            'CHARACTER':[x.get('character') for x in cast],
                            'CAST_ORDER':[x.get('order') for x in cast]
                            })
    delete_records('FILM_CAST', film_id)
    df_to_table(cast_df, 'FILM_CAST', replace_append='append', verbose=False)