import os
import pandas as pd
from datetime import datetime
from tmdbv3api import TMDb, Movie, TV, Person
from sqlite_utils import get_from_table, delete_records, insert_record_into_table, df_to_table, replace_record, update_record
from dotenv import load_dotenv

load_dotenv()

tmdb_api_key = os.getenv('TMDB_API_KEY')
tmdb = TMDb()
tmdb.api_key = tmdb_api_key

attrs = {
    'belongs_to_collection': None,
    'budget': None,
    'original_language': 'languages',
    'popularity': 'popularity',
    'release_date': 'first_air_date',
    'revenue': None,
    'runtime': ['episode_run_time', 'number_of_episodes'],
    'status': 'status',
    'vote_average': 'vote_average',
    'vote_count': 'vote_count',
    'keywords': 'keywords',
    'casts': 'credits'
}

required_crew = [
    'Director',
    'Director of Photography',
    'Editor',
    'Original Music Composer',
    'Screenplay',
    ''
]

person_attrs = {
    'id': 'PERSON_ID',
    'name': 'PERSON_NAME',
    'imdb_id': 'IMDB_PERSON_ID',
    'gender': 'GENDER',
    'birthday': 'DATE_OF_BIRTH',
    'deathday': 'DATE_OF_DEATH',
    'known_for_department': 'KNOWN_FOR_DEPARTMENT',
    'place_of_birth': 'PLACE_OF_BIRTH',
    'popularity': 'PERSON_POPULARITY'
}

def create_movie_metadata_dict(film_id):
    tmdb_id = get_from_table('TMDB_ID', film_id, 'TMDB_ID')
    content_type = get_from_table('CONTENT_TYPE', film_id, 'CONTENT_TYPE')
    movie_metadata_dict = {'FILM_ID': film_id}
    if content_type == 'movie':
        try:
            movie = Movie()
            details = movie.details(tmdb_id)
            # import ipdb; ipdb.set_trace()
            for k in attrs:
                movie_metadata_dict[k] = details.get(k, None)
            if len(movie_metadata_dict.get('keywords', {'keywords': []}).get('keywords', [])) == 0:
                movie_metadata_dict['keywords']['keywords'] = [{'id': -1, 'name': 'none'}]
        except:
            movie_metadata_dict = None
            update_record('TMDB_ID', 'VALID', 0, film_id)
    elif content_type == 'tv':
        try:
            tv = TV()
            details = tv.details(tmdb_id, 'credits,keywords')
            for k in attrs:
                if type(attrs[k]) == list:
                    movie_metadata_dict[k] = [details.get(x, None) for x in attrs[k]]
                else:
                    movie_metadata_dict[k] = details.get(attrs[k], None)
            movie_metadata_dict['original_language'] = movie_metadata_dict['original_language'][0]
            try:
                movie_metadata_dict['runtime'] = movie_metadata_dict['runtime'][0][0] * movie_metadata_dict['runtime'][1]
            except:
                movie_metadata_dict['runtime'] = None
            movie_metadata_dict['keywords']['keywords'] = movie_metadata_dict['keywords'].pop('results')
            movie_metadata_dict['status'] = movie_metadata_dict['status'].replace('Returning Series', 'Released').replace('Ended', 'Released')
            if len(movie_metadata_dict.get('keywords', {'keywords': []}).get('keywords', [])) == 0:
                movie_metadata_dict['keywords']['keywords'] = [{'id': -1, 'name': 'none'}]
        except:
            movie_metadata_dict = None
            update_record('TMDB_ID', 'VALID', 0, film_id)
    else:
        movie_metadata_dict = None
    return movie_metadata_dict

def update_financials(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    budget = movie_metadata_dict.get('budget', None)
    revenue = movie_metadata_dict.get('revenue', None)
    financials_record = {
        'FILM_ID': film_id,
        'FILM_BUDGET': budget,
        'FILM_REVENUE': revenue,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_FINANCIALS', financials_record, film_id)

def update_tmdb_stats(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    popularity = movie_metadata_dict.get('popularity', None)
    vote_count = movie_metadata_dict.get('vote_count', None)
    vote_average = movie_metadata_dict.get('vote_average', None)
    tmdb_stats_record = {
        'FILM_ID': film_id,
        'FILM_POPULARITY': popularity,
        'FILM_VOTE_COUNT': vote_count,
        'FILM_VOTE_AVERAGE': vote_average,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_TMDB_STATS', tmdb_stats_record, film_id)

def get_language(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    original_language = movie_metadata_dict.get('original_language', None)
    language_record = {
        'FILM_ID': film_id,
        'FILM_LANGUAGE': original_language,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_LANGUAGE', language_record, film_id)

def get_runtime(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    runtime = movie_metadata_dict.get('runtime', None)
    runtime_record = {
        'FILM_ID': film_id,
        'FILM_RUNTIME': runtime,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_RUNTIME', runtime_record, film_id)

def update_release_info(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    release_date = movie_metadata_dict.get('release_date', None)
    status = movie_metadata_dict.get('status', None)
    release_info_record = {
        'FILM_ID': film_id,
        'FILM_RELEASE_DATE': release_date,
        'FILM_STATUS': status,
        'CREATED_AT': datetime.now()
        }
    replace_record('FILM_RELEASE_INFO', release_info_record, film_id)

def update_keywords(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    keywords = movie_metadata_dict.get('keywords', {'keywords': [{'id': -1, 'name': 'none'}]})
    if keywords:
        keywords = keywords.get('keywords')
        keyword_df = pd.DataFrame({
            'FILM_ID': [film_id]*len(keywords),
            'KEYWORD_ID':[x.get('id') for x in keywords],
            'KEYWORD':[x.get('name') for x in keywords],
            'CREATED_AT': [datetime.now()]*len(keywords)
                                   })
        delete_records('FILM_KEYWORDS', film_id)
        df_to_table(keyword_df, 'FILM_KEYWORDS', replace_append='append', verbose=False)
    
def update_cast(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    cast = movie_metadata_dict.get('casts', {'cast': [{'id': -1, 'character': '', 'order':-1}]})
    if cast:
        cast = cast.get('cast')
        cast_df = pd.DataFrame({
            'FILM_ID': [film_id]*len(cast),
            'PERSON_ID':[x.get('id') for x in cast],
            'CHARACTER':[x.get('character') for x in cast],
            'CAST_ORDER':[x.get('order') for x in cast],
            'CREATED_AT': [datetime.now()]*len(cast)
            })
        delete_records('FILM_CAST', film_id)
        df_to_table(cast_df, 'FILM_CAST', replace_append='append', verbose=False)

def update_crew(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    crew = movie_metadata_dict.get('casts', {'crew': [{'id': -1, 'job': ''}]})
    if crew:
        crew = crew.get('crew')
        crew = [x for x in crew if x['job'] in required_crew]
        crew_df = pd.DataFrame({
            'FILM_ID': [film_id]*len(crew),
            'PERSON_ID':[x.get('id') for x in crew],
            'JOB':[x.get('job') for x in crew],
            'CREATED_AT': [datetime.now()]*len(crew)
            })
        delete_records('FILM_CREW', film_id)
        df_to_table(crew_df, 'FILM_CREW', replace_append='append', verbose=False)

def update_collections(movie_metadata_dict):
    film_id = movie_metadata_dict.get('FILM_ID')
    collection = movie_metadata_dict.get('belongs_to_collection')
    if collection:
        collection_record = {
            'FILM_ID': film_id,
            'COLLECTION_ID': collection.get('id'),
            'COLLECTION_NAME': collection.get('name'),
            'CREATED_AT':datetime.now()
            }
    else:
        collection_record = {
            'FILM_ID': film_id,
            'COLLECTION_ID': -1,
            'COLLECTION_NAME': '',
            'CREATED_AT':datetime.now()
            }
    replace_record('FILM_COLLECTIONS', collection_record, film_id)

def update_tmbd_metadata(film_id):
    movie_metadata_dict = create_movie_metadata_dict(film_id)
    # import ipdb; ipdb.set_trace()
    if movie_metadata_dict:
        update_financials(movie_metadata_dict)
        update_tmdb_stats(movie_metadata_dict)
        update_release_info(movie_metadata_dict)
        update_keywords(movie_metadata_dict)
        update_cast(movie_metadata_dict)
        update_crew(movie_metadata_dict)
        update_collections(movie_metadata_dict)
    return movie_metadata_dict
    
def get_tmbd_metadata(film_id):
    movie_metadata_dict = update_tmbd_metadata(film_id)
    if movie_metadata_dict:
        get_language(movie_metadata_dict)
        get_runtime(movie_metadata_dict)

def create_person_metadata_dict(person_id):
    person_metadata_dict = {'PERSON_ID': person_id}
    try:
        person = Person()
        details = person.details(person_id)
        for k in person_attrs:
            person_metadata_dict[person_attrs[k]] = details.get(k, None)
        person_metadata_dict['VALID'] = 1
    except:
        person_metadata_dict = None
        update_record('PERSON_INFO', 'VALID', 0, person_id, primary_key='PERSON_ID')
    return person_metadata_dict

def update_person_metadata(person_id):
    person_metadata_dict = create_person_metadata_dict(person_id)
    if person_metadata_dict:
        replace_record('PERSON_INFO', person_metadata_dict, person_id, primary_key='PERSON_ID')

def get_person_metadata(person_id):
    update_person_metadata(person_id)