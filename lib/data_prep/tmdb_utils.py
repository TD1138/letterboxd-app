import os
import pandas as pd
from datetime import datetime
from tmdbv3api import TMDb, Movie, TV, Person
from sqlite_utils import get_from_table, delete_records, insert_record_into_table, df_to_table, replace_record, update_record
from letterboxd_utils import get_cast_from_letterboxd
from dotenv import load_dotenv

load_dotenv(override=True)

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
            for k in attrs:
                movie_metadata_dict[k] = details.get(k, None)
            if len(movie_metadata_dict.get('keywords', {'keywords': []}).get('keywords', [])) == 0:
                movie_metadata_dict['keywords']['keywords'] = [{'id': -1, 'name': 'none'}]
        except:
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
            update_record('TMDB_ID', 'VALID', 0, film_id)
    return movie_metadata_dict

def update_financials(movie_metadata_dict, verbose=False):
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
    if verbose: print(financials_record)

def update_tmdb_stats(movie_metadata_dict, verbose=False):
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
    if verbose: print(tmdb_stats_record)

def get_language(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    original_language = movie_metadata_dict.get('original_language', None)
    language_record = {
        'FILM_ID': film_id,
        'FILM_LANGUAGE': original_language,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_LANGUAGE', language_record, film_id)
    if verbose: print(language_record)

def get_runtime(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    runtime = movie_metadata_dict.get('runtime', None)
    runtime_record = {
        'FILM_ID': film_id,
        'FILM_RUNTIME': runtime,
        'CREATED_AT':datetime.now()
    }
    replace_record('FILM_RUNTIME', runtime_record, film_id)
    if verbose: print(runtime_record)

def update_release_info(movie_metadata_dict, verbose=False):
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
    if verbose: print(release_info_record)

def update_keywords(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    keywords = movie_metadata_dict.get('keywords', {'keywords': [{'id': -1, 'name': 'none'}]})
    keywords = keywords.get('keywords', [{'id': -1, 'name': 'none'}])
    if len(keywords) == 0:
        keywords = [{'id': -1, 'name': 'none'}]
    keyword_record = {
        'FILM_ID': [film_id]*len(keywords),
        'KEYWORD_ID':[x.get('id') for x in keywords],
        'KEYWORD':[x.get('name') for x in keywords],
        'CREATED_AT': [datetime.now()]*len(keywords)
        }
    delete_records('FILM_KEYWORDS', film_id)
    df_to_table(pd.DataFrame(keyword_record), 'FILM_KEYWORDS', replace_append='append', verbose=False)
    if verbose: print(keyword_record)
    
def update_cast(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    cast = movie_metadata_dict.get('casts', {'cast': [{'id': -1, 'character': '', 'order':-1}]})
    cast = cast.get('cast')
    if len(cast) == 0:
        cast = [{'id': -1, 'character': '', 'order':-1}]
    cast_record = {
        'FILM_ID': [film_id]*len(cast),
        'PERSON_ID':[x.get('id') for x in cast],
        'CHARACTER':[x.get('character') for x in cast],
        'CAST_ORDER':[x.get('order') for x in cast],
        'CREATED_AT': [datetime.now()]*len(cast)
        }
    delete_records('FILM_CAST', film_id)
    df_to_table(pd.DataFrame(cast_record), 'FILM_CAST', replace_append='append', verbose=False)
    if verbose: print(cast_record)

def update_crew(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    crew = movie_metadata_dict.get('casts', {'crew': [{'id': -1, 'job': ''}]})
    crew = crew.get('crew')
    crew = [x for x in crew if x['job'] in required_crew]
    if len(crew) == 0:
        crew = [{'id': -1, 'job': ''}]
    crew_record = {
        'FILM_ID': [film_id]*len(crew),
        'PERSON_ID':[x.get('id') for x in crew],
        'JOB':[x.get('job') for x in crew],
        'CREATED_AT': [datetime.now()]*len(crew)
        }
    delete_records('FILM_CREW', film_id)
    df_to_table(pd.DataFrame(crew_record), 'FILM_CREW', replace_append='append', verbose=False)
    if verbose: print(crew_record)

def update_collections(movie_metadata_dict, verbose=False):
    film_id = movie_metadata_dict.get('FILM_ID')
    collection = movie_metadata_dict.get('belongs_to_collection', {'id': -1, 'name': ''})
    if not collection or len(collection) == 0:
        collection = {'id': -1, 'name': ''}
    collection_record = {
        'FILM_ID': film_id,
        'COLLECTION_ID': collection.get('id'),
        'COLLECTION_NAME': collection.get('name'),
        'CREATED_AT':datetime.now()
        }
    replace_record('FILM_COLLECTIONS', collection_record, film_id)
    if verbose: print(collection_record)

def update_tmbd_metadata(film_id, verbose=False):
    movie_metadata_dict = create_movie_metadata_dict(film_id)
    if verbose: print(movie_metadata_dict)
    update_financials(movie_metadata_dict, verbose=verbose)
    update_tmdb_stats(movie_metadata_dict, verbose=verbose)
    update_release_info(movie_metadata_dict, verbose=verbose)
    update_keywords(movie_metadata_dict, verbose=verbose)
    update_cast(movie_metadata_dict, verbose=verbose)
    update_crew(movie_metadata_dict, verbose=verbose)
    update_collections(movie_metadata_dict, verbose=verbose)
    return movie_metadata_dict
    
def get_tmbd_metadata(film_id, verbose=False):
    movie_metadata_dict = update_tmbd_metadata(film_id, verbose=verbose)
    get_language(movie_metadata_dict, verbose=verbose)
    get_runtime(movie_metadata_dict, verbose=verbose)

def create_person_metadata_dict(person_id):
    person_metadata_dict = {'PERSON_ID': person_id}
    try:
        person = Person()
        details = person.details(person_id)
        for k in person_attrs:
            person_metadata_dict[person_attrs[k]] = details.get(k, None)
        person_metadata_dict['VALID'] = 1
    except:
        person_metadata_dict['VALID'] = 0
    return person_metadata_dict

def update_person_info(person_metadata_dict, verbose=False):
    person_id = person_metadata_dict.get('PERSON_ID')
    person_record = {
        'PERSON_ID': person_id,
        'PERSON_NAME': person_metadata_dict.get('PERSON_NAME', None),
        'IMDB_PERSON_ID': person_metadata_dict.get('IMDB_PERSON_ID', None),
        'GENDER': person_metadata_dict.get('GENDER', None),
        'DATE_OF_BIRTH': person_metadata_dict.get('DATE_OF_BIRTH', None),
        'DATE_OF_DEATH': person_metadata_dict.get('DATE_OF_DEATH', None),
        'KNOWN_FOR_DEPARTMENT': person_metadata_dict.get('KNOWN_FOR_DEPARTMENT', None),
        'PLACE_OF_BIRTH': person_metadata_dict.get('PLACE_OF_BIRTH', None),
        'PERSON_POPULARITY': person_metadata_dict.get('PERSON_POPULARITY', None),
        'VALID': person_metadata_dict.get('VALID', 0),
        'CREATED_AT': datetime.now()
    }
    replace_record('PERSON_INFO', person_record, person_id, primary_key='PERSON_ID')
    if verbose: print(person_record)

def update_person_metadata(person_id, verbose=False):
    person_metadata_dict = create_person_metadata_dict(person_id)
    update_person_info(person_metadata_dict, verbose=verbose)

def get_person_metadata(person_id):
    update_person_metadata(person_id)