import numpy as np
import pandas as pd
import json
from datetime import datetime
from sqlite_utils import get_from_table, delete_records, df_to_table
from simplejustwatchapi.justwatch import search

# TEMP_OVERRIDE in case Justwatch API goes down:

def update_streaming_info(film_id, log_reason='UPDATE', verbose=False, TEMP_OVERRIDE=False):
    if TEMP_OVERRIDE:
        return None
    with open('my_streaming_services.json', 'r') as schema:
        my_streaming_services = json.load(schema)
    my_streaming_services_abbr = [x for x in set([x['technical_name'] for x in my_streaming_services]) if len(x) > 0]
    abbr_to_full_dict = {x['technical_name']:x['streaming_service'] for x in my_streaming_services if len(x['technical_name']) > 0}
    abbr_to_full_dict['rent'] = 'Rental'
    film_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    valid_tech_names = ['none']
    valid_full = ['none']
    min_rental_price = None
    valid = 0
    if film_title:
        imdb_id = get_from_table('IMDB_ID', film_id, 'IMDB_ID')
        results = search(film_title, 'GB', 'en', 5, True)
        offers = [x.offers for x in results if x.imdb_id == imdb_id]
        if offers:
            offers = offers[0]
        else:
            offers = None
    else:
        offers = None
    delete_records('FILM_STREAMING_SERVICES', film_id)
    if offers:
        provider_technical_names = list(set([x.technical_name for x in offers if (x.technical_name != 'itv' and x.monetization_type in ['FLATRATE', 'FREE', 'ADS']) or (x.technical_name == 'itv' and x.monetization_type == 'FREE')]))
        valid_tech_names = [x for x in provider_technical_names if x in my_streaming_services_abbr]
        min_rental_price = 0
        valid = 1
        if len(valid_tech_names) == 0:
            valid_tech_names = ['rent']
            rental_prices = [x.price_value for x in offers if x.monetization_type == 'RENT' and x.presentation_type == 'HD' and x.price_value]
            if len(rental_prices) > 0:
                min_rental_price = min(rental_prices)
            else:
                min_rental_price = None
                valid = 0
        valid_full = [abbr_to_full_dict.get(x) for x in valid_tech_names]
            
    streaming_record = {
        'FILM_ID': [film_id]*len(valid_tech_names),
        'STREAMING_SERVICE_ABBR': valid_tech_names,
        'STREAMING_SERVICE_FULL': valid_full,
        'CREATED_AT': [datetime.now()]*len(valid_tech_names),
        'PRICE': [min_rental_price]*len(valid_tech_names),
        'VALID': [valid]*len(valid_tech_names)
        }
    df_to_table(pd.DataFrame(streaming_record), 'FILM_STREAMING_SERVICES', replace_append='append', verbose=verbose)
    if verbose:
        try:
            print(streaming_record)
        except:
            print('No streaming offers at this time')