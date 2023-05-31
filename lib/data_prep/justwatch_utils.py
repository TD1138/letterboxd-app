import numpy as np
import pandas as pd
import json
from datetime import datetime
from sqlite_utils import get_from_table, delete_records, df_to_table
from justwatch import JustWatch

def update_streaming_info(film_id):
    with open('my_streaming_services.json', 'r') as schema:
        my_streaming_services = json.load(schema)
    my_streaming_services_abbr = [x for x in set([x['provider_abbreviation'] for x in my_streaming_services]) if len(x) > 0]
    abbr_to_full_dict = {x['provider_abbreviation']:x['streaming_service'] for x in my_streaming_services if len(x['provider_abbreviation']) > 0}
    abbr_to_full_dict['rent'] = 'Rental'
    just_watch = JustWatch(country='GB')
    film_url_title = get_from_table('FILM_URL_TITLE', film_id, 'FILM_URL_TITLE')
    if film_url_title == '':
        film_url_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    film_release_year = get_from_table('FILM_YEAR', film_id, 'FILM_YEAR')
    results = just_watch.search_for_item(query=film_url_title, release_year_from=film_release_year-1, release_year_until=film_release_year+1, page_size=1)
    delete_records('FILM_STREAMING_SERVICES', film_id)
    if len(results['items']) > 0:
        first_result = results['items'][0]
        if first_result.get('title') == get_from_table('FILM_TITLE', film_id, 'FILM_TITLE'):
            provider_abbreviations = list(set([x['package_short_name'] for x in first_result.get('offers', []) if x['monetization_type'] in ['flatrate', 'free', 'ads']]))
            valid_abbr = [x for x in provider_abbreviations if x in my_streaming_services_abbr]
            min_rental_price = 0
            valid = 1
            if len(valid_abbr) == 0:
                valid_abbr = ['rent']
                rental_prices = [x['retail_price'] for x in first_result.get('offers', []) if x['monetization_type'] == 'rent' and x['presentation_type'] == 'hd' and x.get('retail_price', False)]
                if len(rental_prices) > 0:
                    min_rental_price = min(rental_prices)
                else:
                    min_rental_price = None
                    valid = 0
            valid_full = [abbr_to_full_dict.get(x) for x in valid_abbr]
            film_streaming_services_df = pd.DataFrame(index=range(len(valid_abbr)))
            film_streaming_services_df['FILM_ID'] = film_id
            film_streaming_services_df['STREAMING_SERVICE_ABBR'] = valid_abbr
            film_streaming_services_df['STREAMING_SERVICE_FULL'] = valid_full
            film_streaming_services_df['CREATED_AT'] = datetime.now()
            film_streaming_services_df['PRICE'] = min_rental_price
            film_streaming_services_df['VALID'] = valid
            df_to_table(film_streaming_services_df, 'FILM_STREAMING_SERVICES', replace_append='append', verbose=False)