from tqdm import tqdm
from enrichment_utils import get_ingested_films, get_new_films, update_streaming_info
from sqlite_utils import get_from_table

films_to_ingest = get_ingested_films(error_count=0)
# films_to_ingest = ['f_01ZLI'] # OVERRIDE TO DEBUG SPECIFIC FILMS
ingestion_limit = len(films_to_ingest)
print('In total, there are {} films left to; update streaming for'.format(len(films_to_ingest)))
for film_id in tqdm(films_to_ingest[:ingestion_limit]):
    film_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    print('\n', film_id, film_title)
    update_streaming_info(film_id)