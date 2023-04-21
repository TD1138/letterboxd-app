from tqdm import tqdm
from enrichment_utils import get_all_films, get_tmdb_id, get_film_ids_from_select_statement, update_all_letterboxd_info
from sqlite_utils import get_from_table

# films_to_ingest = get_all_films()
# films_to_ingest = ['f_0tSXS'] # OVERRIDE TO DEBUG SPECIFIC FILMS
films_to_ingest = get_film_ids_from_select_statement("""
    SELECT * FROM INGESTED WHERE LETTERBOXD_ERROR == 1
""")
ingestion_limit = len(films_to_ingest)
print('In total, there are {} films left to; update letterboxd info for'.format(len(films_to_ingest)))
for film_id in tqdm(films_to_ingest[:ingestion_limit]):
    film_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    print('\n', film_id, film_title)
    update_all_letterboxd_info(film_id)