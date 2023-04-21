import time
from tqdm import tqdm
from export_utils import set_latest_export, unzip_letterboxd_downloads, refresh_core_tables
from selenium_utils import download_letterboxd_zip
from enrichment_utils import get_ingested_films, get_new_films, ingest_film, get_all_films
from sqlite_utils import db_basic_setup, get_from_table, set_working_db

# download_letterboxd_zip()
# download_letterboxd_zip(hide_actions=False)
# unzip_letterboxd_downloads()
# set_latest_export()
# db_name = 'lb-film.db'
# set_working_db(db_name)
# db_basic_setup(db_name, overwrite=True)
# refresh_core_tables()
# films_to_ingest = get_new_films()
# films_to_ingest = get_ingested_films(error_type='LETTERBOXD_ERROR')
films_to_ingest = ['f_01ICY'] # OVERRIDE TO DEBUG SPECIFIC FILMS
films_to_ingest = get_all_films()
ingestion_limit = len(films_to_ingest)
#ingestion_limit = 10
print('In total, there are {} films left to ingest'.format(len(films_to_ingest)))
for film_id in tqdm(films_to_ingest[:ingestion_limit]):
    film_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    print('\n', film_id, film_title)
    ingest_film(film_id)