import time
from tqdm import tqdm
from export_utils import set_latest_export, unzip_letterboxd_downloads, refresh_core_tables
from selenium_utils import download_letterboxd_zip
from enrichment_utils import get_new_films, update_letterboxd_info, update_film_metadata, update_streaming_info
from sqlite_utils import db_basic_setup, get_from_table, set_working_db, insert_record_into_table

# download_letterboxd_zip()
# download_letterboxd_zip(hide_actions=False)
# unzip_letterboxd_downloads()
# set_latest_export()
db_name = 'lb-film.db'
set_working_db(db_name)
db_basic_setup(db_name, overwrite=True)
refresh_core_tables()
new_films = get_new_films()
test_films = ['f_01ZLI', 'f_0jWEA', 'f_02uds'] # Jumanji (0), Knives Out (1, Netflix), Hunger Games (2, Netflix, Prime)
ingestion_limit = 1#00
# for film_id in tqdm(new_films[:ingestion_limit]):
for film_id in tqdm(test_films):
    film_title = get_from_table('FILM_TITLE', film_id, 'FILM_TITLE')
    print(film_id, film_title)
    update_letterboxd_info(film_id)
    # update_film_metadata(film_id)
    update_streaming_info(film_id)
    # import ipdb; ipdb.set_trace()

# upload_file_to_drive('1')