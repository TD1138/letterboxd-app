import time
from export_utils import set_latest_export, unzip_letterboxd_downloads, refresh_core_tables
from selenium_utils import download_letterboxd_zip
from enrichment_utils import get_new_films, get_letterboxd_info, get_film_metadata, get_streaming_info
from sqlite_utils import db_basic_setup, get_from_table, set_working_db, insert_record_into_table

# download_letterboxd_zip()
# unzip_letterboxd_downloads()
# set_latest_export()
db_name = 'lb-film-testing.db'
set_working_db(db_name)
db_basic_setup(db_name, overwrite=True, verbose=False)
refresh_core_tables()
new_films = get_new_films()
ingestion_limit = 1#00
for film_id in new_films[:ingestion_limit]:
    print(film_id)
    get_letterboxd_info(film_id)
    get_film_metadata(film_id)
    # get_streaming_info(film_id)
    # import ipdb; ipdb.set_trace()

# upload_file_to_drive('1')