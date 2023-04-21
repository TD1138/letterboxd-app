import time
from tqdm import tqdm
from export_utils import set_latest_export, unzip_letterboxd_downloads, refresh_core_tables
from selenium_utils import download_letterboxd_zip
from enrichment_utils import ingest_new_films, update_existing_films

# download_letterboxd_zip(hide_actions=False)
# unzip_letterboxd_downloads()
# set_latest_export()
# refresh_core_tables()
ingest_new_films()
update_existing_films()