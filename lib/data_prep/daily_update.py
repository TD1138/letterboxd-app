from selenium_utils import download_letterboxd_zip
from export_utils import unzip_letterboxd_downloads, set_latest_export, cleanup_exports_folder, refresh_core_tables
from enrichment_utils import ingest_new_films, ingest_new_people
from error_utils import correct_all_errors
from update_utils import update_oldest_records, update_most_popular_films, update_letterboxd_top_250, update_recent_films, update_upcoming_films
from algo_utils import run_algo
from letterboxd_utils import get_letterboxd_top_250
from precompute_tables import precompute_tables
from gcp_utils import download_db, upload_db, backup_db
import sys
from datetime import date

import warnings
warnings.filterwarnings("ignore")

download_db()
if len(sys.argv) > 1:
    if sys.argv[1] == 'nozip':
        print('Proceeding with daily update with no download of letterboxd zip file')
else:
    download_letterboxd_zip(hide_actions=False)
    unzip_letterboxd_downloads()
    set_latest_export()
    refresh_core_tables()
ingest_new_films()
# ingest_new_people()
correct_all_errors()
get_letterboxd_top_250()
update_oldest_records()
update_most_popular_films()
update_letterboxd_top_250()
update_recent_films()
update_upcoming_films()
run_algo()
precompute_tables()
cleanup_exports_folder()
upload_db()
if date.today().weekday() == 0:
    backup_db()