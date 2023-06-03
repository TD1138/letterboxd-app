from selenium_utils import download_letterboxd_zip
from export_utils import unzip_letterboxd_downloads, set_latest_export, cleanup_exports_folder, refresh_core_tables
from enrichment_utils import ingest_new_films, ingest_new_people
from error_utils import correct_all_errors
from update_utils import update_oldest_records
from algo_utils import run_algo
import sys

if len(sys.argv) > 1:
    if sys.argv[1] == 'nozip':
        print('Proceeding with daily update with no download of letterboxd zip file')
else:
    download_letterboxd_zip(hide_actions=True)
    unzip_letterboxd_downloads()
    set_latest_export()
    refresh_core_tables()
ingest_new_films()
ingest_new_people()
correct_all_errors()
update_oldest_records()
run_algo()
cleanup_exports_folder()