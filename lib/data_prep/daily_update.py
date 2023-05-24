from selenium_utils import download_letterboxd_zip
from export_utils import unzip_letterboxd_downloads, set_latest_export, cleanup_exports_folder, refresh_core_tables
from enrichment_utils import ingest_new_films
from error_utils import correct_all_errors
from update_utils import update_oldest_records

download_letterboxd_zip(hide_actions=True)
unzip_letterboxd_downloads()
set_latest_export()
cleanup_exports_folder()
refresh_core_tables()
ingest_new_films()
correct_all_errors()
update_oldest_records()