from selenium_utils import download_letterboxd_zip
from export_utils import unzip_letterboxd_downloads, set_latest_export, cleanup_exports_folder, refresh_core_tables
from enrichment_utils import ingest_new_films
from algo_utils import run_algo
from precompute_tables import precompute_tables
from gcp_utils import download_db, upload_db

import warnings
warnings.filterwarnings("ignore")

download_db()
download_letterboxd_zip(hide_actions=False)
unzip_letterboxd_downloads()
set_latest_export()
refresh_core_tables()
ingest_new_films()
run_algo('decision_tree')
precompute_tables()
cleanup_exports_folder()
upload_db()