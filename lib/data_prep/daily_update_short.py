from selenium_utils import download_letterboxd_zip
from export_utils import unzip_letterboxd_downloads, set_latest_export, cleanup_exports_folder, refresh_core_tables
from enrichment_utils import ingest_new_films
from algo_utils import run_algo
from precompute_tables import precompute_tables

import warnings
warnings.filterwarnings("ignore")

download_letterboxd_zip(hide_actions=True)
unzip_letterboxd_downloads()
set_latest_export()
refresh_core_tables()
ingest_new_films()
run_algo('linear_regression')
precompute_tables()
cleanup_exports_folder()