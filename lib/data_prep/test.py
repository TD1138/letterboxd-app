from tqdm import tqdm
from export_utils import refresh_core_tables, cleanup_exports_folder
from enrichment_utils import ingest_new_films, get_all_films, ingest_film, ingest_new_people
from sqlite_utils import get_from_table, get_film_ids_from_select_statement, select_statement_to_df
from tmdb_utils import update_tmdb_stats, update_tmbd_metadata, update_person_metadata
from error_utils import correct_tmdb_metadata_errors, correct_all_errors, correct_letterboxd_stats_errors
from update_utils import update_oldest_records, update_streaming_records, update_tmdb_metadata_records, update_recent_films, update_upcoming_films, update_letterboxd_stats, update_letterboxd_top_250
from algo_utils import run_algo
from selenium_utils import download_letterboxd_zip
from justwatch_utils import update_streaming_info
from letterboxd_utils import get_letterboxd_top_250
from precompute_tables import precompute_tables
from gcp_utils import download_db, upload_db
import sys

import warnings
warnings.filterwarnings("ignore")

run_algo('decision_tree')
precompute_tables()
cleanup_exports_folder()
upload_db()