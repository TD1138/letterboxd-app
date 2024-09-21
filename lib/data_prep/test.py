import os
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
from letterboxd_utils import get_letterboxd_top_250, download_poster, desensitise_case, resensitise_case
from precompute_tables import precompute_tables
from gcp_utils import download_db, upload_db
import sys

import warnings
warnings.filterwarnings("ignore")

# all_films = select_statement_to_df('SELECT FILM_ID FROM FILM_LETTERBOXD_STATS ORDER BY FILM_WATCH_COUNT DESC')['FILM_ID']
all_films = select_statement_to_df('SELECT FILM_ID FROM FILM_ALGO_SCORE ORDER BY ALGO_SCORE DESC')['FILM_ID']
# all_films = select_statement_to_df('SELECT FILM_ID FROM PERSONAL_RATING ORDER BY FILM_RATING_SCALED DESC')['FILM_ID']
total_to_download = 100

posters_dir = 'C:\\Users\\tom\\Desktop\\dev\\PersonalProjects\\letterboxd-app\\db\\posters\\'
posters = [x.replace('.jpg', '') for x in os.listdir(posters_dir)]
all_films_desensitised = [desensitise_case(x) for x in all_films]
films_no_posters = [x for x in all_films_desensitised if x not in posters][:total_to_download]
print('There are {} films to get posters for'.format(len(films_no_posters)))
film_ids_no_posters = [resensitise_case(x) for x in films_no_posters]
for film_id in tqdm(film_ids_no_posters):
    download_poster(film_id)