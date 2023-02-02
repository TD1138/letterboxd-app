import time
from export_utils import find_latest_export, unzip_letterboxd_downloads
from selenium_utils import download_letterboxd_zip
from enrichment_utils import read_all_films

download_letterboxd_zip(hide_actions=False)
unzip_letterboxd_downloads()
latest_export_path = find_latest_export()
films_to_ingest = get_new_films(latest_export_path)
for film in films_to_ingest:
    print(film)
# upload_file_to_drive('1')