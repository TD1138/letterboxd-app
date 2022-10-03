import time
from utils import find_latest_export, unzip_letterboxd_downloads
from selenium_utils import download_letterboxd_zip

download_letterboxd_zip(hide_actions=False)
unzip_letterboxd_downloads()
latest_export = find_latest_export()