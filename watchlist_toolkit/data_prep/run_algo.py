from gcp_utils import download_db, upload_db
from algo_utils import run_algo
from precompute_tables import precompute_tables

import warnings
warnings.filterwarnings("ignore")

download_db()
run_algo(verbose=True)
precompute_tables()
upload_db()