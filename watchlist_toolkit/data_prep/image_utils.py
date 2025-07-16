import os
from tqdm import tqdm
import cloudscraper
import io
from PIL import Image
from datetime import datetime

from sqlite_utils import select_statement_to_df, insert_record_into_table
from letterboxd_utils import get_poster_url, desensitise_case, resensitise_case
from tmdb_utils import get_portrait_url
from watchlist_toolkit.utils.sql_loader import read_sql

posters_dir   = 'C:\\Users\\tom\\Desktop\\dev\\PersonalProjects\\letterboxd-app\\db\\posters\\'
portraits_dir = 'C:\\Users\\tom\\Desktop\\dev\\PersonalProjects\\letterboxd-app\\db\\portraits\\'

def download_image_from_url(url, save_path, verbose=False):
    try:
        # Send a GET request to the URL
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Open the image using PIL
        image = Image.open(io.BytesIO(response.content))
        image = image.resize((230, 345))
        # Save the image
        image.save(save_path)
        if verbose: print(f"Image successfully downloaded: {save_path}")
        return True
    except scraper.exceptions.RequestException as e:
        print(f"Error downloading image: {e}")
    except IOError as e:
        print(f"Error saving image: {e}")
    return False

def download_poster(film_id):
    poster_url = get_poster_url(film_id)
    if poster_url:
        save_dir = os.path.join(posters_dir, desensitise_case(film_id)+'.jpg')
        download_image_from_url(poster_url, save_dir)

def download_portrait(person_id):
    portrait_url = get_portrait_url(person_id)
    if portrait_url:
        save_dir = os.path.join(portraits_dir, str(person_id)+'.jpg')
        download_image_from_url(portrait_url, save_dir)
    else:
        portrait_missing_record = {
            'PERSON_ID': person_id,
            'CREATED_AT': datetime.now()
            }
        insert_record_into_table(portrait_missing_record, 'PORTRAIT_MISSING', logging=False, append=True)

def update_posters(total_to_download=100):

    posters = [x.replace('.jpg', '') for x in os.listdir(posters_dir)]

    all_films = select_statement_to_df(read_sql('update_posters_select_statement'))['FILM_ID']
    all_films_desensitised = [desensitise_case(x) for x in all_films]
    films_no_posters = [x for x in all_films_desensitised if x not in posters][:total_to_download]
    print('There are {} films to get posters for ordered by letterboxd watch count'.format(len(films_no_posters)))
    film_ids_no_posters = [resensitise_case(x) for x in films_no_posters]
    for film_id in tqdm(film_ids_no_posters):
        download_poster(film_id)

def update_portraits(total_to_download=100, verbose=False):
    portraits = [x.replace('.jpg', '') for x in os.listdir(portraits_dir)]
    portrait_select_statement = read_sql('portrait_select_statement')
    all_people = [str(int(x)) for x in select_statement_to_df(portrait_select_statement)['PERSON_ID'] if x]
    people_no_portrait = [x for x in all_people if x not in portraits]
    portrait_missing = [str(int(x)) for x in select_statement_to_df('SELECT PERSON_ID FROM PORTRAIT_MISSING')['PERSON_ID'] if x]
    people_no_portrait_not_missing = [x for x in people_no_portrait if x not in portrait_missing]
    print('There are {} people to get portraits for ordered by total watch count - getting {}'.format(len(people_no_portrait), total_to_download))
    for person_id in tqdm(people_no_portrait_not_missing[:total_to_download]):
        if verbose: print(person_id)
        download_portrait(person_id)

def update_images(total_to_download=100):
    update_posters(total_to_download=total_to_download)
    update_portraits(total_to_download=total_to_download)