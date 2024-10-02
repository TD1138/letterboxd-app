import os
from tqdm import tqdm
import requests
import io
from PIL import Image

from sqlite_utils import select_statement_to_df
from letterboxd_utils import get_poster_url, desensitise_case, resensitise_case
from tmdb_utils import get_portrait_url

posters_dir   = 'C:\\Users\\tom\\Desktop\\dev\\PersonalProjects\\letterboxd-app\\db\\posters\\'
portraits_dir = 'C:\\Users\\tom\\Desktop\\dev\\PersonalProjects\\letterboxd-app\\db\\portraits\\'

def download_image_from_url(url, save_path, verbose=False):
    try:
        # Send a GET request to the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Open the image using PIL
        image = Image.open(io.BytesIO(response.content))
        image = image.resize((230, 345))
        # Save the image
        image.save(save_path)
        if verbose: print(f"Image successfully downloaded: {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading image: {e}")
    except IOError as e:
        print(f"Error saving image: {e}")
    return False

def download_poster(film_id):
    poster_url = get_poster_url(film_id)
    save_dir = os.path.join(posters_dir, desensitise_case(film_id)+'.jpg')
    download_image_from_url(poster_url, save_dir)

def download_portrait(person_id):
    portrait_url = get_portrait_url(person_id)
    save_dir = os.path.join(portraits_dir, str(person_id)+'.jpg')
    download_image_from_url(portrait_url, save_dir)

def update_posters(total_to_download=100):

    posters = [x.replace('.jpg', '') for x in os.listdir(posters_dir)]

    all_films = select_statement_to_df('SELECT FILM_ID FROM FILM_LETTERBOXD_STATS ORDER BY FILM_WATCH_COUNT DESC')['FILM_ID']
    all_films_desensitised = [desensitise_case(x) for x in all_films]
    films_no_posters = [x for x in all_films_desensitised if x not in posters][:total_to_download]
    print('There are {} films to get posters for ordered by letterboxd watch count'.format(len(films_no_posters)))
    film_ids_no_posters = [resensitise_case(x) for x in films_no_posters]
    for film_id in tqdm(film_ids_no_posters):
        download_poster(film_id)

    all_films = select_statement_to_df('SELECT FILM_ID FROM FILM_ALGO_SCORE ORDER BY ALGO_SCORE DESC')['FILM_ID']
    all_films_desensitised = [desensitise_case(x) for x in all_films]
    films_no_posters = [x for x in all_films_desensitised if x not in posters][:total_to_download]
    print('There are {} films to get posters for ordered by algo score'.format(len(films_no_posters)))
    film_ids_no_posters = [resensitise_case(x) for x in films_no_posters]
    for film_id in tqdm(film_ids_no_posters):
        download_poster(film_id)

    all_films = select_statement_to_df('SELECT FILM_ID FROM PERSONAL_RATING ORDER BY FILM_RATING_SCALED DESC')['FILM_ID']
    all_films_desensitised = [desensitise_case(x) for x in all_films]
    films_no_posters = [x for x in all_films_desensitised if x not in posters][:total_to_download]
    print('There are {} films to get posters for ordered by my personal rating'.format(len(films_no_posters)))
    film_ids_no_posters = [resensitise_case(x) for x in films_no_posters]
    for film_id in tqdm(film_ids_no_posters):
        download_poster(film_id)

def update_portraits(total_to_download=100):

    portraits = [x.replace('.jpg', '') for x in os.listdir(portraits_dir)]
    all_people = select_statement_to_df('SELECT PERSON_ID FROM PERSON_INFO WHERE KNOWN_FOR_DEPARTMENT = "Directing" ORDER BY PERSON_POPULARITY DESC')['PERSON_ID']
    people_no_portrait = [x for x in all_people if x not in portraits][:total_to_download]
    print('There are {} people to get portraits for ordered by tmdb popularity'.format(len(people_no_portrait)))
    for person_id in tqdm(people_no_portrait):
        download_portrait(person_id)

def update_images(total_to_download=100):
    # update_posters(total_to_download=total_to_download)
    update_portraits(total_to_download=total_to_download)