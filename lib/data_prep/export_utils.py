import numpy as np
import pandas as pd
import os
import shutil
from datetime import datetime
import time
from sqlite_utils import df_to_table
import dotenv

dotenv.load_dotenv(override=True)

def convert_uri_to_id(letterboxd_uri):
    return 'f_'+letterboxd_uri.replace('https://boxd.it/', '').zfill(5)

def unzip_letterboxd_downloads():
    raw_exports_folder = '/db/raw_exports/'
    for file in os.listdir(raw_exports_folder):
        if file[-4:] == '.zip':
            file_path = os.path.join(raw_exports_folder, file)
            shutil.unpack_archive(file_path, file_path[:-4])
            time.sleep(5)
            os.remove(file_path)

def get_all_export_folders():
    letterboxd_exports_folder_dir = 'db/raw_exports'
    letterboxd_export_folders = os.listdir(letterboxd_exports_folder_dir)
    return letterboxd_export_folders

def get_all_export_dates():
    letterboxd_export_folders = get_all_export_folders()
    letterboxd_exports_datetimes = [x.replace('letterboxd-{}-'.format(os.getenv('LETTERBOXD_USER')), '') for x in letterboxd_export_folders]
    return letterboxd_exports_datetimes

def get_latest_export_date():
    letterboxd_exports_datetimes = get_all_export_dates()
    latest_export_date = sorted(letterboxd_exports_datetimes)[-1]
    return latest_export_date

def set_latest_export(verbose=False):
    latest_export_filename = 'letterboxd-' + os.getenv('LETTERBOXD_USER') + '-' + get_latest_export_date()
    latest_export_file_loc = './db/raw_exports/' + latest_export_filename
    dotenv.set_key(dotenv.find_dotenv(), 'LATEST_EXPORT', latest_export_file_loc)
    if verbose: print('Latest export set to {}'.format(latest_export_file_loc))

def cleanup_exports_folder(folders_to_keep=5):
    letterboxd_export_folders = get_all_export_folders()
    if len(letterboxd_export_folders) == 5:
        raw_ind_to_keep = [x/(folders_to_keep-1) for x in range(folders_to_keep)]
        ind_to_keep = [int((len(letterboxd_export_folders)-1) * x) for x in raw_ind_to_keep]
        folders_to_keep = [sorted(letterboxd_export_folders)[x] for x in ind_to_keep]
        folders_to_delete = [x for x in letterboxd_export_folders if x not in folders_to_keep]
        for folder in folders_to_delete:
            shutil.rmtree('/db/raw_exports/'+folder)

def exportfile_to_df(export_filename, skiprows=None):
    export_df = pd.read_csv(os.path.join(os.getenv('LATEST_EXPORT'), export_filename), skiprows=skiprows)
    export_df.columns = [x.upper().replace(' ', '_') for x in export_df.columns]
    return export_df

def create_ratings_dict(feature_diary_df):
    ratings_dict = {}
    for rating in sorted(feature_diary_df['FILM_RATING'].unique(), reverse=True):
        if not np.isnan(rating):
            tmp_df = feature_diary_df[feature_diary_df['FILM_RATING']==rating]
            rating_count = len(tmp_df['FILM_NAME'].unique())
            increment = 0.5/rating_count
            max_rating = rating + 0.25
            if rating == 5:
                increment = 0.25/rating_count
                max_rating = 5
            elif rating == 0.5:
                increment = 0.75/rating_count
            highest_position = tmp_df['FILM_POSITION'].min()
            lowest_position = tmp_df['FILM_POSITION'].max()
            ratings_dict[rating] = {'RATING_COUNT':rating_count, 'MAX_RATING':max_rating, 'INCREMENT':increment, 'MAX_POSITION':highest_position, 'MIN_POSITION':lowest_position}
    return ratings_dict

def scale_rating(basic_rating, position, ratings_dict):
    if np.isnan(basic_rating):
        return np.nan
    else:
        dict_entry = ratings_dict[basic_rating]
        relative_position = position - dict_entry['MAX_POSITION']
        required_increment = relative_position*dict_entry['INCREMENT']
        final_rating = dict_entry['MAX_RATING'] - required_increment
        return final_rating

def refresh_core_tables(verbose=False):
    dotenv.load_dotenv(override=True)
    watched_df = exportfile_to_df('watched.csv')
    watched_df['FILM_ID'] = watched_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    df_to_table(watched_df[['FILM_ID']], 'WATCHED', replace_append='replace', verbose=verbose)

    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_df['FILM_ID'] = watchlist_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    watchlist_df.columns = ['ADDED_DATE', 'NAME', 'YEAR', 'LETTERBOXD_URI', 'FILM_ID']
    df_to_table(watchlist_df[['FILM_ID', 'ADDED_DATE']], 'WATCHLIST', replace_append='replace', verbose=verbose)

    all_films_df = pd.concat([watched_df, watchlist_df])
    all_films_df = all_films_df.drop_duplicates(subset='FILM_ID')
    title_df = all_films_df[['FILM_ID', 'NAME', 'LETTERBOXD_URI']]
    title_df.columns = ['FILM_ID', 'FILM_TITLE', 'LETTERBOXD_URL']
    df_to_table(title_df, 'FILM_TITLE', replace_append='replace', verbose=verbose)

    year_df = all_films_df[['FILM_ID', 'YEAR']]
    year_df.columns = ['FILM_ID', 'FILM_YEAR']
    year_df['FILM_YEAR'] = year_df['FILM_YEAR'].fillna(int(datetime.now().strftime('%Y')) + 2).astype(int)
    year_df['FILM_DECADE'] = year_df['FILM_YEAR'].apply(lambda x: str(x)[:3]+'0s')
    df_to_table(year_df, 'FILM_YEAR', replace_append='replace', verbose=verbose)

    ranking_list = exportfile_to_df('lists/every-film-ranked.csv', skiprows=3)
    ranking_list['FILM_ID'] = ranking_list['URL'].apply(convert_uri_to_id)
    ranking_list.columns = ['FILM_POSITION', 'FILM_NAME', 'FILM_YEAR', 'LETTERBOXD_URI', 'DESCRIPTION', 'FILM_ID']
    ranking_list['FILM_POSITION'] = ranking_list['FILM_POSITION'].astype('Int64')
    df_to_table(ranking_list[['FILM_ID', 'FILM_POSITION']], 'PERSONAL_RANKING', replace_append='replace', verbose=verbose)

    diary_df = exportfile_to_df('diary.csv')
    diary_df.columns = ['DIARY_DATE', 'FILM_NAME', 'FILM_YEAR', 'DIARY_URI', 'FILM_RATING', 'REWATCH', 'TAGS', 'WATCH_DATE']
    diary_df = diary_df.merge(watched_df, how='left', left_on=['FILM_NAME', 'FILM_YEAR'], right_on=['NAME', 'YEAR'])
    diary_df['FIRST_TIME_WATCH'] = np.where(diary_df['REWATCH']=='Yes', 0, 1)
    diary_df['TAGS'] = diary_df['TAGS'].fillna('')
    diary_df = diary_df.merge(ranking_list[['FILM_ID', 'FILM_POSITION']], how='left', on='FILM_ID')
    diary_df['IS_NARRATIVE_FEATURE'] = np.where(diary_df['FILM_POSITION'].isnull(), 0, 1)
    diary_df.insert(2, 'WATCH_ORDER_DAY', diary_df.groupby(['WATCH_DATE']).cumcount() + 1)
    df_to_table(diary_df[['FILM_ID', 'WATCH_DATE', 'WATCH_ORDER_DAY', 'FILM_RATING', 'TAGS', 'FIRST_TIME_WATCH', 'IS_NARRATIVE_FEATURE']], 'DIARY', replace_append='replace', verbose=verbose)

    feature_diary_df = diary_df[diary_df['IS_NARRATIVE_FEATURE']==1].reset_index(drop=True)
    ratings_dict = create_ratings_dict(feature_diary_df)
    rating_scaling_df = pd.DataFrame(ratings_dict).T.reset_index()
    rating_scaling_df.columns = ['FILM_RATING_BASIC', 'RATING_COUNT', 'MAX_RATING', 'INCREMENT', 'MAX_POSITION', 'MIN_POSITION']
    rating_scaling_df.insert(0, 'FILM_RATING_STR', rating_scaling_df['FILM_RATING_BASIC'].astype(str) + ' stars')
    df_to_table(rating_scaling_df, 'RATING_SCALING_DETAILS', replace_append='replace', verbose=verbose)

    feature_diary_df['RATING_ADJUSTED'] = feature_diary_df.apply(lambda row: scale_rating(row['FILM_RATING'], row['FILM_POSITION'], ratings_dict), axis=1)
    feature_diary_df['RATING_PERCENT'] = feature_diary_df['RATING_ADJUSTED'] / 5.0
    film_ratings_df = feature_diary_df.groupby('FILM_ID').agg({'FILM_RATING':'mean', 'RATING_ADJUSTED':'mean', 'RATING_PERCENT':'mean'}).reset_index()
    film_ratings_df.columns = ['FILM_ID', 'FILM_RATING_BASIC', 'FILM_RATING_SCALED', 'FILM_RATING_PERCENT']
    df_to_table(film_ratings_df, 'PERSONAL_RATING', replace_append='replace', verbose=verbose)




