import streamlit as st
import numpy as np
import pandas as pd
from PIL import Image
import os
import sys
sys.path.insert(0, '../data_prep')
from sqlite_utils import select_statement_to_df
from letterboxd_utils import desensitise_case

st.set_page_config(layout="wide")

FILMS_PER_ROW = 8
ROWS = 10
FILMS_PER_PAGE = FILMS_PER_ROW * ROWS

poster_dir = '..\\..\\db\\posters\\'

all_film_titles_query = """

SELECT

     a.FILM_TITLE
    ,b.FILM_YEAR
    ,a.FILM_ID
    
FROM FILM_TITLE a

LEFT JOIN FILM_YEAR b
ON a.FILM_ID = b.FILM_ID

"""

all_features_query = """

WITH BASE_TABLE AS (
    
    SELECT

      a.FILM_ID
      ,c.FILM_TITLE
      ,d.FILM_RATING
      ,e.FILM_RATING_SCALED
      ,CASE WHEN e.FILM_RATING_SCALED IS NOT NULL THEN 1 ELSE 0 END AS RATED

    FROM ALL_FILMS a
    LEFT JOIN CONTENT_TYPE b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN FILM_TITLE c
    ON a.FILM_ID = c.FILM_ID
    LEFT JOIN FILM_LETTERBOXD_STATS d
    ON a.FILM_ID = d.FILM_ID
    LEFT JOIN PERSONAL_RATING e
    ON a.FILM_ID = e.FILM_ID

    WHERE b.CONTENT_TYPE = 'movie'
      
    )

, RECENTLY_WATCHED AS (

    SELECT FILM_ID, 1 AS RECENTLY_WATCHED
    FROM DIARY
    WHERE WATCH_DATE >= date('now', '-14 days')
    AND IS_NARRATIVE_FEATURE = 1

    )

, DIRECTOR_TABLE AS (

  SELECT FILM_ID, GROUP_CONCAT(DIRECTOR_NAME, ', ') AS DIRECTOR_NAME FROM ( 

    SELECT
      
      a.FILM_ID
      ,c.PERSON_NAME AS DIRECTOR_NAME

    FROM ALL_FEATURE_FILMS a
    LEFT JOIN FILM_CREW b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN PERSON_INFO c
    ON b.PERSON_ID = c.PERSON_ID

    WHERE b.job = 'Director'
    AND c.PERSON_NAME IS NOT NULL

    )

  GROUP BY FILM_ID

)
   
    SELECT

      a.FILM_ID
      ,b.FILM_TITLE
      ,g.FILM_RATING_SCALED
      ,i.FILM_POSITION
      ,c.FILM_WATCH_COUNT
      ,k.TOP_250_POSITION AS FILM_TOP_250
      ,c.FILM_RATING
      ,COALESCE(1.0*c.FILM_LIKES_COUNT/c.FILM_WATCH_COUNT, 0.0) AS LIKES_PER_WATCH
      ,COALESCE(1.0*c.FILM_FAN_COUNT/c.FILM_WATCH_COUNT, 0.0) AS FANS_PER_WATCH
      ,h.DIRECTOR_NAME
      ,d.FILM_RUNTIME
      ,f.FILM_YEAR
      ,f.FILM_DECADE
      ,e.FILM_GENRE
      ,e.ALL_FILM_GENRES
      ,l.ACTION AS GENRE_ACTION
      ,l.ADVENTURE AS GENRE_ADVENTURE
      ,l.ANIMATION AS GENRE_ANIMATION
      ,l.COMEDY AS GENRE_COMEDY
      ,l.CRIME AS GENRE_CRIME
      ,l.DRAMA AS GENRE_DRAMA
      ,l.FAMILY AS GENRE_FAMILY
      ,l.FANTASY AS GENRE_FANTASY
      ,l.HISTORY AS GENRE_HISTORY
      ,l.HORROR AS GENRE_HORROR
      ,l.MUSIC AS GENRE_MUSIC
      ,l.MYSTERY AS GENRE_MYSTERY
      ,l.ROMANCE AS GENRE_ROMANCE
      ,l."SCIENCE-FICTION" AS GENRE_SCIENCE_FICTION
      ,l.THRILLER AS GENRE_THRILLER
      ,l.WAR AS GENRE_WAR
      ,l.WESTERN AS GENRE_WESTERN
      ,j.COLLECTION_NAME
      ,COALESCE(m.RECENTLY_WATCHED, 0) AS RECENTLY_WATCHED

    
    FROM ALL_FEATURE_FILMS a
    LEFT JOIN FILM_TITLE b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN FILM_LETTERBOXD_STATS c
    ON a.FILM_ID = c.FILM_ID
    LEFT JOIN FILM_RUNTIME d
    ON a.FILM_ID = d.FILM_ID
    LEFT JOIN FILM_GENRE e
    ON a.FILM_ID = e.FILM_ID
    LEFT JOIN FILM_YEAR f
    ON a.FILM_ID = f.FILM_ID
    LEFT JOIN PERSONAL_RATING g
    ON a.FILM_ID = g.FILM_ID
    LEFT JOIN DIRECTOR_TABLE h
    ON a.FILM_ID = h.FILM_ID
    LEFT JOIN PERSONAL_RANKING i
    ON a.FILM_ID = i.FILM_ID
    LEFT JOIN FILM_COLLECTIONS j
    ON a.FILM_ID = j.FILM_ID
    LEFT JOIN FILM_LETTERBOXD_TOP_250 k
    ON a.FILM_ID = k.FILM_ID
    LEFT JOIN FILM_ALGO_SCORE l
    ON a.FILM_ID = l.FILM_ID
    LEFT JOIN RECENTLY_WATCHED m
    ON a.FILM_ID = m.FILM_ID

"""

keyword_query = """

WITH BASE_TABLE AS (
    
    SELECT
    
        a.FILM_ID
        ,c.FILM_TITLE
        ,d.KEYWORD
        ,d.KEYWORD_ID
        ,e.FILM_RATING
        ,f.FILM_RATING_SCALED
        ,CASE WHEN f.FILM_RATING_SCALED IS NOT NULL THEN 1 ELSE 0 END AS RATED
    
    FROM ALL_FEATURE_FILMS a
    LEFT JOIN CONTENT_TYPE b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN FILM_TITLE c
    ON a.FILM_ID = c.FILM_ID
    LEFT JOIN FILM_KEYWORDS d
    ON a.FILM_ID = d.FILM_ID
    LEFT JOIN FILM_LETTERBOXD_STATS e
    ON a.FILM_ID = e.FILM_ID
    LEFT JOIN PERSONAL_RATING f
    ON a.FILM_ID = f.FILM_ID
    
    WHERE b.CONTENT_TYPE = 'movie'
    AND a.FILM_ID IS NOT NULL
    
    )
    
, SCORE_TABLE AS (

    SELECT

    KEYWORD_ID
    ,KEYWORD
    ,AVG(FILM_RATING) AS MEAN_RATING
    ,AVG(FILM_RATING_SCALED) AS MY_MEAN_RATING
    ,AVG(FILM_RATING_SCALED) - AVG(FILM_RATING) AS MY_VARIANCE
    ,((AVG(FILM_RATING_SCALED) - AVG(FILM_RATING)) * ((SUM(RATED)+0.0)/COUNT(*))) AS VARIANCE_SCORE
    ,COUNT(*) AS KEYWORD_COUNT
    ,SUM(RATED) AS MY_RATING_COUNT
    ,(SUM(RATED)+0.0)/COUNT(*) AS SCALER
    
    FROM BASE_TABLE
    
    GROUP BY KEYWORD
    
    HAVING KEYWORD_COUNT >= 30
    AND MY_RATING_COUNT >= 3
)

SELECT
    a.FILM_ID
    ,a.KEYWORD_ID
    ,b.KEYWORD
    
FROM FILM_KEYWORDS a
LEFT JOIN SCORE_TABLE b
ON a.KEYWORD_ID = b.KEYWORD_ID

WHERE a.FILM_ID IS NOT NULL 
AND b.KEYWORD_ID IS NOT NULL
AND b.KEYWORD_ID > -1

"""

top_actor_film_level_query = """

WITH BASE_TABLE AS (

    SELECT

        a.FILM_ID
        ,d.FILM_TITLE
        ,b.PERSON_ID
        ,e.PERSON_NAME AS ACTOR_NAME
        ,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED
        ,CASE WHEN f.FILM_RATING_SCALED IS NOT NULL THEN 1 ELSE 0 END AS RATED
        ,f.FILM_RATING_SCALED

    FROM ALL_FEATURE_FILMS a
    
    LEFT JOIN FILM_CAST b
    ON a.FILM_ID = b.FILM_ID
    
    LEFT JOIN WATCHED c
    ON a.FILM_ID = c.FILM_ID
    
    LEFT JOIN FILM_TITLE d
    ON a.FILM_ID = d.FILM_ID

    LEFT JOIN PERSON_INFO e
    ON b.PERSON_ID = e.PERSON_ID

    LEFT JOIN PERSONAL_RATING f
    ON a.FILM_ID = f.FILM_ID

    WHERE e.PERSON_NAME IS NOT NULL
    
    )
    
, ACTOR_TABLE AS (

    SELECT

    PERSON_ID
    ,SUM(WATCHED) AS TOTAL_WATCHED
    
    FROM BASE_TABLE
    
    GROUP BY PERSON_ID
    
    HAVING TOTAL_WATCHED >= 20
)

SELECT
    
     a.FILM_ID
    ,a.PERSON_ID
    ,a.ACTOR_NAME
    ,1 AS ACTOR_IN_FILM
    
FROM BASE_TABLE a
INNER JOIN ACTOR_TABLE b
ON a.PERSON_ID = b.PERSON_ID

"""

ranking_columns = [
    'DIRECTOR_NAME',
    'FILM_YEAR',
    'FILM_DECADE',
    'COLLECTION_NAME',
    'RECENTLY_WATCHED',
    'FILM_GENRE',
    'ALL_FILM_GENRES',
    'GENRE_ACTION',
    'GENRE_ADVENTURE',
    'GENRE_ANIMATION',
    'GENRE_COMEDY',
    'GENRE_CRIME',
    'GENRE_DRAMA',
    'GENRE_FAMILY',
    'GENRE_FANTASY',
    'GENRE_HISTORY',
    'GENRE_HORROR',
    'GENRE_MUSIC',
    'GENRE_MYSTERY',
    'GENRE_ROMANCE',
    'GENRE_SCIENCE_FICTION',
    'GENRE_THRILLER',
    'GENRE_WAR',
    'GENRE_WESTERN'
]

if 'dfs' not in st.session_state:
    st.session_state['dfs'] = {}
    all_film_titles = select_statement_to_df(all_film_titles_query)
    st.session_state['dfs']['all_film_titles'] = all_film_titles
    eligible_watchlist_df = select_statement_to_df(all_features_query)
    keyword_df = select_statement_to_df(keyword_query)
    keyword_df['COUNT'] = 1
    keyword_df_wide = pd.pivot_table(keyword_df, values='COUNT', index=['FILM_ID'], columns=['KEYWORD']).fillna(0).reset_index()
    eligible_watchlist_df = eligible_watchlist_df.merge(keyword_df_wide, how='left', on='FILM_ID')
    top_actor_film_level_df = select_statement_to_df(top_actor_film_level_query)
    actor_lookup_df = top_actor_film_level_df.groupby(['PERSON_ID', 'ACTOR_NAME']).count().reset_index()
    actor_lookup_dict = {id:name for id, name in zip(actor_lookup_df['PERSON_ID'], actor_lookup_df['ACTOR_NAME'])}
    top_actor_film_level_df_wide = pd.pivot_table(top_actor_film_level_df, values='ACTOR_IN_FILM', index=['FILM_ID'], columns='PERSON_ID').fillna(0)
    top_actor_film_level_df_wide.columns = [actor_lookup_dict.get(x, x) for x in top_actor_film_level_df_wide.columns]
    eligible_watchlist_df = eligible_watchlist_df.merge(top_actor_film_level_df_wide, how='left', on='FILM_ID').fillna(0)
    st.session_state['dfs']['eligible_watchlist_df'] = eligible_watchlist_df
else:
    eligible_watchlist_df = st.session_state['dfs']['eligible_watchlist_df']
    all_film_titles = st.session_state['dfs']['all_film_titles']

film_position_max = int(eligible_watchlist_df['FILM_POSITION'].max())

if 'highest_allowed_position' not in st.session_state:
    st.session_state['highest_allowed_position'] = 1

if 'lowest_allowed_position' not in st.session_state:
    st.session_state['lowest_allowed_position'] = film_position_max

if 'display_dash' not in st.session_state:
    st.session_state['display_dash'] = False

def reset_dash():
    st.session_state['display_dash'] = False
    st.session_state['highest_allowed_position'] = 1
    st.session_state['lowest_allowed_position'] = film_position_max

def get_poster_path(film_id):
    poster_path = os.path.join(poster_dir, desensitise_case(film_id) + '.jpg')
    if not os.path.exists(poster_path):
        poster_path = os.path.join(poster_dir, 'f_00000.jpg')
    return poster_path

def film_card(film):
    col1, col2, col3, col4 = st.columns([0.2, 0.1, 0.1, 0.7])
    with col1:
        # st.write("**{}**".format(int(film['FILM_POSITION'])))
        st.markdown("""
                    <style>
                    .big-font {
                        font-size:40px !important;
                        text-align: center;
                    }
                    </style>
                    """, unsafe_allow_html=True)
        st.markdown('<p class="big-font"> {} </p>'.format(int(film['FILM_POSITION'])), unsafe_allow_html=True)
    with col2:
        poster_path = get_poster_path(film['FILM_ID'])
        st.image(poster_path, width=75)
    with col4:
        card_text1 = f"**{film['FILM_TITLE']} ({film['FILM_YEAR']})**"
        st.write(card_text1)
        card_text2 = f"Watch Count: {film['FILM_WATCH_COUNT']:,}"
        st.write(card_text2)
        card_text3 = f"Letterboxd Rating: {film['FILM_RATING']}"
        st.write(card_text3)
        card_text4 = f"My Rating: {film['FILM_RATING_SCALED']:.4}"
        st.write(card_text4)

def display_film_grid(films_df):
    for _, film in films_df.iterrows():
        film_card(film)

film_name_search = st.text_input('Enter Film Name:', on_change=reset_dash)
film_name_search_list = film_name_search.split(' ')
film_name_search_list = [''.join(ch for ch in x if ch.isalnum()) for x in film_name_search_list]
valid_titles = [x[0] for x in all_film_titles[['FILM_TITLE']].values]
valid_titles = [x[0] + ' (' + str(x[1]) + ') - ' + x[2] for x in all_film_titles.values]
for word in film_name_search_list:
    valid_titles = [x for x in valid_titles if word in ''.join(ch for ch in x if ch.isalnum()).lower()]

if len(valid_titles) == 0:
    st.write('No Valid Titles - please try again')
elif len(valid_titles) <= 20:
    st.session_state['display_dash'] = True
elif len(film_name_search) > 0:
    if st.checkbox('More than 20 titles - show all?'):
        st.session_state['display_dash'] = True

if st.session_state['display_dash']:
    selected_film = st.selectbox('Select Film:', valid_titles, on_change=reset_dash)
    selected_film_id = selected_film.split(' - ')[-1]
    selected_record = eligible_watchlist_df[eligible_watchlist_df['FILM_ID']==selected_film_id]
    col1, col2 = st.columns([1,1])
    with col1:
        highest_allowed_position = st.number_input('Highest Position:', step=1, key='highest_allowed_position')
    with col2:
        lowest_allowed_position = st.number_input('Lowest Position:', step=1, key='lowest_allowed_position')
    valid_df = eligible_watchlist_df[eligible_watchlist_df['FILM_POSITION'].notnull()]
    valid_df = valid_df[(valid_df['FILM_POSITION'] < st.session_state['lowest_allowed_position']) & (valid_df['FILM_POSITION'] > st.session_state['highest_allowed_position'])]
    valid_df = valid_df.sort_values('FILM_POSITION')
    # st.write(valid_df)
    # st.write(selected_record)
    # st.write(selected_record.loc[:, (selected_record != 0).all()])
    col1, col2 = st.columns([0.5, 0.5])
    with col1:
        try:
            tag_columns = [x for x in selected_record.columns if selected_record[x].values[0] == 1.0]
            tag_columns = [x for x in tag_columns if x not in ranking_columns]
        except:
            tag_columns = []
        for rcol in ranking_columns + tag_columns:
            if rcol == 'RECENTLY_WATCHED':
                rcol_val = 1
            else:
                rcol_val = selected_record[rcol].values[0]
            if rcol_val:
                rcol_df = valid_df[valid_df[rcol]==rcol_val].sort_values('FILM_POSITION').reset_index(drop=True)
                container = st.expander('**{}={}**'.format(rcol, rcol_val), expanded=True)
                with container:
                    display_film_grid(rcol_df.tail(50))
        st.write('**All films:**')
        display_film_grid(valid_df.tail(50))
    with col2:
        selected_record['FILM_POSITION'] = int((st.session_state['lowest_allowed_position'] + st.session_state['highest_allowed_position'])/2)
        selected_record['FILM_RATING_SCALED'] = valid_df['FILM_RATING_SCALED'].mean()
        display_film_grid(selected_record)