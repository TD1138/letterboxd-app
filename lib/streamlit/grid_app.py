import streamlit as st
import numpy as np
import pandas as pd
from PIL import Image
import plotly.express as px
import os
import sys
sys.path.insert(0, '../data_prep')
from sqlite_utils import select_statement_to_df
from letterboxd_utils import desensitise_case

st.set_page_config(layout="wide")

FILMS_PER_ROW = 8
ROWS = 32
FILMS_PER_PAGE = FILMS_PER_ROW * ROWS

if 'dfs' not in st.session_state:

    st.session_state['dfs'] = {}
    
    watchlist_statement = 'SELECT * FROM precomputed_all_films'
    watchlist_df = select_statement_to_df(watchlist_statement)
    st.session_state['dfs']['watchlist'] = watchlist_df

    algo_features_df = select_statement_to_df("""
SELECT a.*, CASE WHEN b.FILM_ID IS NOT NULL THEN 1 ELSE 0 END AS WATCHED, c.LETTERBOXD_URL, d.FILM_POSITION
FROM FILM_ALGO_SCORE a
LEFT JOIN WATCHED b
ON a.FILM_ID = b.FILM_ID
LEFT JOIN FILM_TITLE c
ON a.FILM_ID = c.FILM_ID
LEFT JOIN PERSONAL_RANKING d
ON a.FILM_ID = d.FILM_ID
                                              """)
    non_features = ['FILM_TOP_250', 'FILM_RUNTIME', 'DIRECTOR_MEAN_RATING', 'I_VS_LB']
    all_features = algo_features_df.columns
    keep_features = [x for x in all_features if x not in non_features]
    algo_features_df['IN_LETTERBOXD_TOP_250'] = np.where(algo_features_df['FILM_TOP_250']<251, 1, 0)
    # algo_features_df = algo_features_df[keep_features]
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
    
    WHERE KEYWORD_ID > -1
    
    GROUP BY KEYWORD
    
    HAVING MY_RATING_COUNT > 0
    AND KEYWORD_COUNT >= 50

)

SELECT
    a.FILM_ID
    ,a.KEYWORD_ID
    ,b.KEYWORD
    ,b.KEYWORD_COUNT
    ,b.MY_RATING_COUNT
    ,b.MEAN_RATING
    ,b.MY_MEAN_RATING
    
FROM FILM_KEYWORDS a
LEFT JOIN SCORE_TABLE b
ON a.KEYWORD_ID = b.KEYWORD_ID

WHERE b.KEYWORD_ID IS NOT NULL
"""
    keyword_df = select_statement_to_df(keyword_query)
    keyword_df['COUNT'] = 1
    keyword_df_wide = pd.pivot_table(keyword_df, values='COUNT', index=['FILM_ID'], columns=['KEYWORD']).fillna(0).reset_index()
    keyword_valid_cols = ['FILM_ID'] + [x for x in keyword_df_wide if x not in algo_features_df.columns]
    algo_features_df = algo_features_df.merge(keyword_df_wide[keyword_valid_cols], how='left', on='FILM_ID', )
    algo_features_df[keyword_valid_cols] = algo_features_df[keyword_valid_cols].apply(lambda x: x.fillna(0))
    st.session_state['dfs']['ranked'] = algo_features_df

    raw_shap_df = select_statement_to_df('SELECT * FROM FILM_SHAP_VALUES')
    st.session_state['dfs']['model_features'] = [x for x in raw_shap_df.columns if x not in ['FILM_ID', 'BASE_VALUE', 'PREDICTION']]
    raw_shap_df.columns = [x if x == 'FILM_ID' else x+'_SHAP' for x in raw_shap_df.columns]
    shap_df = algo_features_df[['FILM_ID', 'FILM_TITLE'] + st.session_state['dfs']['model_features']].merge(raw_shap_df, how='left', on='FILM_ID')
    shap_df = shap_df.merge(watchlist_df[['FILM_ID', 'RATED']], how='left', on='FILM_ID')
    # shap_df2 = shap_df.drop(['FILM_ID', 'FILM_TITLE', 'ALGO_SCORE'], axis=1)
    # shap_df2.insert(0, 'FILM_ID', st.session_state['dfs']['watchlist']['FILM_ID'])
    # shap_df2.insert(1, 'FILM_TITLE', st.session_state['dfs']['watchlist']['FILM_TITLE'])
    # shap_df2 = shap_df2.sort_values('PREDICTION', ascending=False)
    st.session_state['dfs']['shap'] = shap_df

    director_statement = 'SELECT * FROM precomputed_director_completion'
    director_df = select_statement_to_df(director_statement)
    st.session_state['dfs']['director'] = director_df

    actor_statement = 'SELECT * FROM precomputed_actor_completion'
    actor_df = select_statement_to_df(actor_statement)
    st.session_state['dfs']['actor'] = actor_df

    year_statement = 'SELECT * FROM precomputed_year_completion'
    year_df = select_statement_to_df(year_statement)
    st.session_state['dfs']['year'] = year_df

    diary_statement = 'SELECT WATCH_DATE, SUM(IS_NARRATIVE_FEATURE) AS MOVIE_COUNT, AVG(CASE WHEN IS_NARRATIVE_FEATURE = 1 THEN FILM_RATING END) AS MOVIE_RATING FROM DIARY GROUP BY WATCH_DATE ORDER BY WATCH_DATE ASC'
    diary_df = select_statement_to_df(diary_statement)
    diary_df['WATCH_DATE'] = pd.to_datetime(diary_df['WATCH_DATE'])
    date_range = pd.date_range(start=diary_df['WATCH_DATE'].min(), end=diary_df['WATCH_DATE'].max())
    diary_df2 = diary_df.set_index('WATCH_DATE').reindex(date_range).fillna(0).rename_axis('WATCH_DATE').reset_index()
    diary_df2['MOVIE_COUNT_ROLLING_7']   = diary_df2['MOVIE_COUNT'].rolling(window=7).mean()
    diary_df2['MOVIE_COUNT_ROLLING_28']  = diary_df2['MOVIE_COUNT'].rolling(window=28).mean()
    diary_df2['MOVIE_RATING_ROLLING_7']  = diary_df2['MOVIE_RATING'].rolling(window=7).mean()
    diary_df2['MOVIE_RATING_ROLLING_28'] = diary_df2['MOVIE_RATING'].rolling(window=28).mean()
    diary_df = diary_df2
    st.session_state['dfs']['diary'] = diary_df

else:
    watchlist_df = st.session_state['dfs']['watchlist']
    director_df = st.session_state['dfs']['director']
    actor_df = st.session_state['dfs']['actor']
    year_df = st.session_state['dfs']['year']
    diary_df = st.session_state['dfs']['diary']
    
if 'selected_director' not in st.session_state:
    st.session_state.selected_director = None

if 'selected_actor' not in st.session_state:
        st.session_state.selected_actor = None

if 'director_sort_order_persistent' not in st.session_state:
    st.session_state['director_sort_order_persistent'] = 'Total Films'
if 'actor_sort_order' not in st.session_state:
    st.session_state['director_sort_order'] = st.session_state['director_sort_order_persistent']

if 'actor_sort_order_persistent' not in st.session_state:
    st.session_state['actor_sort_order_persistent'] = 'Total Films'
if 'actor_sort_order' not in st.session_state:
    st.session_state['actor_sort_order'] = st.session_state['actor_sort_order_persistent']

# def open_url(url):
#     open_script= """
#         <script type="text/javascript">
#             window.open('%s', '_blank').focus();
#         </script>
#     """ % (url)
#     html(open_script)

def get_streaming_icon(streaming_provider):
    icon_dir = '..\\..\\db\\icons\\'
    icon_path = os.path.join(icon_dir, streaming_provider + '.jpg')
    if not os.path.exists(icon_path):
        icon_path = os.path.join(icon_dir, 'Not Streaming.jpg')
    return icon_path

def get_poster_path(film_id):
    poster_dir = '..\\..\\db\\posters\\'
    poster_path = os.path.join(poster_dir, desensitise_case(film_id) + '.jpg')
    if not os.path.exists(poster_path):
        poster_path = os.path.join(poster_dir, 'f_00000.jpg')
    return poster_path

def get_portrait_path(person_id):
    portrait_dir = '..\\..\\db\\portraits\\'
    portrait_path = os.path.join(portrait_dir, str(person_id) + '.jpg')
    if not os.path.exists(portrait_path):
        portrait_path = os.path.join(portrait_dir, '-1.jpg')
    return portrait_path

def display_film_grid(films_df, films_per_page=50, films_per_row=10, custom_col=None):
    for i in range(0, films_per_page, films_per_row):
        row_films = films_df.iloc[i:i+films_per_row]
        cols = st.columns(films_per_row)
        for j, (_, film) in enumerate(row_films.iterrows()):
            with cols[j]:
                film_card(film, custom_col=custom_col)

def streaming_row(streaming_text):
    streaming_icon_width = 33
    if streaming_text:
        streaming_list = streaming_text.split(', ')
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        with col1:
            try:
                icon_path = get_streaming_icon(streaming_list[0])
                st.image(icon_path, width=streaming_icon_width)
            except:
                pass
        with col2:
            try:
                icon_path = get_streaming_icon(streaming_list[1])
                st.image(icon_path, width=streaming_icon_width)
            except:
                pass
        with col3:
            try:
                icon_path = get_streaming_icon(streaming_list[2])
                st.image(icon_path, width=streaming_icon_width)
            except:
                pass
        with col4:
            try:
                icon_path = get_streaming_icon(streaming_list[3])
                st.image(icon_path, width=streaming_icon_width)
            except:
                pass
            
def film_card(film, custom_col=None):
    poster_path = get_poster_path(film['FILM_ID'])
    st.image(poster_path, use_column_width=True)
    streaming_row(film['STREAMING_SERVICES'])
    button_text = \
    f"**{film['FILM_TITLE']} ({film['FILM_YEAR']})**\n\n\n" \
    f"Algo Score: {film['ALGO_SCORE']:.2f}\n\n" \
    f"Watches: {film['FILM_WATCH_COUNT']:,}\n\n" \
    f"Rating: {film['FILM_RATING']:.2f}\n\n"
    if custom_col:
        button_text = button_text + f"{sort_obj['print_name']}: {film[sort_obj['col_name']]}"
    st.link_button(button_text, url=film['LETTERBOXD_URL'], use_container_width=True)

def film_card_ranked(film, rated=False, custom_col=None):
    col1, col2, col3, col4 = st.columns([0.2, 0.2, 0.1, 0.7])
    with col1:
        st.markdown("""
                    <style>
                    .big-font {
                        font-size:40px !important;
                        text-align: center;
                    }
                    </style>
                    """, unsafe_allow_html=True)
        st.markdown('<p class="big-font"> {} </p>'.format(int(film['Ranking'])), unsafe_allow_html=True)
    with col2:
        poster_path = get_poster_path(film['FILM_ID'])
        st.image(poster_path, width=166)
    with col4:
        card_text1 = f"**{film['FILM_TITLE']} ({film['FILM_YEAR']})**"
        st.link_button(card_text1, url=film['LETTERBOXD_URL'])
        if custom_col:
            card_text_custom = f"{custom_col.replace('_', ' ').title()} = {film[custom_col]:}"
            st.write(card_text_custom)
        card_text2 = f"Watch Count: {film['FILM_WATCH_COUNT']:,}"
        st.write(card_text2)
        card_text3 = f"Letterboxd Rating: {film['FILM_RATING']}"
        st.write(card_text3)
        film_rating = film['FILM_RATING_SCALED'] if film['FILM_RATING_SCALED'] else -1
        if film_rating > 0:
            card_text4 = f"My Rating: {film['FILM_RATING_SCALED']:.4}"
        else:
            card_text4 = f"Algo Score: {film['ALGO_SCORE']:.4}"
        st.write(card_text4)
        if rated:
            card_text_rated = f"My Ranking: {int(film['FILM_POSITION']):,}"
        else:
            card_text_rated = ""
        st.write(card_text_rated)

def display_film_grid_ranking(films_df, limit=50, rated=False, custom_col=None):
    for _, film in films_df.head(limit).iterrows():
        film_card_ranked(film, rated=rated, custom_col=custom_col)

def person_card(person, name_column='DIRECTOR_NAME', custom_col=None):
    portrait_path = get_portrait_path(person['PERSON_ID'])
    st.image(portrait_path, use_column_width=True)
    button_text = \
    f"**{person[name_column]}**\n\n\n" \
    f"Total Films: {person['TOTAL_FILMS']:,}\n\n" \
    f"% Watched: {person['PERCENT_WATCHED']:,.1%}\n\n" \
    f"My Mean Rating: {person['MY_RATING_MEAN']:.2f}\n\n" \
    f"Total Letterboxd Watches: {person['LETTERBOXD_WATCH_COUNT']:,}\n\n" \
    f"Letterboxd Mean Rating: {person['LETTERBOXD_RATING_MEAN']:.2f}\n\n"
    if custom_col:
        button_text = button_text + f"{custom_col['print_name']}: {person[custom_col['col_name']]}"
    if st.button(button_text, use_container_width=True):
        if name_column=='DIRECTOR_NAME':
            st.session_state.selected_director = person['PERSON_ID']
        elif name_column=='ACTOR_NAME':
            st.session_state.selected_actor = person['PERSON_ID']
        st.rerun()

def display_person_grid(people_df, people_per_page=50, people_per_row=10, name_column='DIRECTOR_NAME', custom_col=None):
    for i in range(0, people_per_page, people_per_row):
        row_people = people_df.iloc[i:i+people_per_row]
        cols = st.columns(people_per_row)
        for j, (_, person) in enumerate(row_people.iterrows()):
            with cols[j]:
                person_card(person, name_column=name_column, custom_col=custom_col)

# st.write(watchlist_df)

# Streamlit app

# st.write('st.session_state.selected_actor={}'.format(st.session_state.selected_actor))
# st.write('st.session_state.selected_director={}'.format(st.session_state.selected_director))

watchlist_tab, ranked_tab, director_tab, actor_tab, diary_tab, year_tab, algo_tab, filmid_lookup_tab = st.tabs(['Watchlist', 'Ranked', 'Director', 'Actor', 'Diary', 'Year', 'Algo', 'FILM_ID Lookup'])

with watchlist_tab:
    
    pos0, pos1, pos2, pos3, pos4 = st.columns(5)
    with pos0:
        watchlist_filter = st.radio('Watchlist:', ['Either', 'Yes', 'No'], horizontal=True, index=1)
        if watchlist_filter != 'Either':
            watchlist_df = watchlist_df[watchlist_df['RATED'] == int(watchlist_filter.replace('Yes', '0').replace('No', '1'))]
    with pos1:
        seen_filter = st.radio('Seen', ['Either', 'Yes', 'No'], horizontal=True)
        if seen_filter == 'Yes':
            watchlist_df = watchlist_df[watchlist_df['SEEN'] == 1]
        elif seen_filter == 'No':
            watchlist_df = watchlist_df[watchlist_df['SEEN'] == 0]
    with pos2:
        rated_filter = st.radio('Rated', ['Either', 'Yes', 'No'], horizontal=True)
        if rated_filter == 'Yes':
            watchlist_df = watchlist_df[watchlist_df['RATED'] == 1]
        elif rated_filter == 'No':
            watchlist_df = watchlist_df[watchlist_df['RATED'] == 0]
    with pos3:
        streaming_filter = st.radio('Streaming:', ['Either', 'Yes', 'No'], horizontal=True)
        if streaming_filter != 'Either':
            watchlist_df = watchlist_df[watchlist_df['STREAMING'] == streaming_filter]
    with pos4:
        services = select_statement_to_df('SELECT DISTINCT STREAMING_SERVICE_FULL FROM FILM_STREAMING_SERVICES')['STREAMING_SERVICE_FULL'].tolist()
        specific_streaming_filter = st.selectbox('Specific Service:', services, index=None)
        if specific_streaming_filter:
            watchlist_df = watchlist_df[watchlist_df['STREAMING_SERVICES'].str.contains(specific_streaming_filter)]
    if len(watchlist_df) > 0:
        pos0, pos1, pos2, pos3, pos4 = st.columns([0.6, 0.4, 1, 1, 1])
        
        with pos0:
            released_filter = st.radio('Released:', ['Either', 'Yes', 'No'], horizontal=True)
            if released_filter != 'Either':
                watchlist_df = watchlist_df[watchlist_df['RELEASED'] == int(released_filter.replace('Yes', '1').replace('No', '0'))]
        with pos1:
            pos0_, pos1_ = st.columns(2)
            with pos0_:
                year_from = st.number_input('Year:', value=watchlist_df['FILM_YEAR'].min(), step=1)
            with pos1_:
                year_to = st.number_input('', value=watchlist_df['FILM_YEAR'].max(), step=1)
            watchlist_df = watchlist_df[(watchlist_df['FILM_YEAR'] >= year_from) & (watchlist_df['FILM_YEAR'] <= year_to)]
        with pos2:
            genre_filter = st.multiselect("Select Genre:", sorted(watchlist_df['FILM_GENRE'].unique()))
            if genre_filter:
                for genre in genre_filter:
                    watchlist_df = watchlist_df[watchlist_df['ALL_FILM_GENRES'].str.contains(genre)]
        with pos3:
            quant_film_lengths = {'Any': 999, '<90m': 90, '<1h40': 100, '<2h': 120, '<3h': 180}
            quant_length_filter = st.radio('Film Length:', quant_film_lengths.keys(), horizontal=True)
            watchlist_df = watchlist_df[watchlist_df['FILM_RUNTIME'] <= quant_film_lengths[quant_length_filter]]
        with pos4:
            sort_options = {
                'Algo Score': {'print_name': 'Algo Score', 'col_name': 'ALGO_SCORE', 'asc': False},
                'My Rating': {'print_name': 'My Rating', 'col_name': 'FILM_RATING_SCALED', 'asc': False},
                'Letterboxd Watch Count': {'print_name': 'Letterboxd Watch Count', 'col_name': 'FILM_WATCH_COUNT', 'asc': False},
                'Letterboxd Rating': {'print_name': 'Letterboxd Rating', 'col_name': 'FILM_RATING', 'asc': False},
                'Letterboxd Likes per Watch': {'print_name': 'Letterboxd Likes per Watch', 'col_name': 'LIKES_PER_WATCH', 'asc': False},
                'Letterboxd Fans per Watch': {'print_name': 'Letterboxd Fans per Watch', 'col_name': 'FANS_PER_WATCH', 'asc': False},
                'Letterboxd Top 250': {'print_name': 'Letterboxd Top 250', 'col_name': 'FILM_TOP_250', 'asc': True},
                'My Rating vs Letterboxd': {'print_name': 'My Rating vs Letterboxd', 'col_name': 'MY_RATING_VS_LB', 'asc': False},
                'Release Date': {'print_name':'Release Date', 'col_name':'FILM_RELEASE_DATE', 'asc': False},
                'Film Title': {'print_name': 'Film Title', 'col_name': 'FILM_TITLE', 'asc': True}
                            }
            sort_order = st.selectbox('Sort Order:', sort_options.keys())
            sort_obj = sort_options[sort_order]
            watchlist_df = watchlist_df.sort_values(sort_obj['col_name'], ascending=sort_obj['asc'])
        default_displays_values = ['Algo Score', 'Letterboxd Watch Count', 'Letterboxd Rating']
        if sort_order in default_displays_values:
            display_film_grid(watchlist_df, FILMS_PER_PAGE, FILMS_PER_ROW)
        else:
            display_film_grid(watchlist_df, FILMS_PER_PAGE, FILMS_PER_ROW, custom_col=sort_obj)

with ranked_tab:

    non_ranking_features = ['FILM_ID', 'FILM_TITLE']
    selectable_features = [x for x in st.session_state['dfs']['ranked'].columns if x not in non_ranking_features]
    # selectable_features.pop(-1)
    selected_feature_df = st.session_state['dfs']['ranked']
    selected_feature = st.selectbox("Select Feature to show ranking:", selectable_features, index=None)
    if selected_feature:
        unique_values = st.session_state['dfs']['ranked'][selected_feature].unique()
        if len(unique_values) == 2:
            selected_feature_value = 1
            display_feature = None
            selected_feature_df = selected_feature_df[selected_feature_df[selected_feature] == selected_feature_value]
        else:
            display_feature = selected_feature
            # selected_feature_value = st.selectbox("Select Value for Feature to show ranking:", np.sort(unique_values), index=None)
            col1, col2, _ = st.columns([1,1,5])
            with col1:
                lowest_allowed_value = st.number_input('Lowest value:', value=unique_values.min())
            with col2:
                highest_allowed_value = st.number_input('Highest value:', value=unique_values.max())
            selected_feature_df = selected_feature_df[(selected_feature_df[selected_feature] >= lowest_allowed_value) & (selected_feature_df[selected_feature] <= highest_allowed_value)]
        rating_ranked_df = selected_feature_df[selected_feature_df['FILM_RATING_SCALED'].notnull()].sort_values('FILM_RATING_SCALED', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'FILM_RATING', 'FILM_WATCH_COUNT', 'FILM_RATING_SCALED']]
        rating_ranked_df.insert(0, 'Ranking', rating_ranked_df.index + 1)
        algo_ranked_df = selected_feature_df[selected_feature_df['FILM_RATING_SCALED'].isnull()].sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR','FILM_RATING', 'FILM_WATCH_COUNT',  'ALGO_SCORE']]
        algo_ranked_df.insert(0, 'Ranking', algo_ranked_df.index + 1)
        mean_ranking = rating_ranked_df['FILM_RATING_SCALED'].mean()
        total_films = len(selected_feature_df)
        if total_films > 0:
            watched_films = selected_feature_df['WATCHED'].sum()
            rated_films = len(rating_ranked_df)
            st.write(selected_feature+' Ranked!')
            st.write('Watched {}/{} ({:.2%})'.format(watched_films, total_films, watched_films / total_films))
            st.write('Rated {}/{} ({:.2%})'.format(rated_films, total_films, rated_films / total_films))
            st.write('Mean rating of {:.2f} vs the top level mean of {:.2f}'.format(mean_ranking, st.session_state['dfs']['ranked']['FILM_RATING_SCALED'].mean()))
            left_pos, right_pos = st.columns(2)
            with left_pos:
                display_film_grid_ranking(rating_ranked_df, rated=True, custom_col=display_feature)
            with right_pos:
                display_film_grid_ranking(algo_ranked_df, custom_col=display_feature)
        else:
            st.write('No Films to show - adjust filters above')

with director_tab:

    if st.session_state.selected_director is not None:
        selected_director_df = select_statement_to_df('SELECT * FROM precomputed_director_film_level WHERE PERSON_ID = {}'.format(st.session_state.selected_director))
        selected_director_name = selected_director_df.loc[0, 'DIRECTOR_NAME']
        rating_director_df = selected_director_df[selected_director_df['FILM_RATING_SCALED'].notnull()].sort_values('FILM_RATING_SCALED', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'FILM_RATING', 'FILM_WATCH_COUNT', 'FILM_RATING_SCALED']]
        rating_director_df.insert(0, 'Ranking', rating_director_df.index + 1)
        algo_director_df = selected_director_df[selected_director_df['FILM_RATING_SCALED'].isnull()].sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR','FILM_RATING', 'FILM_WATCH_COUNT',  'ALGO_SCORE']]
        algo_director_df.insert(0, 'Ranking', algo_director_df.index + 1)
        mean_ranking = rating_director_df['FILM_RATING_SCALED'].mean()
        total_films = len(selected_director_df)
        watched_films = selected_director_df['WATCHED'].sum()
        rated_films = len(rating_director_df)
        if st.button('Return to Director Grid'):
            st.session_state.selected_director = None
            st.rerun()
        st.write(selected_director_name+' Ranked!')
        st.write('Watched {}/{} ({:.2%})'.format(watched_films, total_films, watched_films / total_films))
        st.write('Rated {}/{} ({:.2%})'.format(rated_films, total_films, rated_films / total_films))
        if mean_ranking > 0:
            st.write('Mean rating of {:.2f} vs the top level mean of {:.2f}'.format(mean_ranking, st.session_state['dfs']['ranked']['FILM_RATING_SCALED'].mean()))
        left_pos, right_pos = st.columns(2)
        with left_pos:
            display_film_grid_ranking(rating_director_df, rated=True)
        with right_pos:
            display_film_grid_ranking(algo_director_df)
    else:
        pos0, pos1, pos2, pos3, pos4 = st.columns([0.6, 0.4, 1, 1, 1])
        with pos0:
            director_name = st.text_input('Enter Director:')
            if director_name:
                tmp_df = select_statement_to_df('SELECT PERSON_ID, PERSON_NAME FROM PERSON_INFO WHERE REPLACE(PERSON_NAME, ".", "") LIKE "%{}%"'.format(director_name))
                director_df = director_df.merge(tmp_df, how='inner', on='PERSON_ID')
        with pos4:
            sort_options2 = {
                'Total Films': {'print_name': 'Total Films', 'col_name': 'TOTAL_FILMS', 'asc': False},
                'Films Watched': {'print_name': 'Films Watched', 'col_name': 'FILMS_WATCHED', 'asc': False},
                '% Watched': {'print_name': '% Watched', 'col_name': 'PERCENT_WATCHED', 'asc': False},
                'Films Rated': {'print_name': 'Films Rated', 'col_name': 'FILMS_RATED', 'asc': False},
                '% Rated': {'print_name': '% Rated', 'col_name': 'PERCENT_RATED', 'asc': False},
                'My Mean Rating': {'print_name': 'My Mean Rating', 'col_name': 'MY_RATING_MEAN', 'asc': False},
                'My Mean Rating Top 5': {'print_name': 'My Mean Rating Top 5', 'col_name': 'TOP_FIVE_RATING', 'asc': False},
                'Letterboxd Mean Rating': {'print_name': 'Letterboxd Mean Rating', 'col_name': 'LETTERBOXD_RATING_MEAN', 'asc': False},
                'Total Letterboxd Watches': {'print_name': 'Total Letterboxd Watches', 'col_name': 'LETTERBOXD_WATCH_COUNT', 'asc': False},
                'Letterboxd Watches per Film': {'print_name': 'Letterboxd Watches per Film', 'col_name': 'LETTERBOXD_WATCHES_PER_FILM', 'asc': False}
                            }
            st.session_state['director_sort_order'] = st.selectbox('Director Sort Order:', sort_options2.keys(), index=list(sort_options2).index(st.session_state['director_sort_order']))
            st.session_state['director_sort_order_persistent'] = st.session_state['director_sort_order']
            sort_obj2 = sort_options2[st.session_state['director_sort_order']]
            director_df = director_df.sort_values([sort_obj2['col_name'], 'TOTAL_FILMS'], ascending=[sort_obj2['asc'], False])
        default_displays_values2 = ['Total Films', 'Percent Watched', 'My Mean Rating', 'Letterboxd Mean Rating', 'Total Letterboxd Watches']
        if st.session_state['director_sort_order'] in default_displays_values2:
            display_person_grid(director_df, FILMS_PER_PAGE, FILMS_PER_ROW)
        else:
            display_person_grid(director_df, FILMS_PER_PAGE, FILMS_PER_ROW, custom_col=sort_obj2)

with actor_tab:
    
    if st.session_state.selected_actor is not None:
        selected_actor_df = select_statement_to_df('SELECT * FROM precomputed_actor_film_level WHERE PERSON_ID = {}'.format(st.session_state.selected_actor))
        selected_actor_name = selected_actor_df.loc[0, 'ACTOR_NAME']
        rating_actor_df = selected_actor_df[selected_actor_df['FILM_RATING_SCALED'].notnull()].sort_values('FILM_RATING_SCALED', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'FILM_RATING', 'FILM_WATCH_COUNT', 'FILM_RATING_SCALED']]
        rating_actor_df.insert(0, 'Ranking', rating_actor_df.index + 1)
        algo_actor_df = selected_actor_df[selected_actor_df['FILM_RATING_SCALED'].isnull()].sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)#[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR','FILM_RATING', 'FILM_WATCH_COUNT',  'ALGO_SCORE']]
        algo_actor_df.insert(0, 'Ranking', algo_actor_df.index + 1)
        mean_ranking = rating_actor_df['FILM_RATING_SCALED'].mean()
        total_films = len(selected_actor_df)
        watched_films = selected_actor_df['WATCHED'].sum()
        rated_films = len(rating_actor_df)
        if st.button('Return to Actor Grid'):
            st.session_state.selected_actor = None
            st.rerun()
        st.write(selected_actor_name+' Ranked!')
        st.write('Watched {}/{} ({:.2%})'.format(watched_films, total_films, watched_films / total_films))
        st.write('Rated {}/{} ({:.2%})'.format(rated_films, total_films, rated_films / total_films))
        if mean_ranking > 0:
            st.write('Mean rating of {:.2f} vs the top level mean of {:.2f}'.format(mean_ranking, st.session_state['dfs']['ranked']['FILM_RATING_SCALED'].mean()))
        left_pos, right_pos = st.columns(2)
        with left_pos:
            display_film_grid_ranking(rating_actor_df, rated=True)
        with right_pos:
            display_film_grid_ranking(algo_actor_df)
    else:
        pos0, pos1, pos2, pos3, pos4 = st.columns([0.6, 0.4, 1, 1, 1])
        with pos0:
            actor_name = st.text_input('Enter Actor:')
            if actor_name:
                tmp_df = select_statement_to_df('SELECT PERSON_ID, PERSON_NAME FROM PERSON_INFO WHERE REPLACE(PERSON_NAME, ".", "") LIKE "%{}%"'.format(actor_name))
                actor_df = actor_df.merge(tmp_df, how='inner', on='PERSON_ID')
        with pos4:
            sort_options3 = {
                'Total Films': {'print_name': 'Total Films', 'col_name': 'TOTAL_FILMS', 'asc': False},
                'Films Watched': {'print_name': 'Films Watched', 'col_name': 'FILMS_WATCHED', 'asc': False},
                '% Watched': {'print_name': '% Watched', 'col_name': 'PERCENT_WATCHED', 'asc': False},
                'Films Rated': {'print_name': 'Films Rated', 'col_name': 'FILMS_RATED', 'asc': False},
                '% Rated': {'print_name': '% Rated', 'col_name': 'PERCENT_RATED', 'asc': False},
                'My Mean Rating': {'print_name': 'My Mean Rating', 'col_name': 'MY_RATING_MEAN', 'asc': False},
                'Letterboxd Mean Rating': {'print_name': 'Letterboxd Mean Rating', 'col_name': 'LETTERBOXD_RATING_MEAN', 'asc': False},
                'Total Letterboxd Watches': {'print_name': 'Total Letterboxd Watches', 'col_name': 'LETTERBOXD_WATCH_COUNT', 'asc': False},
                'Letterboxd Watches per Film': {'print_name': 'Letterboxd Watches per Film', 'col_name': 'LETTERBOXD_WATCHES_PER_FILM', 'asc': False}
                            }
            st.session_state['actor_sort_order'] = st.selectbox('Actor Sort Order:', sort_options3.keys(), index=list(sort_options3).index(st.session_state['actor_sort_order']))
            st.session_state['actor_sort_order_persistent'] = st.session_state['actor_sort_order']
            sort_obj3 = sort_options3[st.session_state['actor_sort_order']]
            actor_df = actor_df.sort_values([sort_obj3['col_name'], 'TOTAL_FILMS'], ascending=[sort_obj3['asc'], False])
        default_displays_values3 = ['Total Films', 'Percent Watched', 'My Mean Rating', 'Letterboxd Mean Rating', 'Total Letterboxd Watches']
        if st.session_state['actor_sort_order'] in default_displays_values3:
            display_person_grid(actor_df, FILMS_PER_PAGE, FILMS_PER_ROW, name_column='ACTOR_NAME')
        else:
            display_person_grid(actor_df, FILMS_PER_PAGE, FILMS_PER_ROW, name_column='ACTOR_NAME', custom_col=sort_obj3)

with diary_tab:
    st.line_chart(data=diary_df, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
    st.line_chart(data=diary_df, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
    st.line_chart(data=diary_df, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
    # st.dataframe(st.session_state['dfs']['diary_detail'], use_container_width=True, hide_index=True)

with year_tab:

    pos0, pos1, pos2, pos3, pos4 = st.columns([0.6, 0.4, 1, 1, 1])
    with pos4:
        sort_options4 = {
            'Total Films': {'print_name': 'Total Films', 'col_name': 'TOTAL_FILMS', 'asc': False},
            'Films Watched': {'print_name': 'Films Watched', 'col_name': 'FILMS_WATCHED', 'asc': False},
            '% Watched': {'print_name': '% Watched', 'col_name': 'PERCENT_WATCHED', 'asc': False},
            'Films Rated': {'print_name': 'Films Rated', 'col_name': 'FILMS_RATED', 'asc': False},
            '% Rated': {'print_name': '% Rated', 'col_name': 'PERCENT_RATED', 'asc': False},
            'My Mean Rating': {'print_name': 'My Mean Rating', 'col_name': 'MY_RATING_MEAN', 'asc': False},
            'Letterboxd Mean Rating': {'print_name': 'Letterboxd Mean Rating', 'col_name': 'LETTERBOXD_RATING_MEAN', 'asc': False},
            'Total Letterboxd Watches': {'print_name': 'Total Letterboxd Watches', 'col_name': 'LETTERBOXD_WATCH_COUNT', 'asc': False},
            'Letterboxd Watches per Film': {'print_name': 'Letterboxd Watches per Film', 'col_name': 'LETTERBOXD_WATCHES_PER_FILM', 'asc': False},
            'Letterboxd Watches Watched': {'print_name': 'Letterboxd Watches Watched', 'col_name': 'LETTERBOXD_WATCH_COUNT_WATCHED', 'asc': False},
            'Letterboxd Watches Watched %': {'print_name': 'Letterboxd Watches Watched %', 'col_name': 'LETTERBOXD_WATCH_COUNT_WATCHED_PERCENT', 'asc': False}
                        }
        sort_order4 = st.selectbox('Year Display:', sort_options4.keys())
        sort_obj4 = sort_options4[sort_order4]
    st.dataframe(year_df, use_container_width=True, hide_index=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y=sort_obj4['col_name'], use_container_width=True)
    year_selection = st.selectbox('', np.sort(year_df['FILM_YEAR'].unique()), index=None, placeholder='Select a Year:', label_visibility='collapsed')
    if year_selection:
        st.dataframe(year_df[year_df['FILM_YEAR']==year_selection], use_container_width=True, hide_index=True)
        algo_features_df_year = st.session_state['dfs']['ranked'][st.session_state['dfs']['ranked']['FILM_YEAR'] == year_selection].reset_index(drop=True)
        st.dataframe(algo_features_df_year, use_container_width=True, hide_index=True)

with algo_tab:

    st.write(st.session_state['dfs']['shap'])

    algo_feature = st.selectbox('Select a Feature:', st.session_state['dfs']['model_features'])
    feature_values = st.session_state['dfs']['shap'][algo_feature]
    shap_values = st.session_state['dfs']['shap'][algo_feature+'_SHAP']
    # st.dataframe(shap_values)
    feature_shap_scatter = px.scatter(
        x=feature_values,
        y=shap_values,
        # size='FILM_WATCH_COUNT',
        color=st.session_state['dfs']['shap']['RATED'],
        hover_name=st.session_state['dfs']['shap']['FILM_TITLE'],
        size_max=30,
        template="plotly_dark"
        )
    feature_shap_scatter.update_traces(marker_sizemin=10)
    st.plotly_chart(feature_shap_scatter, theme="streamlit", use_container_width=True)

    tmp_df = st.session_state['dfs']['ranked'].copy().sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE']]
    tmp_df['FILM_TITLE_YEAR_ID'] = tmp_df['FILM_TITLE'] + ' - ' + tmp_df['FILM_YEAR'].astype(str) + ' (' + tmp_df['FILM_ID'] + ')'
    tmp_df = tmp_df.sort_values('ALGO_SCORE', ascending=False)
    film_name_years = st.multiselect('Select Films:', tmp_df['FILM_TITLE_YEAR_ID'].unique())

    if len(film_name_years) > 0:
        film_ids = [tmp_df[tmp_df['FILM_TITLE_YEAR_ID']==x]['FILM_ID'].values[0] for x in film_name_years]
        film_names = [x[:-17] for x in film_name_years]
        tmp_df = st.session_state['dfs']['shap'][st.session_state['dfs']['shap']['FILM_ID'].isin(film_ids)]

        val_cols = [x for x in tmp_df.columns if len(x)<=5 or x[-5:] != '_SHAP']
        tmp_df_val = tmp_df[val_cols]
        melted_df_val = pd.melt(tmp_df_val, id_vars=['FILM_ID', 'FILM_TITLE'])

        shap_cols = ['FILM_ID', 'FILM_TITLE'] + [x for x in tmp_df.columns if len(x)>5 and x[-5:] == '_SHAP']
        tmp_df_shap = tmp_df[shap_cols]
        tmp_df_shap.columns = ['FILM_ID', 'FILM_TITLE'] + [x[:-5] for x in tmp_df.columns if len(x)>5 and x[-5:] == '_SHAP']
        melted_df_shap = pd.melt(tmp_df_shap, id_vars=['FILM_ID', 'FILM_TITLE'])

        melted_df_shap = melted_df_shap[melted_df_shap['value'].abs() > 0.0005].reset_index(drop=True)
        melted_df_shap.columns = [x.replace('value', 'shap_value') for x in melted_df_shap.columns]
        melted_df_shap['feature_value'] = melted_df_shap.apply(lambda x: tmp_df_val[tmp_df_val['FILM_ID'] == x['FILM_ID']].loc[:, x['variable']].values[0] if x['variable'] not in ['BASE_VALUE', 'PREDICTION'] else None, axis=1)
        transposed_df = melted_df_shap.drop('FILM_ID', axis=1).pivot(index='variable', columns='FILM_TITLE', values=['feature_value', 'shap_value'])
        transposed_df.columns = ['_'.join(col) for col in transposed_df.columns]
        transposed_df = transposed_df.reset_index()
        transposed_df = transposed_df.fillna(0)
        if len(film_names) > 1:
            transposed_df2 = transposed_df.copy()[['variable', 'feature_value_'+film_names[0], 'feature_value_'+film_names[1], 'shap_value_'+film_names[0], 'shap_value_'+film_names[1]]]
            transposed_df2.columns = ['variable', film_names[0], film_names[1], film_names[0] + ' SHAP', film_names[1] + ' SHAP']
            transposed_df2['VAR'] = transposed_df2[film_names[1]+' SHAP'] - transposed_df2[film_names[0]+' SHAP']
            transposed_df2['ABS_VAR'] = transposed_df2['VAR'].abs()
            transposed_df2 = transposed_df2[transposed_df2['ABS_VAR'] > 0]
            transposed_df2 = transposed_df2.sort_values('ABS_VAR', ascending=False)
        else:
            transposed_df2 = transposed_df.copy()[['variable', 'feature_value_'+film_names[0], 'shap_value_'+film_names[0]]]
            transposed_df2.columns = ['variable', film_names[0], film_names[0]+' SHAP']
            transposed_df2 = transposed_df2.sort_values(film_names[0]+' SHAP', ascending=False)
        transposed_df2 = transposed_df2.round(3)
        st.dataframe(transposed_df2, use_container_width=True, hide_index=True)
        
with filmid_lookup_tab:
    
    film_search = st.text_input('Enter Film Name or ID:')
    film_id_df = select_statement_to_df('SELECT * FROM FILM_TITLE WHERE (FILM_ID LIKE "%{}%") OR (FILM_TITLE LIKE "%{}%")'.format(film_search, film_search))
    if len(film_id_df) < 50:
        st.dataframe(film_id_df, hide_index=True)