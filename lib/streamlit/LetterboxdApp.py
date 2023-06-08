import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import yaml
import streamlit as st
from streamlit_extras.dataframe_explorer import dataframe_explorer
import sys
sys.path.insert(0, '../data_prep')
from sqlite_utils import select_statement_to_df
# from algo_utils import return_comparison_df

# Read the queries file
with open('streamlit_queries.yaml', 'r') as file:
    queries = yaml.safe_load(file)
watchlist_query = queries['watchlist_query']['sql']
year_completion_query = queries['year_completion_query']['sql']
genre_completion_query = queries['genre_completion_query']['sql']
director_completion_query = queries['director_completion_query']['sql']
director_film_level_query = queries['director_film_level_query']['sql']
film_score_query = queries['film_score_query']['sql']
diary_query_basic = queries['diary_query_basic']['sql']

def scale_col(df, column, suffix='', a=0, b=1):
    col_min = df[column].min()
    col_max = df[column].max()
    col_range = (col_max - col_min)
    if col_range == 0:
        df[column+suffix] = 0
    else:
        df[column+suffix] = ((df[column] - col_min) / col_range) * (b - a) + a
    return df

df = select_statement_to_df(watchlist_query)

year_df = select_statement_to_df(year_completion_query)
genre_df = select_statement_to_df(genre_completion_query)
director_df = select_statement_to_df(director_completion_query)
director_film_level_df = select_statement_to_df(director_film_level_query)

film_score_df = select_statement_to_df(film_score_query)
diary_query_df = select_statement_to_df(diary_query_basic)
diary_query_df['WATCH_DATE'] = pd.to_datetime(diary_query_df['WATCH_DATE'])
date_range = pd.date_range(start=diary_query_df['WATCH_DATE'].min(), end=diary_query_df['WATCH_DATE'].max())
diary_query_df2 = diary_query_df.set_index('WATCH_DATE').reindex(date_range).fillna(0).rename_axis('WATCH_DATE').reset_index()
diary_query_df2['MOVIE_COUNT_ROLLING_7'] = diary_query_df2['MOVIE_COUNT'].rolling(window=7).mean()
diary_query_df2['MOVIE_COUNT_ROLLING_28'] = diary_query_df2['MOVIE_COUNT'].rolling(window=28).mean()
diary_query_df2['MOVIE_RATING_ROLLING_7'] = diary_query_df2['MOVIE_RATING'].rolling(window=7).mean()
diary_query_df2['MOVIE_RATING_ROLLING_28'] = diary_query_df2['MOVIE_RATING'].rolling(window=28).mean()


# FILTERING:

watchable_filter = st.sidebar.radio('Watchable:', ['Either', 'Yes', 'No'])
if watchable_filter != 'Either':
    df = df[df['WATCHABLE'] == watchable_filter]

streaming_filter = st.sidebar.radio('Streaming:', ['Either', 'Yes', 'No'])
if streaming_filter != 'Either':
    df = df[df['STREAMING'] == streaming_filter]
    
seen_filter = st.sidebar.radio('Seen', ['Either', 'Yes', 'No'])
if seen_filter == 'Yes':
    df = df[df['SEEN'] == 1]
elif seen_filter == 'No':
    df = df[df['SEEN'] == 0]

genre_filter = st.sidebar.multiselect("Select Genre:", df['FILM_GENRE'].unique())
if genre_filter:
    df = df[df['FILM_GENRE'].isin(genre_filter)]

decades_list = ['All'] + sorted(df['FILM_DECADE'].unique(), reverse=True)
decade_filter = st.sidebar.radio('Decade:', decades_list, label_visibility='collapsed')
if decade_filter != 'All':
    df = df[df['FILM_DECADE'] == decade_filter]

film_year_min = int(df['FILM_YEAR'].min())
film_year_max = int(df['FILM_YEAR'].max())
film_year = st.sidebar.slider('Film Year:', film_year_min, film_year_max, (film_year_min, film_year_max))
df = df[(df['FILM_YEAR'] >= film_year[0]) & (df['FILM_YEAR'] <= film_year[1])]

quant_film_lengths = {'Any': 999, '<90m': 90, '<1h40': 100, '<2h': 120}
quant_length_filter = st.sidebar.radio('Film Length:', quant_film_lengths.keys())
df = df[df['FILM_RUNTIME'] <= quant_film_lengths[quant_length_filter]]

film_length_min = int(df['FILM_RUNTIME'].min())
film_length_max = int(df['FILM_RUNTIME'].max())
film_length = st.sidebar.slider('Film Runtime (mins):', film_length_min, film_length_max, (film_length_min, film_length_max))
df = df[(df['FILM_RUNTIME'] >= film_length[0]) & (df['FILM_RUNTIME'] <= film_length[1])]

df_scaled = df.copy()
df_scaled['FILM_TOP_250'] = np.where(df_scaled['FILM_TOP_250'].notnull(), 1, 0)
df_scaled['FILM_RUNTIME'] = 1 / df_scaled['FILM_RUNTIME']
df_scaled['FILM_RATING'] = df_scaled['FILM_RATING'].fillna(df_scaled['FILM_RATING'].mean())
df_scaled['SEEN_SCORE'] = np.where(df_scaled['SEEN']==1, 0, 1)
df_scaled = scale_col(df_scaled, 'FILM_WATCH_COUNT')
df_scaled = scale_col(df_scaled, 'FILM_RATING')
df_scaled = scale_col(df_scaled, 'FILM_LIKES_COUNT')
df_scaled = scale_col(df_scaled, 'FILM_FAN_COUNT')
df_scaled = scale_col(df_scaled, 'FILM_RUNTIME')
df_scaled = scale_col(df_scaled, 'GENRE_SCORE')

df_scaled['SCORE_WEIGHTED'] =                  \
      (df_scaled['SEEN_SCORE']       * 3 )     \
    + (df_scaled['FILM_WATCH_COUNT'] * 2 )     \
    + (df_scaled['FILM_TOP_250']     * 1 )     \
    + (df_scaled['FILM_RATING']      * 2 )     \
    + (df_scaled['FILM_FAN_COUNT']   * 1 )     \
    + (df_scaled['FILM_RUNTIME']     * .5)     \
    + (df_scaled['GENRE_SCORE']      * 2 )     \
    
df_scaled = scale_col(df_scaled, 'SCORE_WEIGHTED', '_SCALED', a=0, b=100)
# df_scaled['SEEN_SCORE'] = df_scaled['SEEN_SCORE'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_WATCH_COUNT'] = df_scaled['FILM_WATCH_COUNT'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_TOP_250'] = df_scaled['FILM_TOP_250'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_RATING'] = df_scaled['FILM_RATING'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_FAN_COUNT'] = df_scaled['FILM_FAN_COUNT'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_RUNTIME'] = df_scaled['FILM_RUNTIME'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['GENRE_SCORE'] = df_scaled['GENRE_SCORE'] / df_scaled['SCORE_WEIGHTED']
df2 = df.merge(df_scaled[['FILM_ID', 'SCORE_WEIGHTED']], how='left', on='FILM_ID')
algo_scores = select_statement_to_df('SELECT FILM_ID, ALGO_SCORE FROM FILM_ALGO_SCORE')
df2 = df2.merge(algo_scores, how='left', on='FILM_ID')
df_sorted = df2.sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)
df_sorted['SEEN'] = df_sorted['SEEN'].replace({0: 'No', 1: 'Yes'})

df_display = df_sorted[['FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE', 'SCORE_WEIGHTED', 'STREAMING_SERVICES', 'FILM_WATCH_COUNT', 'FILM_RATING', 'MIN_RENTAL_PRICE']]
df_scores = df_scaled[['FILM_TITLE', 'FILM_YEAR', 'SEEN_SCORE', 'FILM_WATCH_COUNT', 'FILM_TOP_250', 'FILM_RATING', 'FILM_FAN_COUNT', 'FILM_RUNTIME', 'GENRE_SCORE', 'SCORE_WEIGHTED', 'SCORE_WEIGHTED_SCALED']]
px_fig = px.scatter(
    df_sorted,
    x='FILM_RUNTIME',
    y='FILM_RATING',
    size='FILM_WATCH_COUNT',
    color='SEEN',
    hover_name='FILM_TITLE',
    size_max=30,
    template="plotly_dark"
)
px_fig.update_traces(marker_sizemin=10)

watchlist_tab, algo_whiteboard_tab, diary_tab, year_tab, genre_tab, director_tab, filmid_lookup_tab = st.tabs(['Ordered Watchlist', 'Algo Whiteboard', 'Diary Visualisation', 'Year Completion', 'Genre Completion', 'Director Completion', 'FILM_ID Lookup'])
# save
with watchlist_tab:
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    # st.dataframe(df)
    st.plotly_chart(px_fig, theme="streamlit", use_container_width=True)
    st.dataframe(df_scores, use_container_width=True, hide_index=True)
    shap_df = df2[['FILM_ID', 'FILM_TITLE', 'ALGO_SCORE']].merge(select_statement_to_df('SELECT * FROM FILM_SHAP_VALUES'), how='left', on='FILM_ID')
    # shap_df = shap_df.sort_values('PREDICTION', ascending=False).reset_index(drop=True)
    shap_df['SCALER'] = shap_df['ALGO_SCORE'] / shap_df['PREDICTION']
    shap_df2 = shap_df.drop(['FILM_ID', 'FILM_TITLE'], axis=1).mul(shap_df['SCALER'], axis=0).drop(['ALGO_SCORE', 'SCALER'], axis=1)
    shap_df2.insert(0, 'FILM_ID', df2['FILM_ID'])
    shap_df2.insert(1, 'FILM_TITLE', df2['FILM_TITLE'])
    shap_df2 = shap_df2.sort_values('PREDICTION', ascending=False)
    tmp_df = df_sorted[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE']].copy()
    tmp_df['FILM_TITLE_YEAR'] = tmp_df['FILM_TITLE'] + ' - ' + tmp_df['FILM_YEAR'].astype(str)
    tmp_df = tmp_df.sort_values('ALGO_SCORE', ascending=False)
    film_name_years = st.multiselect('Select Films:', tmp_df['FILM_TITLE_YEAR'].unique())
    film_ids = [tmp_df[tmp_df['FILM_TITLE_YEAR']==x]['FILM_ID'].values[0] for x in film_name_years]
    # st.dataframe(return_comparison_df(film_ids, min_shap_val=0.001, decimal_places=3), hide_index=True, use_container_width=True)
    film_names = [x[:-7] for x in film_name_years]
    # film_ids = [tmp_df[tmp_df['FILM_TITLE_YEAR'] == x]['FILM_ID'] for x in film_names]
    melted_df = pd.melt(shap_df2[shap_df2['FILM_ID'].isin(film_ids)], id_vars=['FILM_ID', 'FILM_TITLE'])
    valid_melted_df = melted_df[melted_df['value'].abs() > 0.0005].reset_index(drop=True)
    valid_melted_df = valid_melted_df[valid_melted_df['variable'] != 'BASE_VALUE']
    # valid_df = valid_melted_df.pivot_table(values='value', index=['FILM_ID', 'FILM_TITLE'], columns='variable').reset_index().drop('FILM_ID', axis=1)
    transposed_df = valid_melted_df.drop('FILM_ID', axis=1).pivot(index='variable', columns='FILM_TITLE', values='value').reset_index()
    transposed_df = transposed_df.fillna(0)
    transposed_df = transposed_df[['variable', *film_names]]
    if len(film_names) > 1:
        transposed_df['VAR'] = transposed_df[film_names[1]] - transposed_df[film_names[0]]
        transposed_df['ABS_VAR'] = (transposed_df[film_names[0]] - transposed_df[film_names[1]]).abs()
        transposed_df = transposed_df.sort_values('ABS_VAR', ascending=False)
    else:
        transposed_df = transposed_df.sort_values(film_names[0], ascending=False)
    transposed_df.columns = [x.replace('variable', 'FEATURE_NAME') for x in transposed_df.columns]
    transposed_df = transposed_df.round(3)
    st.dataframe(transposed_df, hide_index=True)

with algo_whiteboard_tab:
    y_cols= ['SEEN_SCORE', 'FILM_WATCH_COUNT', 'FILM_TOP_250', 'FILM_RATING', 'FILM_FAN_COUNT', 'FILM_RUNTIME', 'GENRE_SCORE']
    films_to_show = 66
    SEEN_SCORE_SCALER = st.number_input('SEEN_SCORE_SCALER', min_value=0.0, max_value=10.0, value=3.0, step=.5)
    FILM_WATCH_COUNT_SCALER = st.number_input('FILM_WATCH_COUNT_SCALER', min_value=0.0, max_value=10.0, value=2.0, step=.5)
    FILM_TOP_250_SCALER = st.number_input('FILM_TOP_250_SCALER', min_value=0.0, max_value=10.0, value=1.0, step=.5)
    FILM_RATING_SCALER = st.number_input('FILM_RATING_SCALER', min_value=0.0, max_value=10.0, value=2.0, step=.5)
    FILM_FAN_COUNT_SCALER = st.number_input('FILM_FAN_COUNT_SCALER', min_value=0.0, max_value=10.0, value=1.0, step=.5)
    FILM_RUNTIME_SCALER = st.number_input('FILM_RUNTIME_SCALER', min_value=0.0, max_value=10.0, value=.5, step=.5)
    GENRE_SCORE_SCALER = st.number_input('GENRE_SCORE_SCALER', min_value=0.0, max_value=10.0, value=2.0, step=.5)

    df_weighted = df_scaled.copy()
    df_weighted['SEEN_SCORE'] =           df_weighted['SEEN_SCORE']        * SEEN_SCORE_SCALER
    df_weighted['FILM_WATCH_COUNT'] =     df_weighted['FILM_WATCH_COUNT']  * FILM_WATCH_COUNT_SCALER
    df_weighted['FILM_TOP_250'] =         df_weighted['FILM_TOP_250']      * FILM_TOP_250_SCALER
    df_weighted['FILM_RATING'] =          df_weighted['FILM_RATING']       * FILM_RATING_SCALER
    df_weighted['FILM_FAN_COUNT'] =       df_weighted['FILM_FAN_COUNT']    * FILM_FAN_COUNT_SCALER
    df_weighted['FILM_RUNTIME'] =         df_weighted['FILM_RUNTIME']      * FILM_RUNTIME_SCALER
    df_weighted['GENRE_SCORE'] =          df_weighted['GENRE_SCORE']       * GENRE_SCORE_SCALER

    df_weighted['TOTAL_WEIGHTED_SCORE'] = df_weighted['SEEN_SCORE']          \
                                        + df_weighted['FILM_WATCH_COUNT']    \
                                        + df_weighted['FILM_TOP_250']        \
                                        + df_weighted['FILM_RATING']         \
                                        + df_weighted['FILM_FAN_COUNT']      \
                                        + df_weighted['FILM_RUNTIME']        \
                                        + df_weighted['GENRE_SCORE']         \

    df_weighted = df_weighted.sort_values('TOTAL_WEIGHTED_SCORE', ascending=False).reset_index(drop=True)
    df_weighted['PADDED_INDEX'] = df_weighted.index.map(lambda x: str(x).zfill(2))
    df_weighted['FILM_TITLE_SORT'] = df_weighted['PADDED_INDEX'] + df_weighted['FILM_TITLE']
    st.bar_chart(data=df_weighted.head(films_to_show)[['FILM_TITLE_SORT'] + y_cols], x='FILM_TITLE_SORT', y=y_cols)
    st.dataframe(df_weighted.head(films_to_show)[['FILM_TITLE_SORT', 'TOTAL_WEIGHTED_SCORE'] + y_cols], hide_index=True)

with diary_tab:
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.dataframe(diary_query_df2, hide_index=True)
        
with year_tab:
    st.bar_chart(data=year_df, x='FILM_YEAR', y='PERCENT_WATCHED', use_container_width=True)
    st.dataframe(year_df, hide_index=True)
    year_scatter = px.scatter(
        year_df,
        x='FILMS_WATCHED',
        y='PERCENT_WATCHED',
        hover_name='FILM_YEAR',
        color='FILM_YEAR',
        size_max=30,
        template="plotly_dark"
    )
    st.plotly_chart(year_scatter, theme='streamlit', use_container_width=True)
        
with genre_tab:
    genre_bar = px.bar(genre_df, x='FILM_GENRE', y='PERCENT_WATCHED')
    genre_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(genre_bar, theme='streamlit', use_container_width=True)
    st.dataframe(genre_df, hide_index=True)
    genre_scatter = px.scatter(
        genre_df,
        x='FILMS_WATCHED',
        y='PERCENT_WATCHED',
        hover_name='FILM_GENRE',
        template="plotly_dark"
    )
    st.plotly_chart(genre_scatter, theme='streamlit', use_container_width=True)
	
with director_tab:
    director_hist = px.histogram(director_df, x="PERCENT_WATCHED", nbins=8, range_x=(0,1.05))
    st.plotly_chart(director_hist, theme='streamlit', use_container_width=True)
    director_bar = px.bar(director_df.head(50), x='DIRECTOR_NAME', y='PERCENT_WATCHED')
    director_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(director_bar, theme='streamlit', use_container_width=True)
    pop_director_scatter = px.scatter(
         director_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_WATCHED',
    	 hover_name='DIRECTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.dataframe(director_df, hide_index=True)
    director_scatter = px.scatter(
         director_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_WATCHED',
    	 hover_name='DIRECTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(director_scatter, theme='streamlit', use_container_width=True)
    director_name = st.selectbox('Enter Director:', director_df['DIRECTOR_NAME'].unique())
    st.dataframe(director_film_level_df[director_film_level_df['DIRECTOR_NAME'] == director_name], hide_index=True, height=600)
        
with filmid_lookup_tab:
    film_search = st.text_input('Enter Film Name or ID:')
    film_id_df = select_statement_to_df('SELECT * FROM FILM_TITLE WHERE (FILM_ID LIKE "%{}%") OR (FILM_TITLE LIKE "%{}%")'.format(film_search, film_search))
    st.dataframe(film_id_df, hide_index=True)
    