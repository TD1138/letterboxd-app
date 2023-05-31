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

# Read the queries file
with open('streamlit_queries.yaml', 'r') as file:
    queries = yaml.safe_load(file)
watchlist_query = queries['watchlist_query']['sql']
year_completion_query = queries['year_completion_query']['sql']
genre_completion_query = queries['genre_completion_query']['sql']
director_completion_query = queries['director_completion_query']['sql']
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
if seen_filter != 'Either':
    df = df[df['SEEN'] == seen_filter]

quant_film_lengths = {'Any': 999, '<90m': 90, '<1h40': 100, '<2h': 120}
quant_length_filter = st.sidebar.radio('Film Length:', quant_film_lengths.keys())
df = df[df['FILM_RUNTIME'] <= quant_film_lengths[quant_length_filter]]

film_length_min = int(df['FILM_RUNTIME'].min())
film_length_max = int(df['FILM_RUNTIME'].max())
film_length = st.sidebar.slider('Film Runtime (mins):', film_length_min, film_length_max, (film_length_min, film_length_max))
df = df[(df['FILM_RUNTIME'] >= film_length[0]) & (df['FILM_RUNTIME'] <= film_length[1])]

genre_filter = st.sidebar.multiselect("Select Genre:", df['FILM_GENRE'].unique())
if genre_filter:
    df = df[df['FILM_GENRE'].isin(genre_filter)]

decades_list = ['All'] + sorted(df['FILM_DECADE'].unique(), reverse=True)
decade_filter = st.sidebar.radio('Decade:', decades_list)
if decade_filter != 'All':
    df = df[df['FILM_DECADE'] == decade_filter]

film_year_min = int(df['FILM_YEAR'].min())
film_year_max = int(df['FILM_YEAR'].max())
film_year = st.sidebar.slider('Film Year:', film_year_min, film_year_max, (film_year_min, film_year_max))
df = df[(df['FILM_YEAR'] >= film_year[0]) & (df['FILM_YEAR'] <= film_year[1])]

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

df_scaled['SCORE_WEIGHTED'] =                \
      df_scaled['SEEN_SCORE']       * 3      \
    + df_scaled['FILM_WATCH_COUNT'] * 2      \
    + df_scaled['FILM_TOP_250']     * 1      \
    + df_scaled['FILM_RATING']      * 2      \
    + df_scaled['FILM_FAN_COUNT']   * 1      \
    + df_scaled['FILM_RUNTIME']     * .5     \
    + df_scaled['GENRE_SCORE']      * 2      \
    
df_scaled = scale_col(df_scaled, 'SCORE_WEIGHTED', '_SCALED', a=0, b=100)
# df_scaled['SEEN_SCORE'] = df_scaled['SEEN_SCORE'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_WATCH_COUNT'] = df_scaled['FILM_WATCH_COUNT'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_TOP_250'] = df_scaled['FILM_TOP_250'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_RATING'] = df_scaled['FILM_RATING'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_FAN_COUNT'] = df_scaled['FILM_FAN_COUNT'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['FILM_RUNTIME'] = df_scaled['FILM_RUNTIME'] / df_scaled['SCORE_WEIGHTED']
# df_scaled['GENRE_SCORE'] = df_scaled['GENRE_SCORE'] / df_scaled['SCORE_WEIGHTED']
df2 = df.merge(df_scaled[['FILM_ID', 'SCORE_WEIGHTED']], how='left', on='FILM_ID')
df_sorted = df2.sort_values('SCORE_WEIGHTED', ascending=False).reset_index(drop=True)
df_sorted['SEEN'] = df_sorted['SEEN'].replace({0: 'No', 1: 'Yes'})

df_display = df_sorted[['FILM_TITLE', 'FILM_YEAR', 'STREAMING_SERVICES', 'FILM_WATCH_COUNT', 'FILM_RATING', 'MIN_RENTAL_PRICE']]
df_scores = df_scaled[['FILM_TITLE', 'FILM_YEAR', 'SEEN_SCORE', 'FILM_WATCH_COUNT', 'FILM_TOP_250', 'FILM_RATING', 'FILM_FAN_COUNT', 'FILM_RUNTIME', 'GENRE_SCORE', 'SCORE_WEIGHTED', 'SCORE_WEIGHTED_SCALED']]
#
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

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(['Ordered Watchlist', 'Diary Visualisation', 'Year Completion', 'Genre Completion', 'Director Completion', 'FILM_ID Lookup'])

with tab1:
	st.dataframe(df_display, use_container_width=True)
	st.plotly_chart(px_fig, theme="streamlit", use_container_width=True)
	st.dataframe(df_scores, use_container_width=True)

with tab2:
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.write(diary_query_df2)
        
with tab3:
    st.bar_chart(data=year_df, x='FILM_YEAR', y='PERCENT_WATCHED', use_container_width=True)
    st.write(year_df)
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
        
with tab4:
    genre_bar = px.bar(genre_df, x='FILM_GENRE', y='PERCENT_WATCHED')
    genre_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(genre_bar, theme='streamlit', use_container_width=True)
    st.write(genre_df)
    genre_scatter = px.scatter(
        genre_df,
        x='FILMS_WATCHED',
        y='PERCENT_WATCHED',
        hover_name='FILM_GENRE',
        template="plotly_dark"
    )
    st.plotly_chart(genre_scatter, theme='streamlit', use_container_width=True)
	
with tab5:
    director_bar = px.bar(director_df, x='DIRECTOR_NAME', y='PERCENT_WATCHED')
    director_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(director_bar, theme='streamlit', use_container_width=True)
    st.write(director_df)
    director_scatter = px.scatter(
         director_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_WATCHED',
    	 hover_name='DIRECTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(director_scatter, theme='streamlit', use_container_width=True)
        
with tab6:
    film_name = st.text_input('Enter Film Name:')
    film_id_df = select_statement_to_df('SELECT * FROM FILM_TITLE WHERE FILM_TITLE LIKE "%{}%"'.format(film_name))
    st.write(film_id_df)
    