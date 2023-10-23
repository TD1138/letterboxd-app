import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import yaml
import streamlit as st
from streamlit_extras.dataframe_explorer import dataframe_explorer
import altair as alt
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
director_debut_query = queries['director_debut_query']['sql']
actor_completion_query = queries['actor_completion_query']['sql']
actor_film_level_query = queries['actor_film_level_query']['sql']
actor_debut_query = queries['actor_debut_query']['sql']
film_score_query = queries['film_score_query']['sql']
diary_query_basic = queries['diary_query_basic']['sql']
watched_feature_stats_query = queries['watched_feature_stats_query']['sql']
collection_completion_query = queries['collection_completion_query']['sql']
collection_film_level_query = queries['collection_film_level_query']['sql']

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
director_debut_df = select_statement_to_df(director_debut_query)
actor_df = select_statement_to_df(actor_completion_query)
actor_film_level_df = select_statement_to_df(actor_film_level_query)
actor_debut_df = select_statement_to_df(actor_debut_query)
collection_df = select_statement_to_df(collection_completion_query)
collection_film_level_df = select_statement_to_df(collection_film_level_query)

film_score_df = select_statement_to_df(film_score_query)
diary_query_df = select_statement_to_df(diary_query_basic)
diary_query_df['WATCH_DATE'] = pd.to_datetime(diary_query_df['WATCH_DATE'])
date_range = pd.date_range(start=diary_query_df['WATCH_DATE'].min(), end=diary_query_df['WATCH_DATE'].max())
diary_query_df2 = diary_query_df.set_index('WATCH_DATE').reindex(date_range).fillna(0).rename_axis('WATCH_DATE').reset_index()
diary_query_df2['MOVIE_COUNT_ROLLING_7'] = diary_query_df2['MOVIE_COUNT'].rolling(window=7).mean()
diary_query_df2['MOVIE_COUNT_ROLLING_28'] = diary_query_df2['MOVIE_COUNT'].rolling(window=28).mean()
diary_query_df2['MOVIE_RATING_ROLLING_7'] = diary_query_df2['MOVIE_RATING'].rolling(window=7).mean()
diary_query_df2['MOVIE_RATING_ROLLING_28'] = diary_query_df2['MOVIE_RATING'].rolling(window=28).mean()

watched_feature_stats_df = select_statement_to_df(watched_feature_stats_query)

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
# algo_scores = select_statement_to_df('SELECT FILM_ID, ALGO_SCORE FROM FILM_ALGO_SCORE')
algo_features_df = select_statement_to_df('SELECT * FROM FILM_ALGO_SCORE')
df2 = df2.merge(algo_features_df[['FILM_ID', 'ALGO_SCORE']], how='left', on='FILM_ID')
df_sorted = df2.sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)
df_sorted['SEEN'] = df_sorted['SEEN'].replace({0: 'No', 1: 'Yes'})

df_display = df_sorted[['FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE', 'SCORE_WEIGHTED', 'STREAMING_SERVICES', 'FILM_WATCH_COUNT', 'FILM_RATING', 'MIN_RENTAL_PRICE']]
df_scores = df_scaled[['FILM_TITLE', 'FILM_YEAR', 'SEEN_SCORE', 'FILM_WATCH_COUNT', 'FILM_TOP_250', 'FILM_RATING', 'FILM_FAN_COUNT', 'FILM_RUNTIME', 'GENRE_SCORE', 'SCORE_WEIGHTED', 'SCORE_WEIGHTED_SCALED']]


watchlist_tab, diary_tab, stats, year_tab, genre_tab, director_tab, actor_tab, collections_tab, filmid_lookup_tab = st.tabs(['Ordered Watchlist', 'Diary Visualisation', 'Statistics', 'Year Completion', 'Genre Completion', 'Director Completion', 'Actor Completion', 'Collections Completion', 'FILM_ID Lookup'])
# save
with watchlist_tab:
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    # st.dataframe(df)
    px_fig = px.scatter(
        df_sorted,
        x='FILM_RUNTIME',
        y='ALGO_SCORE',
        size='FILM_WATCH_COUNT',
        color='SEEN',
        hover_name='FILM_TITLE',
        size_max=30,
        template="plotly_dark"
        )
    px_fig.update_traces(marker_sizemin=10)
    st.plotly_chart(px_fig, theme="streamlit", use_container_width=True)
    st.dataframe(algo_features_df, use_container_width=True, hide_index=True)
    shap_df = df2[['FILM_ID', 'FILM_TITLE', 'ALGO_SCORE']].merge(select_statement_to_df('SELECT * FROM FILM_SHAP_VALUES'), how='left', on='FILM_ID')
    # shap_df = shap_df.sort_values('PREDICTION', ascending=False).reset_index(drop=True)
    shap_df['SCALER'] = shap_df['ALGO_SCORE'] / shap_df['PREDICTION']
    shap_df2 = shap_df.drop(['FILM_ID', 'FILM_TITLE'], axis=1).mul(shap_df['SCALER'], axis=0).drop(['ALGO_SCORE', 'SCALER'], axis=1)
    shap_df2.insert(0, 'FILM_ID', df2['FILM_ID'])
    shap_df2.insert(1, 'FILM_TITLE', df2['FILM_TITLE'])
    shap_df2 = shap_df2.sort_values('PREDICTION', ascending=False)
    tmp_df = df_sorted[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE']].copy()
    tmp_df['FILM_TITLE_YEAR_ID'] = tmp_df['FILM_TITLE'] + ' - ' + tmp_df['FILM_YEAR'].astype(str) + ' (' + tmp_df['FILM_ID'] + ')'
    tmp_df = tmp_df.sort_values('ALGO_SCORE', ascending=False)
    st.dataframe(tmp_df)
    film_name_years = st.multiselect('Select Films:', tmp_df['FILM_TITLE_YEAR_ID'].unique())
    if len(film_name_years) > 0:
        film_ids = [tmp_df[tmp_df['FILM_TITLE_YEAR_ID']==x]['FILM_ID'].values[0] for x in film_name_years]
        # st.dataframe(return_comparison_df(film_ids, min_shap_val=0.001, decimal_places=3), hide_index=True, use_container_width=True)
        film_names = [x[:-17] for x in film_name_years]
        # film_ids = [tmp_df[tmp_df['FILM_TITLE_YEAR'] == x]['FILM_ID'] for x in film_names]
        melted_df = pd.melt(shap_df2[shap_df2['FILM_ID'].isin(film_ids)], id_vars=['FILM_ID', 'FILM_TITLE'])
        valid_melted_df = melted_df#[melted_df['value'].abs() > 0.0005].reset_index(drop=True)
        valid_melted_df.columns = [x.replace('value', 'shap_value') for x in valid_melted_df.columns]
        valid_melted_df['feature_value'] = valid_melted_df.apply(lambda x: algo_features_df[algo_features_df['FILM_ID']==x.FILM_ID].loc[:, x['variable']].values[0] if x['variable'] not in ['BASE_VALUE', 'PREDICTION'] else None, axis=1)
        # valid_melted_df = valid_melted_df[valid_melted_df['variable'] != 'BASE_VALUE']
        # valid_melted_df['label'] = np.where(valid_melted_df['variable'] == 'BASE_VALUE', 'BASE_VALUE', valid_melted_df['variable'].astype('str') + '=' + valid_melted_df['og_val'].astype('str'))
        # valid_df = valid_melted_df.pivot_table(values='value', index=['FILM_ID', 'FILM_TITLE'], columns='variable').reset_index().drop('FILM_ID', axis=1)
        st.dataframe(valid_melted_df)
        transposed_df = valid_melted_df.drop('FILM_ID', axis=1).pivot(index='variable', columns='FILM_TITLE', values=['feature_value', 'shap_value'])
        transposed_df.columns = ['_'.join(col) for col in transposed_df.columns]
        transposed_df = transposed_df.reset_index()
        transposed_df = transposed_df.fillna(0)
        if len(film_names) > 1:
            transposed_df2 = transposed_df.copy()[['variable', 'feature_value_'+film_names[0], 'feature_value_'+film_names[1], 'shap_value_'+film_names[0], 'shap_value_'+film_names[1]]]
            transposed_df2.columns = ['variable', film_names[0], film_names[1], film_names[0] + ' SHAP', film_names[1] + ' SHAP']
            transposed_df2['VAR'] = transposed_df2[film_names[1]+' SHAP'] - transposed_df2[film_names[0]+' SHAP']
            transposed_df2['ABS_VAR'] = transposed_df2['VAR'].abs()
            transposed_df2 = transposed_df2.sort_values('ABS_VAR', ascending=False)
        else:
            transposed_df2 = transposed_df.copy()[['variable', 'feature_value_'+film_names[0], 'shap_value_'+film_names[0]]]
            transposed_df2.columns = ['variable', film_names[0], film_names[0]+' SHAP']
            transposed_df2 = transposed_df2.sort_values(film_names[0]+' SHAP', ascending=False)
        transposed_df2 = transposed_df2.round(3)
        st.write(film_ids)
        st.dataframe(transposed_df2, use_container_width=True, hide_index=True)

with diary_tab:
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.dataframe(diary_query_df2, use_container_width=True, hide_index=True)

with stats:
    ratings_basic_hist = px.bar(watched_feature_stats_df[['FILM_RATING_BASIC', 'FILM_ID']].groupby('FILM_RATING_BASIC').count().reset_index(), x="FILM_RATING_BASIC", y='FILM_ID')
    st.plotly_chart(ratings_basic_hist, theme='streamlit', use_container_width=True)
    ratings_hist = px.histogram(watched_feature_stats_df, x="FILM_RATING_SCALED", nbins=10, range_x=(0,5))
    st.plotly_chart(ratings_hist, theme='streamlit', use_container_width=True)
    genre_agg = watched_feature_stats_df.groupby('FILM_GENRE').agg({'FILM_RATING_SCALED': 'mean', 'FILM_ID': 'count'}).reset_index()
    genre_agg.columns = ['Genre', 'Rating_mean', 'Films_watched']
    genre_agg_scatter = px.scatter(
         genre_agg,
    	 x='Films_watched',
    	 y='Rating_mean',
    	 hover_name='Genre',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(genre_agg_scatter, theme='streamlit', use_container_width=True)

with year_tab:
    st.bar_chart(data=year_df, x='FILM_YEAR', y='PERCENT_WATCHED', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='MY_MEAN_RATING', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='MEAN_RATING', use_container_width=True)
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
    year_selection = st.selectbox('Select a Year:', np.sort(tmp_df['FILM_YEAR'].unique()))
    algo_features_df_year = algo_features_df[algo_features_df['FILM_YEAR'] == year_selection].reset_index(drop=True)
    algo_features_df_year_x = dataframe_explorer(algo_features_df_year)
    st.dataframe(algo_features_df_year_x, use_container_width=True, hide_index=True)
        
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
    director_watched_bar = px.bar(director_df.head(50), x='DIRECTOR_NAME', y='PERCENT_WATCHED')
    director_watched_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(director_watched_bar, theme='streamlit', use_container_width=True)
    director_rated_bar = px.bar(director_df[director_df['PERCENT_RATED'] > 0].sort_values('PERCENT_RATED', ascending=False).head(50), x='DIRECTOR_NAME', y='PERCENT_RATED')
    director_rated_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(director_rated_bar, theme='streamlit', use_container_width=True)
    st.dataframe(director_df, hide_index=True)
    director_scatter = px.scatter(
         director_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_WATCHED',
         size='TOTAL_FILMS',
    	 hover_name='DIRECTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(director_scatter, theme='streamlit', use_container_width=True)
    director_watch_rate_scatter = px.scatter(
         director_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_RATED',
         size='TOTAL_FILMS',
    	 hover_name='DIRECTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(director_watch_rate_scatter, theme='streamlit', use_container_width=True)
    director_name = st.selectbox('Enter Director:', director_df['DIRECTOR_NAME'].unique())
    director_df_filtered = director_film_level_df[director_film_level_df['DIRECTOR_NAME'] == director_name]
    st.dataframe(director_df_filtered, hide_index=True, height=600)
    director_df_filtered_reshaped = pd.melt(director_df_filtered, id_vars=['FILM_TITLE', 'DIRECTOR_NAME'], value_vars=['FILM_RATING', 'FILM_RATING_SCALED'], var_name='RATING_TYPE', value_name='RATING')
    st.line_chart(data=director_df_filtered, x="FILM_TITLE", y=["FILM_RATING", "FILM_RATING_SCALED"])
    st.altair_chart(alt.Chart(director_df_filtered_reshaped).mark_line().encode(x=alt.X('FILM_TITLE', sort=None), y='RATING', color='RATING_TYPE'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='DAYS_SINCE_DEBUT', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='AGE_IN_DAYS', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='FILM_RELEASE_DATE', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)

with actor_tab:
    actor_hist = px.histogram(actor_df, x="PERCENT_WATCHED", nbins=8, range_x=(0,1.05))
    st.plotly_chart(actor_hist, theme='streamlit', use_container_width=True)
    actor_watched_bar = px.bar(actor_df.head(50), x='ACTOR_NAME', y='PERCENT_WATCHED')
    actor_watched_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(actor_watched_bar, theme='streamlit', use_container_width=True)
    actor_rated_bar = px.bar(actor_df[actor_df['PERCENT_RATED'] > 0].sort_values('PERCENT_RATED', ascending=False).head(50), x='ACTOR_NAME', y='PERCENT_RATED')
    actor_rated_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(actor_rated_bar, theme='streamlit', use_container_width=True)
    st.dataframe(actor_df, hide_index=True)
    actor_scatter = px.scatter(
         actor_df,
    	 x='FILMS_WATCHED',
    	 y='PERCENT_WATCHED',
         size='TOTAL_FILMS',
    	 hover_name='ACTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(actor_scatter, theme='streamlit', use_container_width=True)
    actor_watch_rate_scatter = px.scatter(
         actor_df,
    	 x='TOTAL_FILMS',
    	 y='PERCENT_RATED',
         size='TOTAL_FILMS',
    	 hover_name='ACTOR_NAME',
    	 size_max=30,
    	 template="plotly_dark"
		)
    st.plotly_chart(actor_watch_rate_scatter, theme='streamlit', use_container_width=True)
    actor_name = st.selectbox('Enter Actor:', actor_df['ACTOR_NAME'].unique())
    actor_df_filtered = actor_film_level_df[actor_film_level_df['ACTOR_NAME'] == actor_name]
    st.dataframe(actor_df_filtered, hide_index=True, height=600)
    st.line_chart(data=actor_df_filtered, x="FILM_TITLE", y=["FILM_RATING", "FILM_RATING_SCALED"])
    st.altair_chart(alt.Chart(actor_debut_df).mark_line().encode(x='DAYS_SINCE_DEBUT', y='FILM_NUMBER', color='ACTOR_NAME'), use_container_width=True)

with collections_tab:
    collection_df_x = dataframe_explorer(collection_df)
    st.dataframe(collection_df_x, use_container_width=True, hide_index=True)
    collection_name = st.selectbox('Enter Collection:', collection_df['COLLECTION_NAME'].unique())
    collection_df_filtered = collection_film_level_df[collection_film_level_df['COLLECTION_NAME'] == collection_name]
    collection_df_filtered = collection_df_filtered.drop('COLLECTION_NAME', axis=1)
    st.dataframe(collection_df_filtered, hide_index=True, height=600)

with filmid_lookup_tab:
    film_search = st.text_input('Enter Film Name or ID:')
    film_id_df = select_statement_to_df('SELECT * FROM FILM_TITLE WHERE (FILM_ID LIKE "%{}%") OR (FILM_TITLE LIKE "%{}%")'.format(film_search, film_search))
    st.dataframe(film_id_df, hide_index=True)