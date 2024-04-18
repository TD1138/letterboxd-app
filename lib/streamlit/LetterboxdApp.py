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

st.set_page_config(layout="wide")

if 'dfs' not in st.session_state:
    st.session_state['dfs'] = {}

    with open('streamlit_queries.yaml', 'r') as file:
        queries = yaml.safe_load(file)
    watchlist_query = queries['watchlist_query']['sql']
    all_films_query = queries['all_films_query']['sql']
    year_completion_query = queries['year_completion_query']['sql']
    genre_completion_query = queries['genre_completion_query']['sql']
    keyword_completion_query = queries['keyword_completion_query']['sql']
    keyword_film_level_query = queries['keyword_film_level_query']['sql']
    director_completion_query = queries['director_completion_query']['sql']
    director_film_level_query = queries['director_film_level_query']['sql']
    director_debut_query = queries['director_debut_query']['sql']
    director_topfive_query = queries['director_topfive_query']['sql']
    director_sparklines_query = queries['director_sparklines_query']['sql']
    actor_completion_query = queries['actor_completion_query']['sql']
    actor_film_level_query = queries['actor_film_level_query']['sql']
    # top_actor_film_level_query = queries['top_actor_film_level_query']['sql']
    actor_debut_query = queries['actor_debut_query']['sql']
    actor_sparklines_query = queries['actor_sparklines_query']['sql']
    film_score_query = queries['film_score_query']['sql']
    diary_query_agg = queries['diary_query_agg']['sql']
    diary_query_detail = queries['diary_query_detail']['sql']
    watched_feature_stats_query = queries['watched_feature_stats_query']['sql']
    collection_completion_query = queries['collection_completion_query']['sql']
    collection_film_level_query = queries['collection_film_level_query']['sql']
    collections_close_to_completion_query = queries['collections_close_to_completion_query']['sql']
    diagnostics_query = queries['diagnostics_query']['sql']
    
    df = select_statement_to_df(watchlist_query)
    all_films_df = select_statement_to_df(all_films_query)
    year_df = select_statement_to_df(year_completion_query)
    genre_df = select_statement_to_df(genre_completion_query)
    keyword_df = select_statement_to_df(keyword_completion_query)
    keyword_film_level_df = select_statement_to_df(keyword_film_level_query)
    director_df = select_statement_to_df(director_completion_query)
    director_film_level_df = select_statement_to_df(director_film_level_query)
    director_debut_df = select_statement_to_df(director_debut_query)
    director_topfive_df = select_statement_to_df(director_topfive_query)
    director_sparklines_df = select_statement_to_df(director_sparklines_query)
    actor_df = select_statement_to_df(actor_completion_query)
    actor_film_level_df = select_statement_to_df(actor_film_level_query)
    # top_actor_film_level_df = select_statement_to_df(top_actor_film_level_query)
    actor_debut_df = select_statement_to_df(actor_debut_query)
    actor_sparklines_df = select_statement_to_df(actor_sparklines_query)
    collection_df = select_statement_to_df(collection_completion_query)
    collection_film_level_df = select_statement_to_df(collection_film_level_query)
    collections_close_to_completion_df = select_statement_to_df(collections_close_to_completion_query)
    film_score_df = select_statement_to_df(film_score_query)
    diary_agg_df = select_statement_to_df(diary_query_agg)
    all_films_df = all_films_df.sort_values('FILM_WATCH_COUNT', ascending=False).reset_index(drop=True)
    diary_agg_df['WATCH_DATE'] = pd.to_datetime(diary_agg_df['WATCH_DATE'])
    date_range = pd.date_range(start=diary_agg_df['WATCH_DATE'].min(), end=diary_agg_df['WATCH_DATE'].max())
    diary_agg_df2 = diary_agg_df.set_index('WATCH_DATE').reindex(date_range).fillna(0).rename_axis('WATCH_DATE').reset_index()
    diary_agg_df2['MOVIE_COUNT_ROLLING_7'] = diary_agg_df2['MOVIE_COUNT'].rolling(window=7).mean()
    diary_agg_df2['MOVIE_COUNT_ROLLING_28'] = diary_agg_df2['MOVIE_COUNT'].rolling(window=28).mean()
    diary_agg_df2['MOVIE_RATING_ROLLING_7'] = diary_agg_df2['MOVIE_RATING'].rolling(window=7).mean()
    diary_agg_df2['MOVIE_RATING_ROLLING_28'] = diary_agg_df2['MOVIE_RATING'].rolling(window=28).mean()
    diary_detail_df = select_statement_to_df(diary_query_detail)
    watched_feature_stats_df = select_statement_to_df(watched_feature_stats_query)
    algo_features_df = select_statement_to_df('SELECT * FROM FILM_ALGO_SCORE')
    df = df.merge(algo_features_df[['FILM_ID', 'ALGO_SCORE']], how='left', on='FILM_ID')
    shap_df = df[['FILM_ID', 'FILM_TITLE', 'ALGO_SCORE']].merge(select_statement_to_df('SELECT * FROM FILM_SHAP_VALUES'), how='left', on='FILM_ID')
    shap_df['SCALER'] = shap_df['ALGO_SCORE'] / shap_df['PREDICTION']
    shap_df2 = shap_df.drop(['FILM_ID', 'FILM_TITLE'], axis=1).mul(shap_df['SCALER'], axis=0).drop(['ALGO_SCORE', 'SCALER'], axis=1)
    shap_df2.insert(0, 'FILM_ID', df['FILM_ID'])
    shap_df2.insert(1, 'FILM_TITLE', df['FILM_TITLE'])
    shap_df2 = shap_df2.sort_values('PREDICTION', ascending=False)
    me_vs_lb_df = watched_feature_stats_df[['FILM_ID', 'FILM_TITLE', 'LETTERBOXD_RATING', 'FILM_RATING_SCALED']].dropna(axis=0, subset=['LETTERBOXD_RATING', 'FILM_RATING_SCALED'])
    me_vs_lb_df['VARIANCE'] = me_vs_lb_df['FILM_RATING_SCALED'] - me_vs_lb_df['LETTERBOXD_RATING']
    me_vs_lb_df['ABSOLUTE_VARIANCE'] = abs(me_vs_lb_df['VARIANCE'])
    diagnostics_df = select_statement_to_df(diagnostics_query)

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
    # director_scatter = px.scatter(
    #      director_df,
    # 	 x='FILMS_WATCHED',
    # 	 y='PERCENT_WATCHED',
    #      size='TOTAL_FILMS',
    # 	 hover_name='DIRECTOR_NAME',
    # 	 size_max=30,
    # 	 template="plotly_dark"
	# 	)
    # director_watch_rate_scatter = px.scatter(
    #      director_df,
    # 	 x='FILMS_WATCHED',
    # 	 y='PERCENT_RATED',
    #      size='TOTAL_FILMS',
    # 	 hover_name='DIRECTOR_NAME',
    # 	 size_max=30,
    # 	 template="plotly_dark"
	# 	)
    # actor_scatter = px.scatter(
    #      actor_df,
    # 	 x='FILMS_WATCHED',
    # 	 y='PERCENT_WATCHED',
    #      size='TOTAL_FILMS',
    # 	 hover_name='ACTOR_NAME',
    # 	 size_max=30,
    # 	 template="plotly_dark"
	# 	)
    # actor_watch_rate_scatter = px.scatter(
    #      actor_df,
    # 	 x='TOTAL_FILMS',
    # 	 y='PERCENT_RATED',
    #      size='TOTAL_FILMS',
    # 	 hover_name='ACTOR_NAME',
    # 	 size_max=30,
    # 	 template="plotly_dark"
	# 	)
    me_vs_lb_df_scatter = px.scatter(
         me_vs_lb_df,
    	 x='FILM_RATING_SCALED',
    	 y='LETTERBOXD_RATING',
    	 hover_name='FILM_TITLE',
    	 size_max=30,
    	 template="plotly_dark",
         color='VARIANCE'
		)
    year_scatter = px.scatter(
        year_df,
        x='FILMS_WATCHED',
        y='PERCENT_WATCHED',
        hover_name='FILM_YEAR',
        color='FILM_YEAR',
        size_max=30,
        template="plotly_dark"
    )
    genre_scatter = px.scatter(
        genre_df,
        x='FILMS_WATCHED',
        y='PERCENT_WATCHED',
        hover_name='FILM_GENRE',
        template="plotly_dark"
    )

    genre_bar = px.bar(genre_df, x='FILM_GENRE', y='PERCENT_WATCHED')
    genre_bar.update_layout(xaxis={'categoryorder': 'total descending'})

    ratings_basic_hist = px.bar(watched_feature_stats_df[['FILM_RATING_BASIC', 'FILM_ID']].groupby('FILM_RATING_BASIC').count().reset_index(), x="FILM_RATING_BASIC", y='FILM_ID')
    ratings_hist = px.histogram(watched_feature_stats_df, x="FILM_RATING_SCALED", nbins=10, range_x=(0,5))

    # director_watched_bar = px.bar(director_df.head(50), x='DIRECTOR_NAME', y='PERCENT_WATCHED')
    # director_watched_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    # director_rated_bar = px.bar(director_df[director_df['PERCENT_RATED'] > 0].sort_values('PERCENT_RATED', ascending=False).head(50), x='DIRECTOR_NAME', y='PERCENT_RATED')
    # director_rated_bar.update_layout(xaxis={'categoryorder': 'total descending'})

    actor_hist = px.histogram(actor_df, x="PERCENT_WATCHED", nbins=8, range_x=(0,1.05))
    actor_days_since_debut_altchart = alt.Chart(actor_debut_df).mark_line().encode(x='DAYS_SINCE_DEBUT', y='FILM_NUMBER', color='ACTOR_NAME')
    actor_age_in_days_altchart = alt.Chart(actor_debut_df).mark_line().encode(x='AGE_IN_DAYS', y='FILM_NUMBER', color='ACTOR_NAME')
    actor_letterboxd_days_altchart = alt.Chart(actor_sparklines_df).mark_line().encode(x='LETTERBOXD_DAYS', y='CUMULATIVE_FILMS', color='ACTOR_NAME')
    actor_watched_bar = px.bar(actor_df.head(50), x='ACTOR_NAME', y='PERCENT_WATCHED')
    actor_watched_bar.update_layout(xaxis={'categoryorder': 'total descending'})
    actor_rated_bar = px.bar(actor_df[actor_df['PERCENT_RATED'] > 0].sort_values('PERCENT_RATED', ascending=False).head(50), x='ACTOR_NAME', y='PERCENT_RATED')
    actor_rated_bar.update_layout(xaxis={'categoryorder': 'total descending'})

    st.session_state['dfs']['df'] = df
    st.session_state['dfs']['all_films_df'] = all_films_df
    st.session_state['dfs']['year_df'] = year_df
    st.session_state['dfs']['genre_df'] = genre_df
    st.session_state['dfs']['keyword_df'] = keyword_df
    st.session_state['dfs']['keyword_film_level_df'] = keyword_film_level_df
    st.session_state['dfs']['director_df'] = director_df
    st.session_state['dfs']['director_film_level_df'] = director_film_level_df
    st.session_state['dfs']['director_debut_df'] = director_debut_df
    st.session_state['dfs']['director_topfive_df'] = director_topfive_df
    st.session_state['dfs']['director_sparklines_df'] = director_sparklines_df
    st.session_state['dfs']['actor_df'] = actor_df
    st.session_state['dfs']['actor_film_level_df'] = actor_film_level_df
    st.session_state['dfs']['actor_debut_df'] = actor_debut_df
    st.session_state['dfs']['actor_sparklines_df'] = actor_sparklines_df
    st.session_state['dfs']['collection_df'] = collection_df
    st.session_state['dfs']['collection_film_level_df'] = collection_film_level_df
    st.session_state['dfs']['collections_close_to_completion_df'] = collections_close_to_completion_df
    st.session_state['dfs']['film_score_df'] = film_score_df
    st.session_state['dfs']['diary_agg_df2'] = diary_agg_df2
    st.session_state['dfs']['diary_detail_df'] = diary_detail_df
    st.session_state['dfs']['watched_feature_stats_df'] = watched_feature_stats_df
    st.session_state['dfs']['algo_features_df'] = algo_features_df
    st.session_state['dfs']['shap_df2'] = shap_df2
    st.session_state['dfs']['ratings_basic_hist'] = ratings_basic_hist
    st.session_state['dfs']['ratings_hist'] = ratings_hist
    st.session_state['dfs']['genre_agg_scatter'] = genre_agg_scatter
    st.session_state['dfs']['me_vs_lb_df_scatter'] = me_vs_lb_df_scatter
    # st.session_state['dfs']['director_watched_bar'] = director_watched_bar
    # st.session_state['dfs']['director_rated_bar'] = director_rated_bar
    # st.session_state['dfs']['director_scatter'] = director_scatter
    # st.session_state['dfs']['director_watch_rate_scatter'] = director_watch_rate_scatter
    # st.session_state['dfs']['actor_scatter'] = actor_scatter
    # st.session_state['dfs']['actor_watch_rate_scatter'] = actor_watch_rate_scatter
    st.session_state['dfs']['actor_days_since_debut_altchart'] = actor_days_since_debut_altchart
    st.session_state['dfs']['actor_age_in_days_altchart'] = actor_age_in_days_altchart
    st.session_state['dfs']['actor_letterboxd_days_altchart'] = actor_letterboxd_days_altchart
    st.session_state['dfs']['actor_watched_bar'] = actor_watched_bar
    st.session_state['dfs']['actor_rated_bar'] = actor_rated_bar
    st.session_state['dfs']['actor_hist'] = actor_hist
    st.session_state['dfs']['me_vs_lb_df'] = me_vs_lb_df
    st.session_state['dfs']['year_scatter'] = year_scatter
    st.session_state['dfs']['genre_bar'] = genre_bar
    st.session_state['dfs']['genre_scatter'] = genre_scatter
else:
    df = st.session_state['dfs']['df']
    all_films_df = st.session_state['dfs']['all_films_df']
    year_df = st.session_state['dfs']['year_df']
    genre_df = st.session_state['dfs']['genre_df']
    keyword_df = st.session_state['dfs']['keyword_df']
    keyword_film_level_df = st.session_state['dfs']['keyword_film_level_df']
    director_df = st.session_state['dfs']['director_df']
    director_film_level_df = st.session_state['dfs']['director_film_level_df']
    director_debut_df = st.session_state['dfs']['director_debut_df']
    director_topfive_df = st.session_state['dfs']['director_topfive_df']
    director_sparklines_df = st.session_state['dfs']['director_sparklines_df']
    actor_df = st.session_state['dfs']['actor_df']
    actor_film_level_df = st.session_state['dfs']['actor_film_level_df']
    actor_debut_df = st.session_state['dfs']['actor_debut_df']
    actor_sparklines_df = st.session_state['dfs']['actor_sparklines_df']
    collection_df = st.session_state['dfs']['collection_df']
    collection_film_level_df = st.session_state['dfs']['collection_film_level_df']
    collections_close_to_completion_df = st.session_state['dfs']['collections_close_to_completion_df']
    film_score_df = st.session_state['dfs']['film_score_df']
    diary_agg_df2 = st.session_state['dfs']['diary_agg_df2']
    diary_detail_df = st.session_state['dfs']['diary_detail_df']
    watched_feature_stats_df = st.session_state['dfs']['watched_feature_stats_df']
    algo_features_df = st.session_state['dfs']['algo_features_df']
    shap_df2 = st.session_state['dfs']['shap_df2']
    me_vs_lb_df = st.session_state['dfs']['me_vs_lb_df']

def display_df_with_clickable_letterboxd_link(df, height=None):
    st.data_editor(
        df,
        column_config={
            'LETTERBOXD_URL': st.column_config.LinkColumn(
                'LETTERBOXD_URL', display_text='Open in Letterboxd'
            ),
        },
        height=height,
        use_container_width=True,
        hide_index=True,
    )

films_to_show = st.sidebar.number_input('Number of Films to show:', value=100, max_value=1000, step=10)

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

quant_film_lengths = {'Any': 999, '<90m': 90, '<1h40': 100, '<2h': 120, '<3h': 180, '<4h': 240}
quant_length_filter = st.sidebar.radio('Film Length:', quant_film_lengths.keys())
df = df[df['FILM_RUNTIME'] <= quant_film_lengths[quant_length_filter]]

film_length_min = int(df['FILM_RUNTIME'].min())
film_length_max = int(df['FILM_RUNTIME'].max())
film_length = st.sidebar.slider('Film Runtime (mins):', film_length_min, film_length_max, (film_length_min, film_length_max))
df = df[(df['FILM_RUNTIME'] >= film_length[0]) & (df['FILM_RUNTIME'] <= film_length[1])]

df_sorted = df.sort_values('ALGO_SCORE', ascending=False).reset_index(drop=True)
df_sorted['SEEN'] = df_sorted['SEEN'].replace({0: 'No', 1: 'Yes'})

df_display = df_sorted[['FILM_TITLE', 'FILM_YEAR', 'LETTERBOXD_URL', 'ALGO_SCORE', 'STREAMING_SERVICES', 'FILM_WATCH_COUNT', 'FILM_RATING', 'MIN_RENTAL_PRICE']]

watchlist_tab, all_films_tab, diary_tab, stats, year_tab, genre_tab, keyword_tab, director_tab, actor_tab, collections_tab, filmid_lookup_tab, diagnostics_tab = st.tabs(['Ordered Watchlist', 'All Films', 'Diary Visualisation', 'Statistics', 'Year Completion', 'Genre Completion', 'Keyword Completion', 'Director Completion', 'Actor Completion', 'Collections Completion', 'FILM_ID Lookup', 'Diagnostics'])

with watchlist_tab:
    display_df_with_clickable_letterboxd_link(df_display)
    film_runtime_scatter = px.scatter(
        df_sorted.head(films_to_show),
        x='FILM_RUNTIME',
        y='ALGO_SCORE',
        size='FILM_WATCH_COUNT',
        color='SEEN',
        hover_name='FILM_TITLE',
        size_max=30,
        template="plotly_dark"
        )
    film_runtime_scatter.update_traces(marker_sizemin=10)
    st.plotly_chart(film_runtime_scatter, theme="streamlit", use_container_width=True)
    tmp_df = df_sorted[['FILM_ID', 'FILM_TITLE', 'FILM_YEAR', 'ALGO_SCORE']].copy()
    tmp_df['FILM_TITLE_YEAR_ID'] = tmp_df['FILM_TITLE'] + ' - ' + tmp_df['FILM_YEAR'].astype(str) + ' (' + tmp_df['FILM_ID'] + ')'
    tmp_df = tmp_df.sort_values('ALGO_SCORE', ascending=False)
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
        # st.dataframe(valid_melted_df)
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
        st.dataframe(transposed_df2, use_container_width=True, hide_index=True)

with all_films_tab:
    seen_filter_all = st.radio('Seen:', ['Either', 'Yes', 'No'])
    if seen_filter_all == 'Yes':
        all_films_df = all_films_df[all_films_df['SEEN'] == 1]
    elif seen_filter_all == 'No':
        all_films_df = all_films_df[all_films_df['SEEN'] == 0]
    film_watch_count_filter = st.number_input('Film Watch Count:', value=1, max_value=3000000, step=1)
    all_films_df = all_films_df[all_films_df['FILM_WATCH_COUNT'] >= film_watch_count_filter]
    display_df_with_clickable_letterboxd_link(all_films_df, height=738)

with diary_tab:
	st.line_chart(data=diary_agg_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
	st.line_chart(data=diary_agg_df2, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.line_chart(data=diary_agg_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
	st.dataframe(diary_detail_df, use_container_width=True, hide_index=True)

with stats:
    ratings_basic_hist = px.bar(watched_feature_stats_df[['FILM_RATING_BASIC', 'FILM_ID']].groupby('FILM_RATING_BASIC').count().reset_index(), x="FILM_RATING_BASIC", y='FILM_ID')
    st.plotly_chart(st.session_state['dfs']['ratings_basic_hist'], theme='streamlit', use_container_width=True)
    st.plotly_chart(st.session_state['dfs']['ratings_hist'], theme='streamlit', use_container_width=True)
    st.plotly_chart(st.session_state['dfs']['genre_agg_scatter'], theme='streamlit', use_container_width=True)
    st.dataframe(me_vs_lb_df)
    st.plotly_chart(st.session_state['dfs']['me_vs_lb_df_scatter'], theme='streamlit')#, , use_container_width=True)

with year_tab:
    st.bar_chart(data=year_df, x='FILM_YEAR', y='TOTAL_FILMS', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='FILMS_WATCHED', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='PERCENT_WATCHED', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='MY_MEAN_RATING', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='MEAN_RATING', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='TOTAL_FILM_WATCH_COUNT', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='TOTAL_FILM_WATCH_COUNT_WATCHED', use_container_width=True)
    st.bar_chart(data=year_df, x='FILM_YEAR', y='TOTAL_FILM_WATCH_COUNT_WATCHED_PERCENT', use_container_width=True)
    st.line_chart(data=year_df, x='FILM_YEAR', y=['MEAN_RATING', 'MY_MEAN_RATING'], use_container_width=True)
    st.dataframe(year_df, hide_index=True)
    st.plotly_chart(st.session_state['dfs']['year_scatter'], theme='streamlit', use_container_width=True)
    year_selection = st.selectbox('Select a Year:', np.sort(tmp_df['FILM_YEAR'].unique()))
    algo_features_df_year = algo_features_df[algo_features_df['FILM_YEAR'] == year_selection].reset_index(drop=True)
    algo_features_df_year_x = dataframe_explorer(algo_features_df_year)
    st.dataframe(algo_features_df_year_x, use_container_width=True, hide_index=True)
        
with genre_tab:
    st.plotly_chart(st.session_state['dfs']['genre_bar'], theme='streamlit', use_container_width=True)
    st.dataframe(genre_df, hide_index=True)
    st.plotly_chart(st.session_state['dfs']['genre_scatter'], theme='streamlit', use_container_width=True)

with keyword_tab:
    st.dataframe(keyword_df, hide_index=True)
    keyword = st.selectbox('Enter Keyword:', keyword_df['KEYWORD'].unique())
    keyword_df_filtered = keyword_film_level_df[keyword_film_level_df['KEYWORD'] == keyword]
    st.dataframe(keyword_df_filtered, hide_index=True, height=600)

with director_tab:
    # st.plotly_chart(st.session_state['dfs']['director_watched_bar'], theme='streamlit', use_container_width=True)
    # st.plotly_chart(st.session_state['dfs']['director_rated_bar'], theme='streamlit', use_container_width=True)
    st.dataframe(director_df, hide_index=True)
    st.dataframe(director_topfive_df.drop('PERSON_ID', axis=1), hide_index=True)
    # st.plotly_chart(st.session_state['dfs']['director_scatter'], theme='streamlit', use_container_width=True)
    # st.plotly_chart(st.session_state['dfs']['director_watch_rate_scatter'], theme='streamlit', use_container_width=True)
    director_name = st.selectbox('Enter Director:', director_df['DIRECTOR_NAME'].unique())
    director_df_filtered = director_film_level_df[director_film_level_df['DIRECTOR_NAME'] == director_name]
    director_df_filtered['my_rating_vs_letterboxd_mean'] = director_df_filtered['FILM_RATING_SCALED'] - director_df_filtered['FILM_RATING']
    st.dataframe(director_df_filtered, hide_index=True, height=600)
    director_df_filtered_reshaped = pd.melt(director_df_filtered, id_vars=['FILM_TITLE', 'DIRECTOR_NAME'], value_vars=['FILM_RATING', 'FILM_RATING_SCALED'], var_name='RATING_TYPE', value_name='RATING')
    st.altair_chart(alt.Chart(director_df_filtered_reshaped).mark_line().encode(x=alt.X('FILM_TITLE', sort=None), y='RATING', color='RATING_TYPE'), use_container_width=True)
    st.altair_chart(alt.Chart(director_sparklines_df).mark_line().encode(x='LETTERBOXD_DAYS', y='CUMULATIVE_FILMS', color='DIRECTOR_NAME'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='DAYS_SINCE_DEBUT', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='AGE_IN_DAYS', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)
    st.altair_chart(alt.Chart(director_debut_df).mark_line().encode(x='FILM_RELEASE_DATE', y='FILM_NUMBER', color='DIRECTOR_NAME'), use_container_width=True)

with actor_tab:
    st.plotly_chart(st.session_state['dfs']['actor_hist'], theme='streamlit', use_container_width=True)
    st.plotly_chart(st.session_state['dfs']['actor_watched_bar'], theme='streamlit', use_container_width=True)
    st.plotly_chart(st.session_state['dfs']['actor_rated_bar'], theme='streamlit', use_container_width=True)
    st.dataframe(actor_df, hide_index=True)
    # st.plotly_chart(st.session_state['dfs']['actor_scatter'], theme='streamlit', use_container_width=True)
    # st.plotly_chart(st.session_state['dfs']['actor_watch_rate_scatter'], theme='streamlit', use_container_width=True)
    display_actor_dash = False
    actor_name = st.text_input('Enter Actor:')
    tmp_df = select_statement_to_df('SELECT PERSON_ID, PERSON_NAME FROM PERSON_INFO WHERE REPLACE(PERSON_NAME, ".", "") LIKE "%{}%"'.format(actor_name))
    person_id_lookup_dict = {person_name:person_id for person_name, person_id in zip(tmp_df['PERSON_NAME'], tmp_df['PERSON_ID'])}
    if 0 < len(tmp_df) <= 20: display_actor_dash = True
    if display_actor_dash:
        selected_actor = st.radio('Select Actor:', tmp_df['PERSON_NAME'])
        actor_df_filtered = actor_film_level_df[actor_film_level_df['PERSON_ID'] == person_id_lookup_dict.get(selected_actor)].drop('PERSON_ID', axis=1)
        st.dataframe(actor_df_filtered, hide_index=True, height=600)
        st.line_chart(data=actor_df_filtered, x="FILM_TITLE", y=["FILM_RATING", "FILM_RATING_SCALED"])
    st.altair_chart(st.session_state['dfs']['actor_days_since_debut_altchart'], use_container_width=True)
    st.altair_chart(st.session_state['dfs']['actor_age_in_days_altchart'], use_container_width=True)
    st.altair_chart(st.session_state['dfs']['actor_letterboxd_days_altchart'], use_container_width=True)

with collections_tab:
    st.dataframe(collection_df, use_container_width=True, hide_index=True)
    st.dataframe(collections_close_to_completion_df, hide_index=True, height=600)
    collection_name = st.selectbox('Enter Collection:', collection_df['COLLECTION_NAME'].unique())
    collection_df_filtered = collection_film_level_df[collection_film_level_df['COLLECTION_NAME'] == collection_name]
    collection_df_filtered = collection_df_filtered.drop('COLLECTION_NAME', axis=1)
    st.dataframe(collection_df_filtered, hide_index=True, height=600)

with filmid_lookup_tab:
    film_search = st.text_input('Enter Film Name or ID:')
    film_id_df = select_statement_to_df('SELECT * FROM FILM_TITLE WHERE (FILM_ID LIKE "%{}%") OR (FILM_TITLE LIKE "%{}%")'.format(film_search, film_search))
    if len(film_id_df) < 50:
        st.dataframe(film_id_df, hide_index=True)

with diagnostics_tab:
    st.dataframe(diagnostics_df)