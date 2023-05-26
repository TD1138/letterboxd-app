import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from streamlit_extras.dataframe_explorer import dataframe_explorer
import sys
sys.path.insert(0, '../data_prep')
from sqlite_utils import select_statement_to_df

watchlist_query = """

WITH BASE_TABLE AS (

	SELECT
	
		 a.FILM_ID
		,c.FILM_TITLE
		,d.FILM_GENRE
		,e.FILM_RATING
		,f.FILM_RATING_SCALED
		,CASE WHEN f.FILM_RATING_SCALED IS NOT NULL THEN 1 ELSE 0 END AS RATED
	
	FROM ALL_FILMS a
	LEFT JOIN CONTENT_TYPE b
	ON a.FILM_ID = b.FILM_ID
	LEFT JOIN FILM_TITLE c
	ON a.FILM_ID = c.FILM_ID
	LEFT JOIN FILM_GENRE d
	ON a.FILM_ID = d.FILM_ID
	LEFT JOIN FILM_LETTERBOXD_STATS e
	ON a.FILM_ID = e.FILM_ID
	LEFT JOIN PERSONAL_RATING f
	ON a.FILM_ID = f.FILM_ID
	
	WHERE CONTENT_TYPE = 'movie'
	
	)
    
, GENRE_SCORE AS (

	SELECT

		 FILM_GENRE
		,AVG(FILM_RATING) AS MEAN_RATING
		,AVG(FILM_RATING_SCALED) AS MY_MEAN_RATING
		,AVG(FILM_RATING_SCALED) - AVG(FILM_RATING) AS MY_VARIANCE
		,((AVG(FILM_RATING_SCALED) - AVG(FILM_RATING)) * ((SUM(RATED)+0.0)/COUNT(*))) AS VARIANCE_SCORE
		,COUNT(*) AS FILM_COUNT
		,SUM(RATED) AS RATED_FILM_COUNT
		,(SUM(RATED)+0.0)/COUNT(*) AS SCALER

	FROM BASE_TABLE

	GROUP BY FILM_GENRE

)

SELECT
	 a.FILM_ID
	,b.FILM_TITLE
    ,b.LETTERBOXD_URL
    ,a.SEEN
	,c.FILM_WATCH_COUNT
	,c.FILM_TOP_250
	,c.FILM_RATING
	,c.FILM_LIKES_COUNT
	,c.FILM_FAN_COUNT
	,CASE WHEN d.FILM_ID IS NULL THEN 'No' ELSE 'Yes' END AS STREAMING
    ,j.STREAMING_SERVICES
    ,CASE WHEN j.MIN_RENTAL_PRICE IS NULL THEN 'No' ELSE 'Yes' END AS RENTABLE
    ,CASE WHEN d.FILM_ID IS NOT NULL OR j.MIN_RENTAL_PRICE IS NOT NULL THEN 'Yes' ELSE 'No' END AS WATCHABLE
    ,j.MIN_RENTAL_PRICE
	,e.FILM_RUNTIME
    ,k.FILM_DECADE
    ,k.FILM_YEAR
	,g.FILM_GENRE
    ,COALESCE(i.VARIANCE_SCORE, 0) AS GENRE_SCORE

FROM EXPANDED_WATCHLIST a
LEFT JOIN FILM_TITLE b
ON a.FILM_ID = b.FILM_ID
LEFT JOIN FILM_LETTERBOXD_STATS c
ON a.FILM_ID = c.FILM_ID
LEFT JOIN FILMS_AVAILABLE_TO_STREAM d
ON a.FILM_ID = d.FILM_ID
LEFT JOIN FILM_RUNTIME e
ON a.FILM_ID = e.FILM_ID
LEFT JOIN CONTENT_TYPE f
ON a.FILM_ID = f.FILM_ID
LEFT JOIN FILM_GENRE g
ON a.FILM_ID = g.FILM_ID
LEFT JOIN FILM_RELEASE_INFO h
ON a.FILM_ID = h.FILM_ID
LEFT JOIN GENRE_SCORE i
ON g.FILM_GENRE = i.FILM_GENRE
LEFT JOIN (
    SELECT
		 FILM_ID
		,GROUP_CONCAT(STREAMING_SERVICE_FULL, ', ') AS STREAMING_SERVICES
		,MIN(CASE WHEN STREAMING_SERVICE_ABBR = 'rent' THEN PRICE END) AS MIN_RENTAL_PRICE
	FROM FILM_STREAMING_SERVICES
	GROUP BY FILM_ID
    ) j
ON a.FILM_ID = j.FILM_ID
LEFT JOIN FILM_YEAR k
ON a.FILM_ID = k.FILM_ID
LEFT JOIN FILM_COLLECTIONS_VALID l
ON a.FILM_ID = l.FILM_ID
LEFT JOIN COLLECTION_STATS m
ON l.COLLECTION_ID = m.COLLECTION_ID

WHERE f.CONTENT_TYPE = 'movie'
AND h.FILM_STATUS = 'Released'
AND a.FILM_ID != 'f_08E3a'
AND e.FILM_RUNTIME < 300
AND e.FILM_RUNTIME > 59
AND g.FILM_GENRE != 'tv-movie'
AND g.FILM_GENRE != 'documentary'
AND l.COLLECTION_NUM <= m.MAX_WATCHED + 1

"""
year_completion_query = """

WITH BASE_TABLE AS (
	
	SELECT
		 a.FILM_ID
		,d.FILM_TITLE
		,b.FILM_YEAR
		,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED

	FROM ALL_RELEASED_FILMS a
	
	LEFT JOIN FILM_YEAR b
	ON a.FILM_ID = b.FILM_ID
	
	LEFT JOIN WATCHED c
	ON a.FILM_ID = c.FILM_ID
	
	LEFT JOIN FILM_TITLE d
	ON a.FILM_ID = d.FILM_ID

	)
	
SELECT
	
	 FILM_YEAR
    ,SUM(WATCHED) AS FILMS_WATCHED
	,COUNT(*) AS TOTAL_FILMS
	,AVG(WATCHED) AS PERCENT_WATCHED
	
FROM BASE_TABLE

GROUP BY FILM_YEAR
ORDER BY FILM_YEAR DESC

"""

genre_completion_query = """

WITH BASE_TABLE AS (
	
	SELECT
		 a.FILM_ID
		,d.FILM_TITLE
		,b.FILM_GENRE
		,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED

	FROM ALL_RELEASED_FILMS a
	
	LEFT JOIN FILM_GENRE b
	ON a.FILM_ID = b.FILM_ID
	
	LEFT JOIN WATCHED c
	ON a.FILM_ID = c.FILM_ID
	
	LEFT JOIN FILM_TITLE d
	ON a.FILM_ID = d.FILM_ID

    WHERE b.FILM_GENRE != 'studiogenre-films'
    
	)
	
SELECT
	
	 FILM_GENRE
    ,SUM(WATCHED) AS FILMS_WATCHED
	,COUNT(*) AS TOTAL_FILMS
	,AVG(WATCHED) AS PERCENT_WATCHED
	
FROM BASE_TABLE

GROUP BY FILM_GENRE
ORDER BY FILM_GENRE DESC

"""

director_completion_query = """

WITH BASE_TABLE AS (
	
	SELECT
		 a.FILM_ID
		,d.FILM_TITLE
		,b.FILM_GENRE
		,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED

	FROM ALL_RELEASED_FILMS a
	
	LEFT JOIN FILM_GENRE b
	ON a.FILM_ID = b.FILM_ID
	
	LEFT JOIN WATCHED c
	ON a.FILM_ID = c.FILM_ID
	
	LEFT JOIN FILM_TITLE d
	ON a.FILM_ID = d.FILM_ID

    WHERE b.FILM_GENRE != 'studiogenre-films'
    
	)
	
SELECT
	
	 FILM_GENRE
    ,SUM(WATCHED) AS FILMS_WATCHED
	,COUNT(*) AS TOTAL_FILMS
	,AVG(WATCHED) AS PERCENT_WATCHED
	
FROM BASE_TABLE

GROUP BY FILM_GENRE
ORDER BY FILM_GENRE DESC

"""

film_score_query = """

SELECT

	 a.FILM_ID
    ,b.FILM_TITLE
    ,f.FILM_GENRE
    ,g.FILM_WATCH_COUNT
    ,g.FILM_RATING
    ,ROUND(g.FILM_WATCH_COUNT * g.FILM_RATING * 100 / (2500000*5), 1) AS FILM_SCORE
    ,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED
    ,CASE WHEN d.FILM_ID IS NULL THEN 'No' ELSE 'Yes' END AS STREAMING
    ,e.STREAMING_SERVICES
    

FROM EXPANDED_WATCHLIST a

LEFT JOIN FILM_TITLE b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN WATCHED c
ON a.FILM_ID = c.FILM_ID

LEFT JOIN FILMS_AVAILABLE_TO_STREAM d
ON a.FILM_ID = d.FILM_ID

LEFT JOIN
	(SELECT FILM_ID, GROUP_CONCAT(STREAMING_SERVICE_FULL, ', ') AS STREAMING_SERVICES FROM FILM_STREAMING_SERVICES GROUP BY FILM_ID) e
ON a.FILM_ID = e.FILM_ID

LEFT JOIN FILM_GENRE f
ON a.FILM_ID = f.FILM_ID

LEFT JOIN FILM_LETTERBOXD_STATS g
ON a.FILM_ID = g.FILM_ID

WHERE b.FILM_TITLE IS NOT NULL

ORDER BY g.FILM_WATCH_COUNT DESC

"""


diary_query_basic = """

SELECT
	 
     WATCH_DATE
    ,SUM(IS_NARRATIVE_FEATURE) AS MOVIE_COUNT
    ,AVG(CASE WHEN IS_NARRATIVE_FEATURE = 1 THEN FILM_RATING END) AS MOVIE_RATING
    
FROM DIARY

GROUP BY WATCH_DATE

ORDER BY WATCH_DATE ASC

"""

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
film_length_min = int(df['FILM_RUNTIME'].min())
film_length_max = int(df['FILM_RUNTIME'].max())
film_length = st.slider('Film Runtime (mins):', film_length_min, film_length_max, (film_length_min, film_length_max))
df = df[(df['FILM_RUNTIME'] >= film_length[0]) & (df['FILM_RUNTIME'] <= film_length[1])]
decade_option = st.multiselect(
    'Decade:',
     list(df['FILM_DECADE'].unique()))
if decade_option:
    df = df[df['FILM_DECADE'].isin(decade_option)]
film_year_min = int(df['FILM_YEAR'].min())
film_year_max = int(df['FILM_YEAR'].max())
film_year = st.slider('Film Year:', film_year_min, film_year_max, (film_year_min, film_year_max))
df = df[(df['FILM_YEAR'] >= film_year[0]) & (df['FILM_YEAR'] <= film_year[1])]
option = st.multiselect(
    'Genre:',
     list(df['FILM_GENRE'].unique()))
if option:
    df = df[df['FILM_GENRE'].isin(option)]
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
    
df_scaled = scale_col(df_scaled, 'SCORE_WEIGHTED', a=0, b=100)
df2 = df.merge(df_scaled[['FILM_ID', 'SCORE_WEIGHTED']], how='left', on='FILM_ID')
df_sorted = df2.sort_values('SCORE_WEIGHTED', ascending=False)
# df_sorted['FILM_YEAR'] = df_sorted['FILM_YEAR'].dt.strftime("%Y")
year_df = select_statement_to_df(year_completion_query)
genre_df = select_statement_to_df(genre_completion_query)
film_score_df = select_statement_to_df(film_score_query)
diary_query_df = select_statement_to_df(diary_query_basic)
diary_query_df['WATCH_DATE'] = pd.to_datetime(diary_query_df['WATCH_DATE'])
date_range = pd.date_range(start=diary_query_df['WATCH_DATE'].min(), end=diary_query_df['WATCH_DATE'].max())
diary_query_df2 = diary_query_df.set_index('WATCH_DATE').reindex(date_range).fillna(0).rename_axis('WATCH_DATE').reset_index()
diary_query_df2['MOVIE_COUNT_ROLLING_7'] = diary_query_df2['MOVIE_COUNT'].rolling(window=7).mean()
diary_query_df2['MOVIE_COUNT_ROLLING_28'] = diary_query_df2['MOVIE_COUNT'].rolling(window=28).mean()
diary_query_df2['MOVIE_RATING_ROLLING_7'] = diary_query_df2['MOVIE_RATING'].rolling(window=7).mean()
diary_query_df2['MOVIE_RATING_ROLLING_28'] = diary_query_df2['MOVIE_RATING'].rolling(window=28).mean()

# st.write(diary_query_df2)
st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28"])
st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])
st.line_chart(data=diary_query_df2, x="WATCH_DATE", y=["MOVIE_COUNT_ROLLING_7", "MOVIE_COUNT_ROLLING_28", "MOVIE_RATING_ROLLING_7", "MOVIE_RATING_ROLLING_28"])

df_sorted_explore = dataframe_explorer(df_sorted, case=False)
st.dataframe(df_sorted_explore, use_container_width=True)

st.bar_chart(data=year_df, x='FILM_YEAR', y='PERCENT_WATCHED', use_container_width=True)
st.write(year_df)

st.bar_chart(data=genre_df, x='FILM_GENRE', y='PERCENT_WATCHED', use_container_width=True)
st.write(genre_df)

fig, ax = plt.subplots()
sns.scatterplot(data=df_sorted, x='FILM_RUNTIME', y='FILM_RATING', hue='SEEN')
st.pyplot(fig)

film_score_df_explore = dataframe_explorer(film_score_df, case=False)
st.dataframe(film_score_df_explore, use_container_width=True)