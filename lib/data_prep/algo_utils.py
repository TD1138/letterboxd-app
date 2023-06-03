import pandas as pd
from xgboost import XGBRegressor
from sqlite_utils import select_statement_to_df, df_to_table

eligible_watchlist_query = """

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
    
    , STREAMING_CONCAT AS (
    
      SELECT
        
        FILM_ID
        ,GROUP_CONCAT(STREAMING_SERVICE_FULL, ', ') AS STREAMING_SERVICES
        ,MIN(CASE WHEN STREAMING_SERVICE_ABBR = 'rent' THEN PRICE END) AS MIN_RENTAL_PRICE
      
      FROM FILM_STREAMING_SERVICES
      
      GROUP BY FILM_ID
    
    )
   
    SELECT

      a.FILM_ID
      ,b.FILM_TITLE
      ,b.LETTERBOXD_URL
      ,c.FILM_WATCH_COUNT
      ,c.FILM_TOP_250
      ,c.FILM_RATING
      ,c.FILM_LIKES_COUNT
      ,c.FILM_FAN_COUNT
      ,CASE WHEN d.FILM_ID IS NULL THEN 'No' ELSE 'Yes' END AS STREAMING
      ,h.STREAMING_SERVICES
      ,CASE WHEN h.MIN_RENTAL_PRICE IS NULL THEN 'No' ELSE 'Yes' END AS RENTABLE
      ,CASE WHEN d.FILM_ID IS NOT NULL OR h.MIN_RENTAL_PRICE IS NOT NULL THEN 'Yes' ELSE 'No' END AS WATCHABLE
      ,h.MIN_RENTAL_PRICE
      ,e.FILM_RUNTIME
      ,i.FILM_DECADE
      ,i.FILM_YEAR
      ,f.FILM_GENRE
      ,COALESCE(g.VARIANCE_SCORE, 0) AS GENRE_SCORE
    
    FROM ALL_FEATURE_FILMS a
    LEFT JOIN FILM_TITLE b
    ON a.FILM_ID = b.FILM_ID
    LEFT JOIN FILM_LETTERBOXD_STATS c
    ON a.FILM_ID = c.FILM_ID
    LEFT JOIN FILMS_AVAILABLE_TO_STREAM d
    ON a.FILM_ID = d.FILM_ID
    LEFT JOIN FILM_RUNTIME e
    ON a.FILM_ID = e.FILM_ID
    LEFT JOIN FILM_GENRE f
    ON a.FILM_ID = f.FILM_ID
    LEFT JOIN GENRE_SCORE g
    ON f.FILM_GENRE = g.FILM_GENRE
    LEFT JOIN STREAMING_CONCAT h
    ON a.FILM_ID = h.FILM_ID
    LEFT JOIN FILM_YEAR i
    ON a.FILM_ID = i.FILM_ID
    LEFT JOIN FILM_COLLECTIONS_VALID j
    ON a.FILM_ID = j.FILM_ID
    LEFT JOIN COLLECTION_STATS k
    ON j.COLLECTION_ID = k.COLLECTION_ID
    
    WHERE COALESCE(j.COLLECTION_NUM, 0) <= COALESCE(k.MAX_WATCHED, 0) + 1;

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
    AND SCALER >= 0.2
    AND MY_RATING_COUNT >= 3
    --ORDER BY MEAN_RATING DESC
    --ORDER BY KEYWORD_COUNT DESC
    --ORDER BY MY_VARIANCE DESC
    --ORDER BY VARIANCE_SCORE DESC
    --ORDER BY MY_MEAN_RATING DESC
    --ORDER BY VARIANCE_SCORE DESC
)

SELECT
    a.FILM_ID
    ,a.KEYWORD_ID
    ,b.KEYWORD
    
FROM FILM_KEYWORDS a
LEFT JOIN SCORE_TABLE b
ON a.KEYWORD_ID = b.KEYWORD_ID

WHERE b.KEYWORD_ID IS NOT NULL

"""

my_rating_query = """

    SELECT
         FILM_ID
        ,FILM_RATING_SCALED
    FROM PERSONAL_RATING

"""

director_rating_query = """

WITH BASE_TABLE AS (

    SELECT

        a.FILM_ID
        ,d.FILM_TITLE
        ,b.PERSON_ID
        ,e.PERSON_NAME AS DIRECTOR_NAME
        ,CASE WHEN c.FILM_ID IS NULL THEN 0 ELSE 1 END AS WATCHED
        ,f.FILM_RATING_SCALED
        ,CASE WHEN f.FILM_RATING_SCALED IS NULL THEN 0 ELSE 1 END AS RATED

    FROM ALL_FEATURE_FILMS a

    LEFT JOIN FILM_CREW b
    ON a.FILM_ID = b.FILM_ID

    LEFT JOIN WATCHED c
    ON a.FILM_ID = c.FILM_ID

    LEFT JOIN FILM_TITLE d
    ON a.FILM_ID = d.FILM_ID

    LEFT JOIN PERSON_INFO e
    ON b.PERSON_ID = e.PERSON_ID

    LEFT JOIN PERSONAL_RATING f
    ON a.FILM_ID = f.FILM_ID

    WHERE b.JOB = 'Director'
        
    )
      
, DIRECTOR_RATINGS AS (

	SELECT
      
       PERSON_ID
	  ,DIRECTOR_NAME
      ,COUNT(*) AS TOTAL_FILMS
      ,SUM(WATCHED) AS FILMS_WATCHED
      ,AVG(WATCHED) AS PERCENT_WATCHED
      ,AVG(FILM_RATING_SCALED) AS MEAN_RATING
      ,SUM(RATED) AS FILMS_RATED
      ,AVG(RATED) AS PERCENT_RATED
      
    FROM BASE_TABLE
    
    GROUP BY PERSON_ID, DIRECTOR_NAME

    HAVING TOTAL_FILMS >= 3
    AND FILMS_WATCHED > 1
    AND FILMS_RATED > 1
    AND MEAN_RATING NOT NULL
    AND PERCENT_RATED >= .2
    
 	)
, DIRECTOR_WATCH_STATS AS (

	SELECT
      
       PERSON_ID
	  ,DIRECTOR_NAME
      ,COUNT(*) AS TOTAL_FILMS
      ,AVG(WATCHED) AS PERCENT_WATCHED
      
    FROM BASE_TABLE
    
    GROUP BY PERSON_ID, DIRECTOR_NAME
    
 	)

, MEAN_RATING AS ( SELECT AVG(MEAN_RATING)AS MEAN_TOTAL_RATING FROM DIRECTOR_RATINGS )

, FILM_DIRECTOR_LEVEL AS (

	SELECT
		
		 a.FILM_ID
		,a.FILM_TITLE
		,a.PERSON_ID
		,a.DIRECTOR_NAME
		,COALESCE(b.MEAN_RATING, (SELECT MEAN_TOTAL_RATING FROM MEAN_RATING)) AS DIRECTOR_MEAN_RATING
		,COALESCE(c.TOTAL_FILMS, 0) AS DIRECTOR_TOTAL_FILMS
		,COALESCE(c.PERCENT_WATCHED, 0) AS DIRECTOR_PERCENT_WATCHED
		
	 FROM BASE_TABLE a
	 LEFT JOIN DIRECTOR_RATINGS b 
	 ON a.PERSON_ID = b.PERSON_ID
	 LEFT JOIN DIRECTOR_WATCH_STATS c
	 ON a.PERSON_ID = c.PERSON_ID
	 
	 )
	 
SELECT
	
	 FILM_ID
	,AVG(DIRECTOR_MEAN_RATING) AS DIRECTOR_MEAN_RATING
	,AVG(DIRECTOR_TOTAL_FILMS) AS DIRECTOR_TOTAL_FILMS
	,AVG(DIRECTOR_PERCENT_WATCHED) AS DIRECTOR_PERCENT_WATCHED

FROM FILM_DIRECTOR_LEVEL

GROUP BY FILM_ID, FILM_TITLE

"""

def scale_col(df, column, suffix='', a=0, b=1):
    col_min = df[column].min()
    col_max = df[column].max()
    col_range = (col_max - col_min)
    df[column+suffix] = ((df[column] - col_min) / col_range) * (b - a) + a
    return df

def run_algo():

    eligible_watchlist_df = select_statement_to_df(eligible_watchlist_query)

    director_rating_df = select_statement_to_df(director_rating_query)

    eligible_watchlist_df = eligible_watchlist_df.merge(director_rating_df, how='left', on='FILM_ID')

    genre_df = eligible_watchlist_df.copy()[['FILM_ID', 'FILM_GENRE']]
    genre_df['COUNT'] = 1
    genre_df_wide = pd.pivot_table(genre_df, values='COUNT', index=['FILM_ID'], columns=['FILM_GENRE']).fillna(0).reset_index()

    eligible_watchlist_df = eligible_watchlist_df[['FILM_ID', 'FILM_TITLE', 'FILM_WATCH_COUNT', 'FILM_TOP_250', 'FILM_RATING', 'FILM_LIKES_COUNT', 'FILM_FAN_COUNT', 'FILM_RUNTIME', 'FILM_YEAR', 'DIRECTOR_MEAN_RATING', 'DIRECTOR_TOTAL_FILMS', 'DIRECTOR_PERCENT_WATCHED']].merge(genre_df_wide, how='left', on='FILM_ID')

    keyword_df = select_statement_to_df(keyword_query)
    keyword_df['COUNT'] = 1
    keyword_df_wide = pd.pivot_table(keyword_df, values='COUNT', index=['FILM_ID'], columns=['KEYWORD']).fillna(0).reset_index()

    eligible_watchlist_df = eligible_watchlist_df.merge(keyword_df_wide, how='left', on='FILM_ID')

    eligible_watchlist_df['FILM_TOP_250'] = eligible_watchlist_df['FILM_TOP_250'].fillna(266)
    eligible_watchlist_df['FILM_RATING'] = eligible_watchlist_df['FILM_RATING'].fillna(2.0)
    eligible_watchlist_df = eligible_watchlist_df.fillna(0)

    my_rating_df = select_statement_to_df(my_rating_query)

    rating_features_df = eligible_watchlist_df.merge(my_rating_df, how='left', on='FILM_ID')

    rated_features = rating_features_df[rating_features_df['FILM_RATING_SCALED'].notnull()].reset_index(drop=True)
    unrated_features = rating_features_df[rating_features_df['FILM_RATING_SCALED'].isnull()].reset_index(drop=True)

    non_features = ['FILM_ID',
                    'FILM_TITLE',
                    'FILM_RATING_SCALED',
                    'FILM_TOP_250',
                    'FILM_RUNTIME'
                    ]


    model_features = [x for x in unrated_features.columns if x not in non_features]

    delete_cols = []
    for col in model_features:
        col_mean = rated_features[col].mean()
        if col_mean <= .01:
            delete_cols.append(col)
    model_features = [x for x in model_features if x not in delete_cols]

    target = ['FILM_RATING_SCALED']

    X_train = rated_features[model_features]
    y_train = rated_features[target]

    xgb_model = XGBRegressor()
    xgb_model.fit(X_train, y_train)

    X_pred = unrated_features[model_features]
    pred_df = unrated_features.copy()
    pred_df['ALGO_SCORE'] = xgb_model.predict(X_pred)

    output_df = pred_df.copy()[['FILM_ID', 'ALGO_SCORE']]
    output_df = scale_col(output_df, 'ALGO_SCORE')

    df_to_table(output_df, 'FILM_ALGO_SCORE', replace_append='replace')