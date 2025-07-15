# import numpy as np
from random import choice, randint
from watchlist_toolkit.data_prep.sqlite_utils import select_statement_to_df, get_from_table

ranking_select_statement = """

SELECT

     a.FILM_ID
    ,c.FILM_TITLE
    ,a.FILM_RATING_SCALED
    ,b.FILM_POSITION
    
FROM PERSONAL_RATING a

LEFT JOIN PERSONAL_RANKING b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN FILM_TITLE c
ON a.FILM_ID = c.FILM_ID

"""

ranking_df = select_statement_to_df(ranking_select_statement)
all_ranked_film_ids = ranking_df['FILM_ID'].tolist()
max_rank = ranking_df['FILM_POSITION'].max()
min_rank = ranking_df['FILM_POSITION'].min()

film1_id = choice(all_ranked_film_ids)
film1_title = ranking_df.loc[ranking_df['FILM_ID']==film1_id, 'FILM_TITLE'].values[0]
film1_rank = ranking_df.loc[ranking_df['FILM_ID']==film1_id, 'FILM_POSITION'].values[0]
film1_rating = ranking_df.loc[ranking_df['FILM_ID']==film1_id, 'FILM_RATING_SCALED'].values[0]

min_displacement = 1
max_displacement = 50
random_rank_displacement_abs = randint(min_displacement, max_displacement)
if film1_rank == 1:
    random_sign = -1
elif film1_rank == ranking_df['FILM_POSITION'].max():
    random_sign = 1
else:
    random_sign = randint(0,1)*2-1
random_rank_displacement = random_rank_displacement_abs * random_sign
film2_rank = film1_rank + random_rank_displacement
film2_rank = min(film2_rank, max_rank)
film2_rank = max(film2_rank, min_rank)

film2_id = ranking_df.loc[ranking_df['FILM_POSITION']==film2_rank, 'FILM_ID'].values[0]
film2_title = ranking_df.loc[ranking_df['FILM_ID']==film2_id, 'FILM_TITLE'].values[0]
film2_rating = ranking_df.loc[ranking_df['FILM_ID']==film2_id, 'FILM_RATING_SCALED'].values[0]

print('random_film1={}, rank={}, rating={}, id={}'.format(film1_title, film1_rank, film1_rating, film1_id))
print('random_film2={}, rank={}, rating={}, id={}'.format(film2_title, film2_rank, film2_rating, film2_id))

