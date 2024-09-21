import streamlit as st
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

watchlist_statement = 'SELECT a.*, b.ALGO_SCORE FROM precomputed_watchlist a LEFT JOIN FILM_ALGO_SCORE b ON a.FILM_ID = b.FILM_ID ORDER BY b.ALGO_SCORE DESC'

def get_poster_path(film_id):
    poster_path = os.path.join(poster_dir, desensitise_case(film_id) + '.jpg')
    if not os.path.exists(poster_path):
        poster_path = os.path.join(poster_dir, 'f_00000.jpg')
    return poster_path

def display_film_details(film):
    st.title(film['FILM_TITLE'])
    col1, col2 = st.columns([1, 2])
    with col1:
        poster_path = get_poster_path(film['FILM_ID'])
        st.image(poster_path, use_column_width=True)
    with col2:
        st.write(f"**Year:** {film['FILM_YEAR']}")
        st.write(f"**Watch Count:** {film['FILM_WATCH_COUNT']}")
        st.write(f"**Rating:** {film['FILM_RATING']}")
        st.write(f"**Algorithm Score:** {film['ALGO_SCORE']:.1f}")
    
    if st.button("Back to Grid"):
        st.session_state.selected_film = None
        st.rerun()

def display_film_grid(films_df, rows=10, films_per_row=8):
    for i in range(0, rows, films_per_row):
        row_films = films_df.iloc[i:i+films_per_row]
        cols = st.columns(films_per_row)
        for j, (_, film) in enumerate(row_films.iterrows()):
            with cols[j]:
                poster_path = get_poster_path(film['FILM_ID'])
                st.image(poster_path, use_column_width=True)
                if st.button(f"**{film['FILM_TITLE']}**\n\n{film['FILM_YEAR']}\n\nAlgo Score: {film['ALGO_SCORE']:.2f}\n\nWatches: {film['FILM_WATCH_COUNT']:,}\n\nRating: {film['FILM_RATING']:.2f}",
                             key=f"film_{film['FILM_ID']}", use_container_width=True):
                    st.session_state.selected_film = film
                    st.rerun()

watchlist_df = select_statement_to_df(watchlist_statement)
# Streamlit app

# st.image(get_poster_path('f_01pyS'))
# st.image(get_poster_path('f_01PYs'))

pos0, pos1, pos2, pos3 = st.columns(4)
with pos0:
    streaming_filter = st.radio('Streaming:', ['Either', 'Yes', 'No'], horizontal=True)
    if streaming_filter != 'Either':
        watchlist_df = watchlist_df[watchlist_df['STREAMING'] == streaming_filter]
with pos1:
    seen_filter = st.radio('Seen', ['Either', 'Yes', 'No'], horizontal=True)
    if seen_filter == 'Yes':
        watchlist_df = watchlist_df[watchlist_df['SEEN'] == 1]
    elif seen_filter == 'No':
        watchlist_df = watchlist_df[watchlist_df['SEEN'] == 0]
with pos2:
    genre_filter = st.multiselect("Select Genre:", watchlist_df['FILM_GENRE'].unique())
    if genre_filter:
        watchlist_df = watchlist_df[watchlist_df['FILM_GENRE'].isin(genre_filter)]
with pos3:
    quant_film_lengths = {'Any': 999, '<90m': 90, '<1h40': 100, '<2h': 120, '<3h': 180}
    quant_length_filter = st.radio('Film Length:', quant_film_lengths.keys(), horizontal=True)
    watchlist_df = watchlist_df[watchlist_df['FILM_RUNTIME'] <= quant_film_lengths[quant_length_filter]]

if 'selected_film' not in st.session_state:
    st.session_state.selected_film = None

if st.session_state.selected_film is not None:
    display_film_details(st.session_state.selected_film)
else:
    # Initial display
    total_pages = display_film_grid(watchlist_df, FILMS_PER_PAGE, FILMS_PER_ROW)

    # Optional: Add sorting and filtering controls
    # sort_by = st.selectbox("Sort by", ["FILM_TITLE", "FILM_YEAR", "ALGO_SCORE"])
    # sort_order = st.radio("Sort order", ["Ascending", "Descending"])

    # Implement sorting logic
    # watchlist_df = watchlist_df.sort_values(by=sort_by, ascending=(sort_order == "Ascending"))
    # total_pages = display_films_with_grid(watchlist_df, 1, FILMS_PER_PAGE, COLS_PER_ROW)