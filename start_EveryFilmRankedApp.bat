@REM cd C:/Users/tomdevine/desktop/dev/PersonalProjects/letterboxd-app
call letterboxd-env/scripts/activate
cd watchlist_toolkit/streamlit
streamlit run EveryFilmRankedApp.py --server.port 8502 --server.baseUrlPath "EveryFilmRankedApp"