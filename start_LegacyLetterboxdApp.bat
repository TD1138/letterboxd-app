@REM cd C:/Users/tomdevine/desktop/dev/PersonalProjects/letterboxd-app
call letterboxd-env/scripts/activate
cd watchlist_toolkit/streamlit
streamlit run streamlit/LetterboxdApp.py --server.port 8509 --server.baseUrlPath "LegacyLetterboxdApp"