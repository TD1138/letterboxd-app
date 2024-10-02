@REM cd C:/Users/tomdevine/desktop/dev/PersonalProjects/letterboxd-app
call letterboxd-env/scripts/activate
cd lib/streamlit
streamlit run grid_app.py --server.port 8515