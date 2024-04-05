@REM cd C:/Users/tomdevine/desktop/dev/PersonalProjects/letterboxd-app
call letterboxd-env/scripts/activate
cd lib/streamlit
streamlit run LetterboxdApp.py --server.port 8501 --server.baseUrlPath "LetterboxdApp"