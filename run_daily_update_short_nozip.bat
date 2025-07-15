@REM cd C:/Users/tomdevine/desktop/dev/PersonalProjects/letterboxd-app
call letterboxd-env/scripts/activate
cd watchlist_toolkit/data_prep
cmd /k python daily_update_short.py nozip