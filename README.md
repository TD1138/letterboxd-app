# Letterboxd Data Explorer & Watch-List Prioritiser

A full end-to-end pipeline that ingests your personal [Letterboxd](https://letterboxd.com/) export, enriches it with data from TMDb & JustWatch, trains a machine-learning model to rank films on your watch-list, and serves interactive dashboards via Streamlit.

---

## 🎯 Key Features

* **Automated ETL** – Selenium downloads your latest Letterboxd export, unzips it, refreshes core tables and uploads a backup to Google Cloud Storage.
* **Rich Data Model** – SQLite database with > 30 tables covering films, ratings, genres, crew, keywords, streaming availability and more.
* **ML-Powered Recommendations** – XGBoost model generates an `ALGO_SCORE` for every film and SHAP values for explainability.
* **Streamlit Dashboards** – Explore an ordered watch-list, stats, year / genre / actor completion and diagnostics from your browser.
* **Daily Update Script** – A single entry-point (`daily_update.py`) keeps everything in sync; ideal for a cron job or Windows Task Scheduler.

---

## 🗺️ Repository Layout

```
letterboxd-app/
├── lib/               # All Python source code
│   ├── data_prep/     # ETL, enrichment, ML, utility modules
│   ├── elo/           # (Future) Elo-style ranking helpers
│   └── streamlit/     # Streamlit apps & pre-computed query YAML
├── db/                # SQLite database & raw export cache (ignored by git)
├── creds/             # OAuth / API credentials (ignored by git)
├── notebooks/         # Jupyter notebooks for exploration (optional)
├── requirements.txt   # Python dependencies
└── README.md          # You are here 🌟
```

---

## 🚀 Quick Start

1. **Clone the repo**
   ```bash
   git clone https://github.com/your-handle/letterboxd-app.git
   cd letterboxd-app
   ```

2. **Create & activate a virtual environment** (Python ≥ 3.10 recommended)
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Copy the environment template and fill in your secrets**
   ```bash
   cp template.env .env  # or copy manually on Windows
   # then edit .env with your Letterboxd credentials, GCP bucket, etc.
   ```

5. **Run the initial update** (downloads export, builds DB, trains model, precalculates views)
   ```bash
   python lib/data_prep/daily_update.py
   ```

6. **Launch the Streamlit app**
   ```bash
   streamlit run lib/streamlit/LetterboxdApp.py
   ```
   Open the local URL shown in the terminal (default: http://localhost:8501).

---

## 🔧 Configuration – `.env`

The project relies on several environment variables. **Do not commit real secrets to git!**

| Variable | Description |
|----------|-------------|
| `PROJECT_PATH` | Absolute path to the project root (used for download dirs) |
| `LETTERBOXD_USER` / `LETTERBOXD_PASSWORD` | Your Letterboxd login credentials |
| `LETTERBOXD_SETTINGS_PAGE` | Usually `https://letterboxd.com/settings/data/` |
| `LETTERBOXD_EXPORT_LINK` | The direct CSV export link (found on settings page) |
| `GCP_BUCKET` | Name of the Google Cloud Storage bucket for DB backups |
| … | plus any TMDb / JustWatch API keys you add |

See `template.env` for the full list.

---

## 💡 Common Commands

| Task | Command |
|------|---------|
| Full daily update + model training | `python lib/data_prep/daily_update.py` |
| Skip the Selenium download step | `python lib/data_prep/daily_update.py nozip` |
| Run only the ML algorithm | `python lib/data_prep/algo_utils.py` (`run_algo()` entry) |
| Start dashboards | `streamlit run lib/streamlit/LetterboxdApp.py` |
| Test the watch-list grid prototype | `streamlit run watchlist_toolkit/streamlit/LetterboxdExplorerApp.py` |

---

## 🧠 Machine-Learning Overview

`lib/data_prep/algo_utils.py` builds an XGBoost regression model to predict how much you’ll like a film. Important points:

* Training data = your historical ratings (`PERSONAL_RATING`) joined to rich Letterboxd metadata.
* Features include popularity, runtime, genre dummies, director stats, keyword embeddings, etc.
* SHAP values are stored in `FILM_SHAP_VALUES` for transparent per-film explanations.

Feel free to tweak the feature list or swap in a different model – the rest of the app reads only the stored `ALGO_SCORE`.

---

## 🧑‍💻 Development Tips

* Run `ruff` or `black` for formatting (future CI).  
* Unit tests will live under `tests/` – pull requests adding coverage are welcome.
* Keep exploratory notebooks lightweight (clear outputs before commit).
* If you’re on Windows, ensure Chrome/Chromedriver are up-to-date (webdriver-manager handles most cases).

---

## 📜 License

Specify your preferred open-source license here (MIT by default):

```
MIT License
Copyright (c) 2024 …
```

---

## 🙏 Acknowledgements

* Letterboxd for the export feature and data.  
* [TMDb](https://www.themoviedb.org/) for supplementary metadata.  
* [JustWatch](https://www.justwatch.com/) for streaming availability.  
* XGBoost, SHAP, Streamlit and the wider open-source community.

Enjoy the films! 🍿