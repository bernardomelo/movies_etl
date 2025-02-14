# Movie ETL - Project Documentation

# Overview
The Movie ETL project is an Extract, Transform, and Load (ETL) pipeline for movie data. It extracts data from the OMDb API, processes and transforms the data, and loads it into a database.

# Project Structure
etl_challenge/
│── movie_etl/                     # Main Django project folder
│   ├── etl/                        # ETL pipeline logic
│   │   ├── extractors/              # Data extraction modules
│   │   │   ├── omdb.py              # Extracts movie data from OMDb API
│   │   ├── loaders/                 # Data loading modules
│   │   │   ├── movie_loader.py      # Loads data into the database
│   │   ├── transformers/            # Data transformation modules
│   │   │   ├── movie_transformer.py # Processes and normalizes movie data
│   │   ├── tasks/                    # Celery tasks for ETL jobs
│   │   │   ├── etl_tasks.py         # High-level ETL task orchestration
│   │   │   ├── extractor_tasks.py   # Handles extraction jobs
│   │   │   ├── transform_tasks.py   # Handles transformation jobs
│   │   │   ├── load_tasks.py        # Handles loading jobs
│   ├── management/                  # Django management commands
│   │   ├── commands/
│   │   │   ├── run_etl.py           # CLI command to run ETL
│   ├── models/                      # Django models for database
│   ├── migrations/                   # Database migrations
│   ├── movie_etl/                   # Django project configuration
│   │   ├── settings.py               # Django settings
│   │   ├── urls.py                   # Django URL routing
│   │   ├── views.py                  # Django views (if applicable)
│   │   ├── admin.py                  # Django admin configurations
│   ├── db.sqlite3                    # SQLite database
│   ├── manage.py                      # Django management script
│   ├── requirements.txt               # Python dependencies
│── venv/                              # Virtual environment (ignored in production)
│── movie_etl.rar                      # Compressed archive of the project

# Installation
Prerequisites
Python 3.x
Django
Celery
SQLite (or any other DB backend)

# Setup Steps
Clone the repository:
git clone https://github.com/your-repo/movie_etl.git
cd movie_etl

# Create and activate a virtual environment:
python -m venv venv
source venv/bin/activate   # On macOS/Linux
venv\Scripts\activate      # On Windows

# Install dependencies:
pip install -r requirements.txt

# Apply database migrations:
python manage.py migrate

# Run the ETL process:
python manage.py run_etl

# Usage
Running the ETL Process
The ETL pipeline is managed by Django's command system. To start it manually:
python manage.py run_etl --years 2010 2011

# Celery Tasks
The ETL is structured to work asynchronously using Celery. To start the Celery worker:
celery -A movie_etl worker --loglevel=info --pool=solo(See Celery documentation for concurrency start)

# Configuration
Update settings.py to modify database configurations, API keys, and Celery broker settings.

# Modules Breakdown
Extractors (etl/extractors/)
omdb.py: Fetches movie data from OMDb API.

# Transformers (etl/transformers/)
movie_transformer.py: Cleans, normalizes, and processes extracted movie data.

# Loaders (etl/loaders/)
movie_loader.py: Loads transformed data into the database.

# Tasks (etl/tasks/)
extractor_tasks.py: Handles data extraction jobs.
transform_tasks.py: Handles transformation steps.
load_tasks.py: Handles data loading.

# Management Commands (management/commands/)
run_etl.py: Custom Django management command to trigger the ETL process.

# Future Improvements
- Implement logging and error handling.
- Migrate from SQLite to PostgreSQL/SQLServer.
- Optimize ETL performance using batch processing.
