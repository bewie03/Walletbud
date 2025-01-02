release: python -c "from database import init_db_sync; init_db_sync()"
web: gunicorn wsgi:app --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0
maintenance: python -m database_maintenance
