release: python -c "from database import init_db_sync; init_db_sync()"
web: gunicorn wsgi:application --bind 0.0.0.0:$PORT --worker-class eventlet --workers 1 --threads 8 --timeout 0 --keep-alive 5 --log-level debug
maintenance: python -m database_maintenance
