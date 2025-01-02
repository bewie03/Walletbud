release: python -c "from database import init_db_sync; init_db_sync()"
web: gunicorn wsgi:get_app --worker-class aiohttp.worker.GunicornWebWorker --log-file -
maintenance: python -m database_maintenance
