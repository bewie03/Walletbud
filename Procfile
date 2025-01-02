release: python -c "from database import init_db_sync; init_db_sync()"
web: gunicorn wsgi:get_app --worker-class aiohttp.GunicornWebWorker --workers=2 --threads=4 --timeout=120 --keep-alive=75 --backlog=2048 --max-requests=10000 --max-requests-jitter=1000 --graceful-timeout=60 --log-file - --log-level info --access-logfile - --error-logfile - --capture-output --preload
maintenance: python -m database_maintenance
