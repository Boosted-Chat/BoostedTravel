# wsgi.py — Gunicorn entrypoint
# Cloud Run defaults to 8080, gunicorn loads wsgi:app from here.
from main import app
