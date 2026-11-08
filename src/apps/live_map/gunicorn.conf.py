"""Gunicorn settings for the live map on Databricks Apps.

One worker process, so every viewer shares the same query cache (one warehouse query per refresh
interval); threads serve viewers in parallel. Databricks Apps gives the port in DATABRICKS_APP_PORT.
"""

import os

bind = f"0.0.0.0:{os.environ.get('DATABRICKS_APP_PORT', '8000')}"
workers = 1
threads = int(os.environ.get("GUNICORN_THREADS", "8"))
timeout = 60
accesslog = "-"
