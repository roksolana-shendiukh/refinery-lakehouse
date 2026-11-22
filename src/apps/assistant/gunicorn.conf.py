"""Gunicorn settings for the assistant on Databricks Apps.

One worker process with threads: an answer takes about 5-10 seconds, mostly waiting for the model, so
threads serve several people asking at once. Databricks Apps gives the port in DATABRICKS_APP_PORT.

The Databricks Apps proxy forwards large headers (the viewer's access token and workspace cookies);
gunicorn rejects headers over 8 KB by default, so the limits are raised.
"""

import os

bind = f"0.0.0.0:{os.environ.get('DATABRICKS_APP_PORT', '8000')}"
workers = 1
threads = int(os.environ.get("GUNICORN_THREADS", "8"))
timeout = 120  # up to three model calls and two searches per question
accesslog = "-"

limit_request_field_size = 65536  # bytes per header (default 8190)
limit_request_fields = 200  # number of headers (default 100)
