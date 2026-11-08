"""Gunicorn settings for the live map on Databricks Apps.

One worker process, so every viewer shares the same query cache (one warehouse query per refresh
interval); threads serve viewers in parallel. Databricks Apps gives the port in DATABRICKS_APP_PORT.

The Databricks Apps proxy forwards large headers to the app (the viewer's access token and workspace
cookies). Gunicorn rejects any header over 8 KB by default ("Request Header Fields Too Large"), so the
header size limit is raised. The proxy in front of the app still authenticates every request.
"""

import os

bind = f"0.0.0.0:{os.environ.get('DATABRICKS_APP_PORT', '8000')}"
workers = 1
threads = int(os.environ.get("GUNICORN_THREADS", "8"))
timeout = 60
accesslog = "-"

limit_request_field_size = 65536  # bytes per header (default 8190)
limit_request_fields = 200  # number of headers (default 100)
