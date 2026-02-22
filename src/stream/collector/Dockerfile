FROM python:3.11-slim

WORKDIR /app

# requests is the only non-stdlib dependency; sqlite3 ships with Python
RUN pip install --no-cache-dir requests

# Copy only the collector script; logs dir and DB are mounted at runtime
COPY collect_streaming_data.py .

# data/streaming/ and logs/ are expected to be bind-mounted from the host
# so that collected data and logs survive container restarts
VOLUME ["/app/data", "/app/logs"]

ENV GOOGLE_AIR_QUALITY_API_KEY=""

CMD ["python", "-u", "collect_streaming_data.py"]
