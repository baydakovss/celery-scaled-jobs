FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV C_FORCE_ROOT=1

RUN pip install --no-cache-dir celery[redis]==5.6.0

WORKDIR /app
COPY app.py .

CMD ["celery", "-A", "app", "worker", "-Q", "test", "-c", "1", "-l", "debug", "--without-gossip", "--disable-prefetch", "--max-tasks-per-child=1"]
