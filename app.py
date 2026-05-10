import os
import time
import logging

from celery import Celery

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Celery('test-worker', broker=os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379/0'))


@app.task
def long_term_sleep_task(minutes_to_sleep: int = 1):
    counter = 0
    total = minutes_to_sleep * 60
    while counter < total:
        time.sleep(1)
        counter += 1
        logger.info(f'Counter {counter} of {total}')
    return f'Done sleeping {minutes_to_sleep} minutes'
