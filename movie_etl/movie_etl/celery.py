from celery import Celery
from celery.schedules import crontab
import os
import logging


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'movie_etl.settings')

app = Celery('movie_etl')
app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks(['etl.tasks'])

# app.conf.update(
#     task_default_queue='default',
#     worker_log_format="%(asctime)s [%(levelname)s]: %(message)s",
#     worker_task_log_format="%(asctime)s [%(levelname)s] %(task_name)s - %(message)s"
# )


# logging.basicConfig(
#     filename='celery_task.log',
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s]: %(message)s'
# )


# periodic tasks
# app.conf.beat_schedule = {
#     'fetch-new-movies': {
#         'task': 'etl.tasks.fetch_new_movies',
#         'schedule': crontab(hour=0, minute=0),  # Daily at midnight
#     },
# }