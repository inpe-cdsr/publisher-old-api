"""
Defines a structure component to run celery worker

Usage:

$ celery -A bdc_scripts.celery.worker:celery -l INFO -Q download
"""

# Python Native
import logging

# 3rdparty
from celery.signals import task_received, worker_shutdown
from celery.backends.database import Task
from celery.states import PENDING

# BDC Scripts
from publisher import create_app
from publisher.celery import create_celery_app


app = create_app()
celery = create_celery_app(app)
