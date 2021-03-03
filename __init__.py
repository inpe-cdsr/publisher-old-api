import os
from flask import Flask
from publisher import config, celery
from publisher.config import get_settings

def create_app(config_name='DevelopmentConfig'):
    """
    Creates Brazil Data Cube application from config object
    Args:
        config_name (string) Config instance name
    Returns:
        Flask Application with config instance scope
    """

    app = Flask(__name__)
    conf = config.get_settings(config_name)
    app.config.from_object(conf)

    with app.app_context():

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app
        import publisher.routes

    return app
