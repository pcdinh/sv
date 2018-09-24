# -*- coding: utf-8 -*-

from vibora import Vibora
from vibora.hooks import Events
import os
import logging

LOGGER_FORMAT = '%(asctime)s %(message)s'
logger = logging.getLogger("app")
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)

app = Vibora()


@app.handle(Events.BEFORE_SERVER_START)
async def initialize_engine(current_app: Vibora):
    # Register the config instance.
    from revopy import initialize_app
    await initialize_app(
        current_app,
        os.path.dirname(os.path.realpath(__file__)),
        os.environ.get("ENV_NAME", None) or "dev1",
        'apps/config/settings.py',
        'apps/config/settings_{}.py'
    )
