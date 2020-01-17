"""Simple pub/sub pattern implementation"""

import logging

from .server import Service

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
STH = logging.StreamHandler()
STH.setLevel(logging.DEBUG)
LOGGER.addHandler(STH)

FH = logging.FileHandler("apubsub.log")
FH.setLevel(logging.DEBUG)
LOGGER.addHandler(FH)
