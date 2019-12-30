import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
STH = logging.StreamHandler()
STH.setLevel(logging.DEBUG)
LOGGER.addHandler(STH)
