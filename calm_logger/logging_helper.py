#!/usr/bin/python3.6
import logging


def setup_logging(name, default_level=logging.INFO):
    """
    Setup logging configuration
    """
    sh = logging.StreamHandler()
    logger = logging.getLogger(name)
    logger.setLevel(default_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

# we do not currently use BugSnag
# import bugsnag
# from bugsnag.handlers import BugsnagHandler

# BUGSNAG_API_KEY = os.environ.get('BUGSNAG_API_KEY')
# def add_bugsnag_handler(logger, level=logging.ERROR, api_key=BUGSNAG_API_KEY):
#     if not BUGSNAG_API_KEY:
#         raise ValueError('BUGSNAG_API_KEY is missing.  Make sure it is an environment variable')
#     bugsnag.configure(api_key=api_key)
#     handler = BugsnagHandler()
#     #send only ERROR-level logs and above
#     handler.setLevel(level)
#     logger.addHandler(handler)
#     return logger
