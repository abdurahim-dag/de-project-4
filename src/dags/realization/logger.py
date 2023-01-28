import logging


logger = logging.getLogger(__name__)

_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
logger.addHandler(_ch)