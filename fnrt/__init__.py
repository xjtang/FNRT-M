""" Module for Fusion Near Real-Time
"""


import logging
from .process import fnrt_train_landsat


log_format = '|%(asctime)s|%(levelname)s|%(module)s|%(lineno)s||%(message)s'
log_formatter = logging.Formatter(log_format,'%Y-%m-%d %H:%M:%S')
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.INFO)

log = logging.getLogger('FNRT-M')
log.addHandler(log_handler)
log.setLevel(logging.INFO)


__all__ = [
    'log',
    'fnrt_train_landsat'
]


# End
