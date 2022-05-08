""" Module for Fusion Near Real-Time
"""
import logging
from .compute import search_images, get_cluster, catalog
from .common import get_tile, get_bbox, lsma

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
    'get_tile',
    'get_bbox',
    'search_images',
    'get_cluster',
    'catalog',
    'lsma'
]
