""" Module for model parameters
"""


import numpy as np


defaults = {
    # planetary computer
    'CATALOG' = 'https://planetarycomputer-staging.microsoft.com/api/stac/v1',
    #'CATALOG' = 'https://planetarycomputer.microsoft.com/api/stac/v1',
    'LANDSAT' = 'landsat-c2-l2',

    # study time periods
    'TRAIN_START': '2019-01-01',
    'TRAIN_END': '2021-12-31',
    'MONITOR_START': '2022-01-01',
    'MONITOR_END': '2022-12-31',

    # near real-time parameters
    'N': 5,
    'K': 4,

    # other common parameters
    'RES': 30,
    'SCALE_FACTOR': 10000,
    'DAYS_IN_YEAR': 365.25,

    # Landsat parameters
    'LST': {
        'BAND': 'NDFI',
        'Z': 2,
        'C': 0,
        'D': 1,
        'C_ONLY': False,
        'MIN_STD': 0.05,
        'RES': 30},

    # Sentinel-2 parameters
    'S2': {
        'BAND': 'NDFI',
        'Z': 2,
        'C': 3,
        'D': 1,
        'C_ONLY': False,
        'MIN_STD': 0.05,
        'RES': 10},

    # Sentinel-1 parameters
    'S1': {
        'BAND': 'VV',
        'Z': 2,
        'C': 0,
        'D': 1,
        'C_ONLY': True,
        'MIN_STD': 0.01,
        'RES': 10},

    # endmembers
    'UNMIX': {
        'ENDMEMBERS': np.array([[500, 900, 400, 6100, 3000, 1000],
                                [1400, 1700, 2200, 3000, 5500, 3000],
                                [2000, 3000, 3400, 5800, 6000, 5800],
                                [0, 0, 0, 0, 0, 0],
                                [9000, 9600, 8000, 7800, 7200, 6500]],
                                dtype=np.int16),
        'CF_THRESHOLD': 0.2
    }
}


# End
