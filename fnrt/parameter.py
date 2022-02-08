""" Module for model parameters
"""
defaults = {
    # study time periods
    'TRAIN_START': '2018-01-01',
    'TRAIN_END': '2020-12-31',
    'MONITOR_START': '2021-01-01',
    'MONITOR_END': '2021-12-31',

    # near real-time parameters
    'N': 5,
    'K': 4,

    # other common parameters
    'RES': 30,
    'SCALE_FACTOR': 0.0001

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
    'END_MEMBERS': {
        'GV': [500, 900, 400, 6100, 3000, 1000],
        'NPV': [1400, 1700, 2200, 3000, 5500, 3000],
        'SOIL': [2000, 3000, 3400, 5800, 6000, 5800],
        'SHADE': [0, 0, 0, 0, 0, 0],
        'CLOUD': [9000, 9600, 8000, 7800, 7200, 6500],
        'CF_THRESHOLD': 0.05
    }
}
