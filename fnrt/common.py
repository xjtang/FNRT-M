import numpy as np
import pysptools
import rasterio.features
import xrspatial.multispectral as ms


def get_tile(grid, h, v):
    return [x for x in grid['features'] if x['properties']['horizontal'] == h
            and x['properties']['vertical'] == v][0]['geometry']

def get_bbox(geometry):
    return rasterio.features.bounds(tile)

def lsma(data):
    M = 0
    U = 0
    return pysptools.abundance_maps.amaps.FCLS(M, U)
