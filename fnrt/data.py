import numpy as np
import xarray as xr
import rasterio.features
import stackstac
import planetary_computer
import xrspatial.multispectral as ms

from dask_gateway import GatewayCluster
from pystac_client import Client

from . import search_images, catalog, get_bbox


def get_landsat_stack(start, end, geometry):
    bbox = get_bbox(geometry)
    items = search_images(catalog, 'landsat-8-c2-l2', geometry, start, end, limit=1000):
    signed_items = [planetary_computer.sign(item).to_dict() for item in items]
    data = (
        stackstac.stack(
            signed_items,
            assets=['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL'],
            chunksize=4096,
            resolution=30,
            bounds_latlon=bbox
        )
        .assign_coords(band=lambda x: x.common_name.rename("band"))  # use common names
    )
    return data
