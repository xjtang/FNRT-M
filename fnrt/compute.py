import numpy as np
import xarray as xr
import stackstac
import planetary_computer
import xrspatial.multispectral as ms

from dask_gateway import GatewayCluster
from pystac_client import Client


catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

def search_images(catalog, collection, geometry, start, end, limit=1000):
    search = catalog.search(
        intersects = geometry,
        datetime = start + '/' + end,
        collections = [collection],
        limit = limit
    )
    return list(search.get_items())

def get_cluster():
    cluster = GatewayCluster()
    client = cluster.get_client()
    cluster.adapt(minimum=4, maximum=24)
    return cluster
