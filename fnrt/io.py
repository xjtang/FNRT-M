""" Module for input/output
"""


import io
import geojson
import stackstac
import azure.storage.blob
import planetary_computer
import xarray as xr
import rioxarray as rioxr

from pystac_client import Client
from dask.distributed import Lock
from .utilities import get_bbox, get_epsg
from .parameter import defaults


def read_file(file):
    """
    Read a text file from local disk.

    Args:
        file: path to input file

    Returns:
        string
    """

    with open(file) as f:
        content = f.read()
    return content


def load_local_grid(file):
    """
    Load the grid file from local disk.

    Args:
        file: path to grid file

    Returns:
        geojson object
    """

    with open(file) as f:
        grid = geojson.load(f)
    return grid


def load_blob_grid(blob_client):
    """
    Load the grid file from blob storage.

    Args:
        blob_client: blob client to load

    Returns:
        geojson object
    """

    return geojson.loads(blob_client.download_blob().readall())


def get_container(container, connection_string):
    """
    Create a container client.

    Args:
        container: name of container
        connection_string: connection string

    Returns:
        container client
    """

    container_client = azure.storage.blob.ContainerClient.from_connection_string(
        connection_string, container_name=container
    )
    return container_client


def get_blob(container_client, blob_name):
    """
    Create a blob client.

    Args:
        container_client: container client
        blob_name: name of blob

    Returns:
        blob client
    """

    blob_client = container_client.get_blob_client(blob_name)
    return blob_client


def search_landsat_images(start, end, geometry, limit=1000):
    """
    Search for Landsat images.

    Args:
        start: start date
        end: end date
        geometry: search boundary
        limit: number of items limit

    Returns:
        items
    """

    catalog = Client.open(defaults['CATALOG'])
    search = catalog.search(
        intersects = geometry,
        datetime = start + '/' + end,
        collections = [defaults['LANDSAT']],
        limit = limit,
        query={'landsat:collection_category': {'eq': 'T1'},
               'eo:cloud_cover': {'lt': 90}}
    )
    return list(search.get_items())


def get_landsat_stack(start, end, geometry, chunksize=32):
    """
    Get Landsat image stack.

    Args:
        start: start date
        end: end date
        geometry: search boundary
        chunksize: chunk size

    Returns:
        xarray data array
    """

    items = search_landsat_images(start, end, geometry, chunksize=1024)
    signed_items = [planetary_computer.sign(item).to_dict() for item in items]

    bbox = get_bbox(geometry)
    epsg = get_epsg(items)

    data = (
        stackstac.stack(
            signed_items,
            #assets=['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel'],
            assets=['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL'],
            #chunksize=(1, -1, 512, 512),
            chunksize=chunksize,
            resolution=30,
            epsg=epsg,
            bounds_latlon=bbox
        )
        .assign_coords(band=['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'QA'])
    )
    return data#.chunk((-1, -1, chunksize, chunksize))


def print_catalog(catalog):
    """
    Print collections in a catalog

    Args:
        catalog: data catalog
    """

    collections = catalog.get_children()
    for collection in collections:
        print(f'{collection.id} - {collection.title}')


def export_to_drive(img, des, driver='COG', nodata=0, dask=False, client=None):
    """
    Export image to drive

    Args:
        img: image to save
        des: path to destination
        driver: output driver
        nodata: no data value
        dask: use dask or not
        client: dask client
    """

    dataset = (img
               .to_dataset(dim='band')
               .rio.write_crs(img.coords['epsg'].item())
              )

    for data_var in dataset.data_vars:
        dataset[data_var].rio.write_nodata(nodata, inplace=True)

    if dask:
        dataset.rio.to_raster(des, driver=driver, tiled=True,
            lock=Lock('fnrtm', client=client))
    else:
        dataset.rio.to_raster(des, driver=driver)


def export_to_blob(img, container_client, blob, driver='COG', nodata=0,
                    dask=False, client=None):
    """
    Export image to blob

    Args:
        img: image to save
        container_client: container client
        blob: blob name
        driver: output driver
        nodata: no data value
        dask: use dask or not
        client: dask client
    """
    dataset = (img
               .to_dataset(dim='band')
               .rio.write_crs(img.coords['epsg'].item())
              )

    for data_var in dataset.data_vars:
        dataset[data_var].rio.write_nodata(nodata, inplace=True)

    with io.BytesIO() as buffer:
        if dask:
            dataset.rio.to_raster(buffer, driver=driver, tiled=True,
                lock=Lock('fnrtm', client=client))
        else:
            dataset.rio.to_raster(buffer, driver=driver)
        buffer.seek(0)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(buffer, overwrite=True)


# End
