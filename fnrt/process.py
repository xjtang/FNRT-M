""" Module for complete processing
"""

from . import log
from .compute import get_cluster
from .io import (read_file, load_blob_grid, get_container, get_blob,
    get_landsat_stack, export_to_blob)
from .utilities import get_tile, mask_landsat, to_surface_reflectance
from .train import xr_training
from .parameter import defaults

def fnrt_train_landsat(h, v, key, container='training',
                        dask=40, parameters=defaults):
    """
    Train FNRT model for one tile using Landsat data.

    Args:
        h: horizontal id
        v: vertical id
        key: path to connection string
        container: blob container to save results
        dask: dask worker size
        parameters: model parameters
    """

    # initiate dask cluster
    (cluster, client) = get_cluster(80)
    log.info((cluster.dashboard_link))

    # load grid and get tile
    connection_string = read_file(key)
    container_client = get_container('misc', connection_string)
    blob_client = get_blob(container_client, 'amazon_grid.geojson')
    grid_blob = load_blob_grid(blob_client)
    tile_blob = get_tile(grid_blob, 41, 28)

    # get and preprocess Landsat data
    lst = get_landsat_stack(parameters['TRAIN_START'], parameters['TRAIN_END'],
        tile_blob)
    processed = to_surface_reflectance(mask_landsat(lst))

    # model training
    trained = xr_training(processed, parameters['UNMIX']['ENDMEMBERS'], 32,
        parameters['SCALE_FACTOR'], parameters['DAYS_IN_YEAR'],
        parameters['UNMIX']['CF_THRESHOLD'])

    # save output
    output_container = get_container(container, connection_string)
    output_name = ('FNRT_' + f'{h:03}' + f'{v:03}' + '_' +
        parameters['TRAIN_START'][2:4] + parameters['TRAIN_END'][2:4] + '.tif')
    export_to_blob(trained, training_container, output_name)

# End
