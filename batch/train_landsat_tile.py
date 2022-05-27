import os

def install():
    os.system('pip install pysptools')
    os.system('pip install cvxopt')
install()

import io
import math
import stackstac
import geojson
import dask_gateway
import planetary_computer
import rasterio.features
import azure.storage.blob
import numpy as np
import xarray as xr
import pysptools.abundance_maps as amp
import matplotlib.pyplot as plt
from dask.distributed import PipInstall, Lock
from scipy.stats import mode
from dask_gateway import GatewayCluster
from pystac_client import Client
from sklearn import linear_model
from sklearn.metrics import mean_squared_error

def read_file(file):
    with open(file) as f:
        content = f.read()
    return content

def load_blob_grid(blob_client):
    return geojson.loads(blob_client.download_blob().readall())

def get_tile(grid, h, v):
    return [x for x in grid['features'] if x['properties']['horizontal'] == h
            and x['properties']['vertical'] == v][0]['geometry']

def get_bbox(geometry):
    return rasterio.features.bounds(geometry)

def get_container(container, connection_string):
    container_client = azure.storage.blob.ContainerClient.from_connection_string(
        connection_string, container_name=container
    )
    return container_client

def get_blob(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client

def get_cluster(n=20, ncore=8, memory=16):
    gateway = dask_gateway.Gateway()
    cluster_options = gateway.cluster_options()
    cluster_options["worker_cores"] = ncore
    cluster_options["worker_memory"] = memory

    cluster = gateway.new_cluster(cluster_options)
    cluster.scale(n)
    client = cluster.get_client()
    return (cluster, client)

def register_package():
    plugin = PipInstall(packages=['pysptools', 'cvxopt'], pip_options=['--upgrade'])
    client.register_worker_plugin(plugin)

def get_epsg(items):
    epsgs = [x.properties['proj:epsg'] for x in items]
    return 'EPSG: ' + str(mode(epsgs).mode[0])

def search_landsat_images(start, end, geometry, limit=1000):
    catalog = Client.open('https://planetarycomputer.microsoft.com/api/stac/v1')
    #catalog = Client.open('https://planetarycomputer-staging.microsoft.com/api/stac/v1')

    search = catalog.search(
        intersects = geometry,
        datetime = start + '/' + end,
        collections = ['landsat-c2-l2'],
        limit = 1000,
        query={'landsat:collection_category': {'eq': 'T1'},
               'eo:cloud_cover': {'lt': 90}}
    )
    return list(search.get_items())

def get_landsat_stack(start, end, geometry, chunksize=128):
    items = search_landsat_images(start, end, geometry)
    signed_items = [planetary_computer.sign(item).to_dict() for item in items]

    bbox = get_bbox(geometry)
    epsg = get_epsg(items)

    data = (
        stackstac.stack(
            signed_items,
            assets=['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel'],
            #assets=['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL'],
            chunksize=(-1, -1, chunksize, chunksize),
            resolution=30,
            epsg=epsg,
            bounds_latlon=bbox
        )
        .assign_coords(band=['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'QA'])
    )
    return data

def array_to_frac_year(array, days_in_year=365.25):
    return array.time.dt.year + array.time.dt.day / days_in_year

def construct_dependents(array, days_in_year=365.25):
    x1 = array_to_frac_year(array, days_in_year)
    omega = 2 * math.pi
    x2 = np.cos(x1 * omega)
    x3 = np.sin(x1 * omega)
    return (
        xr.concat([x1, x2, x3], dim='x')
        .assign_coords(x=['x1', 'x2', 'x3'])
        .transpose(*('time', 'x'))
    )

def fnrt(M, U, X, scale=10000):
    M2 = M.astype('int16')
    qa = M2[:, -1]
    good = [21824, 21952, 5440, 5504]
    mask = np.isin(qa, good)
    sr = (M2[:, 0:6] * 0.0000275 - 0.2) * scale
    unmixed = amp.amaps.FCLS(sr, U)
    unmixed[mask==0, :] = np.nan

    gv = unmixed[:, 0]
    npv = unmixed[:, 1]
    soil = unmixed[:, 2]
    shade = unmixed[:, 3]
    cloud = unmixed[:, 4]

    gv_frac = (gv / (1 - shade)) + (npv + soil)
    mask = ((cloud < 0.2) & (shade < 1) & (gv_frac > 0)).astype('uint16')
    ndfi = (gv / (1 - shade) - (npv + soil)) / gv_frac * scale
    ndfi[mask==0] = np.nan

    regr = linear_model.LinearRegression()
    y_true = ndfi[~np.isnan(ndfi)]
    x_true = X[~np.isnan(ndfi), :]
    lm = regr.fit(x_true, y_true)
    coef = lm.coef_
    intercept = lm.intercept_
    y_pred = lm.predict(x_true)
    rmse = mean_squared_error(y_true=y_true, y_pred=y_pred, squared=False)

    return np.array(
        [intercept, coef[0], coef[1], coef[2], rmse],
        ndmin=2,
        dtype='float64'
    )

def xr_fnrt(col, endmembers, scale=10000):
    X = construct_dependents(col)
    return (
        xr.apply_ufunc(
            fnrt, col,
            input_core_dims=[['time', 'band']],
            output_core_dims=[['time', 'fit']],
            exclude_dims=set(('time', 'band')),
            kwargs={'X': X, 'U': endmembers,'scale': scale},
            dask='parallelized',
            vectorize=True,
            output_dtypes=[col.dtype],
            output_sizes={'time': 1, 'fit': 5}
        )
        .rename({'fit': 'band'})
        .assign_coords(band=['incpt','slope','cos','sin','rmse'])
        .transpose(*col.dims)
        .squeeze()
    )

def export_to_blob(img, container_client, blob, driver='COG', nodata=0, dask=False, client=None):
    import rioxarray as rioxr
    dataset = (img
               .to_dataset(dim='band')
               .rio.write_crs(img.coords['epsg'].item())
              )

    for data_var in dataset.data_vars:
        dataset[data_var].rio.write_nodata(nodata, inplace=True)

    with io.BytesIO() as buffer:
        if dask:
            dataset.rio.to_raster(buffer, driver=driver, tiled=True, lock=Lock('fnrtm', client=client))
        else:
            dataset.rio.to_raster(buffer, driver=driver)
        buffer.seek(0)
        blob_client = container_client.get_blob_client(blob)
        blob_client.upload_blob(buffer, overwrite=True)

def run_tile(h, v, chunksize=32):
    tile = get_tile(grid_blob, h, v)
    lst = get_landsat_stack('2019-01-01', '2021-12-31', tile, chunksize)
    trained = xr_fnrt(lst, endmembers)
    output_name = ('FNRT_' + f'{h:03}' + f'{v:03}' + '_' + '1921' + '.tif')
    print(output_name)
    export_to_blob(trained, training_container, output_name, dask=False)

if __name__ == '__main__':
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=fnrtm;AccountKey=Yi8TZysRhMZT93o9QPGzpOOyB6jukF0qyBudJoSNwiJqzlD08k1fpbPkmpyegi2lx5bnN/PXNm4C9LU4GK0uRA==;EndpointSuffix=core.windows.net'
    container_client = get_container('misc', connection_string)
    blob_client = get_blob(container_client, 'amazon_grid.geojson')
    training_container = get_container('training', connection_string)

    endmembers = np.array([[500, 900, 400, 6100, 3000, 1000],
                           [1400, 1700, 2200, 3000, 5500, 3000],
                           [2000, 3000, 3400, 5800, 6000, 5800],
                           [0, 0, 0, 0, 0, 0],
                           [9000, 9600, 8000, 7800, 7200, 6500]], dtype=np.int16)

    grid_blob = load_blob_grid(blob_client)

    (cluster, client) = get_cluster(400, 1, 8)

    plugin = PipInstall(packages=['pysptools', 'cvxopt'], pip_options=['--upgrade'])
    client.register_worker_plugin(plugin)

    print(cluster.dashboard_link)

    run_tile(37, 28, 256)

    cluster.close()
