""" Module for utility function
"""


import rasterio.features
import matplotlib.pyplot as plt

from scipy.stats import mode


def get_tile(grid, h, v):
    """
    Extract one tile out of the grid.

    Args:
        grid: grid geojson object
        h: horizontal id
        v: vertical id

    Returns:
        geometry
    """

    return [x for x in grid['features'] if x['properties']['horizontal'] == h
            and x['properties']['vertical'] == v][0]['geometry']


def get_bbox(geometry):
    """
    Calculate the bounding box of a geometry.

    Args:
        geometry: input geometry

    Returns:
        geometry
    """

    return rasterio.features.bounds(tile)


def display_image(img, vmin=0, vmax=6000, fsize=8):
    """
    Display an image.

    Args:
        img: input image
        vmin: minimum value
        vmax: maximum value
        fsize: frame size

    """

    fig, ax = plt.subplots(figsize=(fsize, fsize))
    ax.set_axis_off()
    img.plot.imshow(ax=ax, vmin=vmin, vmax=vmax);


def get_epsg(items):
    """
    Find the common EPSG of items.

    Args:
        items: items

    Returns:
        string
    """

    epsgs = [x.properties['proj:epsg'] for x in items]
    return 'EPSG: ' + str(mode(epsgs).mode[0])


def mask_landsat(col):
    """
    Mask Landsat stack.

    Args:
        col: xarary data array

    Returns:
        xarary data array
    """

    good = [21824, 21952, 5440, 5504]
    mask = col.sel(band='QA').astype('uint16').isin(good)
    return (col.sel(band=['Red', 'Green', 'Blue', 'NIR', 'SWIR1', 'SWIR2'])
            .where(mask==1))


def to_surface_reflectance(col):
    """
    Calculate surface reflectance for Landsat Collection 2 images.

    Args:
        col: xarary data array

    Returns:
        xarary data array
    """

    return ((col * 0.0000275 - 0.2) * 10000)


def calculate_ndvi(col, scale=10000, red='Red', nir='NIR'):
    """
    Calculate NDVI.

    Args:
        col: xarary data array
        scale: scale factor
        red: name of red band
        nir: name of nir band

    Returns:
        xarary data array
    """

    red = col.sel(band=red)
    nir = col.sel(band=nir)
    return (nir-red) / (nir + red) * scale


def array_to_frac_year(array, days_in_year=365.25):
    """
    Calcualte fractional year from data array.

    Args:
        array: xarary data array
        days_in_year: number of days in a year

    Returns:
        xarary data array
    """

    return array.time.dt.year + array.time.dt.day / days_in_year


# End
