""" Module for fnrt functions
"""


import math
import numpy as np
import xarray as xr
import pysptools.abundance_maps as amp


def construct_dependents(array, days_in_year=365.25):
    """
    Construct dependents array.

    Args:
        array: grid geojson object
        days_in_year: number of days in a year

    Returns:
        xarray data array
    """

    x1 = array_to_frac_year(array, days_in_year)
    omega = 2 * math.pi
    x2 = np.cos(x1 * omega)
    x3 = np.sin(x1 * omega)
    return (
        xr.concat([x1, x2, x3], dim='x')
        .assign_coords(x=['x1', 'x2', 'x3'])
        .transpose(*('time', 'x'))
    )


def unmix(M, U):
    """
    Construct dependents matrix.

    Args:
        M: input array
        U: endmembers

    Returns:
        numpy array
    """

    mask = (~np.isnan(M)).min(axis=-1)
    M2 = M.astype('int16')
    unmixed = amp.amaps.FCLS(M2, U)
    unmixed[mask==0, :] = np.nan
    return unmixed


def calculate_ndfi(M, scale=10000):
    """
    Calculate NDFI.

    Args:
        M: input array
        scale: scaling factor

    Returns:
        numpy array
    """

    gv = M[:, 0]
    npv = M[:, 1]
    soil = M[:, 2]
    shade = M[:, 3]
    cloud = M[:, 4]

    gv_frac = (gv / (1 - shade)) + (npv + soil)
    mask = ((cloud < 0.2) & (shade < 1) & (gv_frac > 0)).astype('uint16')
    ndfi = (gv / (1 - shade) - (npv + soil)) / gv_frac * scale
    ndfi[mask==0] = np.nan
    return ndfi


# End
