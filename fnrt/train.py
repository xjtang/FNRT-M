""" Module for model training
"""


import numpy as np

from sklearn import linear_model
from sklearn.metrics import mean_squared_error
from .fnrt import construct_dependents, unmix, calculate_ndfi


def model_fit(Y, X):
    """
    Fit one term harmonic model to data.

    Args:
        Y: predict variable
        X: dependent variables

    Returns:
        numpy array
    """

    regr = linear_model.LinearRegression()
    y_true = Y[~np.isnan(Y)]
    x_true = X[~np.isnan(Y), :]
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


def training(M, U, X, scale=10000, cfthreshold=0.2):
    """
    Full training process.

    Args:
        M: input data
        U: endmembers
        X: dependent variables
        scale: scaling factor

    Returns:
        numpy array
    """

    unmixed = unmix(M, U)
    Y = calculate_ndfi(unmixed, scale, cfthreshold)
    if (Y[~np.isnan(Y)].size == 0):
        return np.array(
            [0, 0, 0, 0, 0],
            ndmin=2,
            dtype='float64'
        )
    else:
        return model_fit(Y, X)


def xr_training(col, endmembers, chunksize=32, scale=10000,
                days_in_year=365.25, cfthreshold=0.2):
    """
    Apply full training process on entire xarray.

    Args:
        col: input data
        endmembers: endmembers
        chunksize: chunk size
        scale: scaling factor

    Returns:
        xarray data array
    """

    X = construct_dependents(col, days_in_year)
    col2 = col.chunk((-1, -1, chunksize, chunksize))
    return (
        xr.apply_ufunc(
            training, col2,
            input_core_dims=[['time', 'band']],
            output_core_dims=[['time', 'fit']],
            exclude_dims=set(('time', 'band')),
            kwargs={'X': X, 'U': endmembers,'scale': scale,
                'cfthreshold': cfthreshold},
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


# End
