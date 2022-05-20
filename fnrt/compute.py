""" Module for parallel computing
"""


import os

from dask_gateway import GatewayCluster
from dask.distributed import PipInstall, Lock


def install():
    """
    Install package on workers.
    """

    os.system('pip install pysptools')
    os.system('pip install cvxopt')


def get_cluster(n=20):
    """
    Create a dask cluster.

    Args:
        n: number of workers

    Returns:
        (dask cluster, dask client)
    """

    cluster = GatewayCluster()
    cluster.scale(n)
    client = cluster.get_client()
    plugin = PipInstall(packages=['pysptools', 'cvxopt'], pip_options=['--upgrade'])
    client.register_worker_plugin(plugin)
    return (cluster, client)


# End
