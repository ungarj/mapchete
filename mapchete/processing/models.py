from typing import Optional

from distributed import Client
from pydantic import BaseModel


class DaskConfiguration(BaseModel):
    dask_scheduler: Optional[str] = None
    dask_max_submitted_tasks: int = 1000
    dask_chunksize: int = 100
    dask_client: Optional[Client] = None
    dask_compute_graph: bool = True
    dask_propagate_results: bool = True
