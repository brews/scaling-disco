"""
Combine CMIP6 GDPCIR "tasmax" and "tasmin" ensemble members and output "tas" estimate.

Data is output as a single, cleaned and chunked xarray.DataTree in a Zarr Store. 
Data is read in from our internal GDPCIR storage.
"""

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
import datetime
from typing import Callable
import os
import uuid

from dask_gateway import GatewayCluster
import pandas as pd
import numpy as np
import xarray as xr 


JUPYTER_IMAGE = os.environ['JUPYTER_IMAGE']
UID = str(uuid.uuid4())
START_TIME = datetime.datetime.now(datetime.timezone.utc).isoformat()

# Input URLS are defined en-masse below.
DT_OUT_ZARR = "gs://new_carb_demo/clean_cmip6.zarr"


print(
    f"""
        {JUPYTER_IMAGE=}
        {START_TIME=}
        {UID=}
    """
)


# TODO: Try on workers with less memory.
cluster = GatewayCluster(worker_image=JUPYTER_IMAGE, scheduler_image=JUPYTER_IMAGE)
client = cluster.get_client()
print(client.dashboard_link) 

cluster.scale(100) 


# This is all just a fancy way to structure all the ensemble URLs.

# URL to a ZarrStore we'll read with xarray
# type ZarrStoreUrl = str  # Ugg this only works on py3.12
ZarrStoreUrl = str
# Ug. Can't think of a better name.
@dataclass(frozen=True)
class GdpcirRun:
    """
    Easily define URLs for historical dataset and the SSP experiments we want to concatenate it to
    """
    historical: ZarrStoreUrl
    ssps: Sequence[ZarrStoreUrl]


# Explicitly writing out the full URLs because its easier to debug and I'm lazy.
# We need to make "tas" so we only need to grab "tasmax" and "tasmin".
gdpcir_targets = [
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CSIRO-ARCCSS/ACCESS-CM2/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO-ARCCSS/ACCESS-CM2/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO-ARCCSS/ACCESS-CM2/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CSIRO-ARCCSS/ACCESS-CM2/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO-ARCCSS/ACCESS-CM2/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO-ARCCSS/ACCESS-CM2/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/BCC/BCC-CSM2-MR/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CMCC/CMCC-CM2-SR5/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CMCC/CMCC-CM2-SR5/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-CM2-SR5/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CMCC/CMCC-ESM2/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CCCma/CanESM5/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CCCma/CanESM5/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CCCma/CanESM5/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-AerChem/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-AerChem/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-AerChem/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-AerChem/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-CC/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-CC/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-CC/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-CC/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-CC/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-CC/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg-LR/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-Veg/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3-Veg/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3-Veg/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/EC-Earth-Consortium/EC-Earth3/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CAS/FGOALS-g3/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/CAS/FGOALS-g3/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/CAS/FGOALS-g3/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NOAA-GFDL/GFDL-CM4/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-CM4/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-CM4/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NOAA-GFDL/GFDL-CM4/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-CM4/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-CM4/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NOAA-GFDL/GFDL-ESM4/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NOAA-GFDL/GFDL-ESM4/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NOAA-GFDL/GFDL-ESM4/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MOHC/HadGEM3-GC31-LL/historical/r1i1p1f3/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp126/r1i1p1f3/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp245/r1i1p1f3/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp585/r1i1p1f3/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MOHC/HadGEM3-GC31-LL/historical/r1i1p1f3/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp126/r1i1p1f3/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp245/r1i1p1f3/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/HadGEM3-GC31-LL/ssp585/r1i1p1f3/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/INM/INM-CM4-8/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/INM/INM-CM4-8/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM4-8/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/INM/INM-CM5-0/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/INM/INM-CM5-0/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/INM/INM-CM5-0/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MIROC/MIROC-ES2L/historical/r1i1p1f2/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp126/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp245/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp370/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp585/r1i1p1f2/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MIROC/MIROC-ES2L/historical/r1i1p1f2/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp126/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp245/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp370/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC-ES2L/ssp585/r1i1p1f2/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MIROC/MIROC6/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MIROC/MIROC6/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MIROC/MIROC6/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MPI-M/MPI-ESM1-2-LR/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MPI-M/MPI-ESM1-2-LR/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NUIST/NESM3/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NUIST/NESM3/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NUIST/NESM3/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NCC/NorESM2-LM/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NCC/NorESM2-LM/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-LM/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NCC/NorESM2-MM/historical/r1i1p1f1/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp126/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp245/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp370/r1i1p1f1/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp585/r1i1p1f1/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/NCC/NorESM2-MM/historical/r1i1p1f1/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp126/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp245/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp370/r1i1p1f1/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/NCC/NorESM2-MM/ssp585/r1i1p1f1/day/tasmin/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/day/tasmax/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp126/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp245/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp370/r1i1p1f2/day/tasmax/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp585/r1i1p1f2/day/tasmax/v1.1.zarr']),
 GdpcirRun(historical='gs://downscaled-48ec31ab/outputs/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/day/tasmin/v1.1.zarr',
           ssps=['gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp126/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp245/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp370/r1i1p1f2/day/tasmin/v1.1.zarr',
                 'gs://downscaled-48ec31ab/outputs/ScenarioMIP/MOHC/UKESM1-0-LL/ssp585/r1i1p1f2/day/tasmin/v1.1.zarr']),
]


def build_gdpcir_ensemble(target_runs: Sequence[GdpcirRun]) -> xr.DataTree:
    """
    Takes GDPCIR URL information and transforms into a single xr.DataTree with historical and SSPs concatenated.
    """
    dt_dict = defaultdict(xr.Dataset)
    for spec in target_runs:

        hist_ds = xr.open_zarr(spec.historical)

        for ssp_url in spec.ssps:

            proj_ds = xr.open_zarr(ssp_url)

            concat_ds = xr.concat([hist_ds, proj_ds], dim="time", combine_attrs="drop_conflicts")

            # Grab metadata we want to use for tree index/dict key.
            k = "{experiment_id}/{source_id}".format(
                experiment_id=proj_ds.attrs["experiment_id"],
                # Want to raise error if no source_id, as it should match in hist and proj, thus get preserved by the xr.concat() above.
                source_id=concat_ds.attrs["source_id"],
            )
            # Merge with existing (possibly empty) Dataset so variables from same source are together in same dataset.
            dt_dict[k] = dt_dict[k].merge(concat_ds, combine_attrs="drop_conflicts")

    all_dt = xr.DataTree.from_dict(dt_dict)
    return all_dt


def estimate_tas(ds):
    """
    Return Dataset with tas from input tasmax and tasmin
    """
    # Return ds if is doesn't have variables we need.
    # This happens if it's an empty node on the tree, apparently.
    if not (("tasmax" in ds.variables) and ("tasmin" in ds.variables)):
        return ds

    ds = ds.copy()  # Copy input so we can modify it.

    ds["tas"] = ((ds["tasmax"] + ds["tasmin"]) / 2.0).astype("float32")
    return ds[["tas"]]
    

gdpcir_tas = (
    build_gdpcir_ensemble(gdpcir_targets)
        .map_over_datasets(estimate_tas)
        .chunk({"time": 365 * 20, "lat": 90, "lon": 90})
)

gdpcir_tas.attrs["uid"] = UID
gdpcir_tas.attrs["created_at"] = START_TIME 

print(gdpcir_tas)

gdpcir_tas.to_zarr(DT_OUT_ZARR, mode="w")
print(f"Written to {DT_OUT_ZARR}") 

cluster.scale(0)
cluster.shutdown() 
