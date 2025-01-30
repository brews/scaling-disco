import pandas as pd
import xarray as xr


CARB_SEGMENT_WEIGHTS_URI = "gs://rhg-data/impactlab-rhg/client-projects/2021-carb-cvm/data-prep/source_data/segment_weights/California_2019_census_tracts_weighted_by_population_0p25.csv"

OUT_ZARR = "gs://new_carb_demo/clean_cmip5_segment_weights.zarr"


sw = pd.read_csv(
    CARB_SEGMENT_WEIGHTS_URI,
    dtype={"GEOID": str},  # Otherwise ID is read as int...
)
sw["longitude"] = (sw["longitude"] + 180) % 360 - 180
sw = sw.to_xarray().rename_vars(
    {"longitude": "lon", "latitude": "lat", "GEOID": "region"}
)
sw.to_zarr(OUT_ZARR, mode="w")
print(f"Written to {OUT_ZARR}") 
