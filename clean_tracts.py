""""
Clean TIGER2019 census tract shape file and output.
"""
import geopandas as gpd'


GEO_PATH = "/gcs/rhg-data/impactlab-rhg/spatial/shapefiles/source/us_census/TIGER2019/TRACT/tl_2019_06_tract"
OUT_CLEAN_TRACTS_PARQUET = "gs://new_carb_demo/clean_tracts.parquet"

tracts = gpd.read_file(geo_path)
tracts["region"] = tracts["GEOID"]

tracts.to_parquet(OUT_CLEAN_TRACTS_PARQUET)
