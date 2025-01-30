# scaling-disco
Demo mortality impact analysis using dask, xarray, muuttaa.

This analysis is very loosely based on [mortality impact projections used for a Climate Vulnerability Metric report](https://impactlab.org/research/climate-vulnerability-metric-unequal-climate-impacts-in-the-state-of-california/) for the California Air Resources Board (CARB). This is a quick and dirty example analysis.

The `mortality.ipynb` notebook is the example impact projection. The various `clean_*.py` scripts are run as setup, for data cleaning, to create input data for the example analysis.

This was run within a `pangeo/pangeo-notebook:2024.11.11` container. Analysis notebooks were run on https://notebooks.cilresearch.org. All cleaning scripts were run on https://notebooks.rhgresearch.com.
