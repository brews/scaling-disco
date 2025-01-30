"""Clean and standardize various socioeconomic variables. Write outputs to storage.
"""

import pandas as pd
import xarray as xr


INCOME_PATH = "/gcs/rhg-data/impactlab-rhg/client-projects/2021-carb-cvm/data-prep/output_data/income_adjusted.nc4"
PCI2019_URI = "gs://rhg-data/impactlab-rhg/client-projects/2021-carb-cvm/data-prep/output_data/PCI_2019.csv"
POP_URI = "gs://rhg-data/impactlab-rhg/client-projects/2021-carb-cvm/data-prep/output_data/population_age_binned.csv"

OUT_ZARR = "gs://new_carb_demo/clean_socioeconomics.zarr"


def clean_income_adjusted(target: str) -> xr.Dataset:
    income = xr.open_dataset(target).rename({"GEOID": "region"})
    # Encode "region" as Object rather than <U11 so consistent with transformed climate data region.
    income = income.assign_coords(region=(income["region"].astype("O")))
    return income[["loggdppc_residual_scaled"]].rename({"loggdppc_residual_scaled": "loggdppc"})


def clean_pci(target: str) -> xr.Dataset:
    # load 2019 income for valuation metric
    pci2019 = (
        pd.read_csv(target, dtype={"GEOID": str})
        .set_index("GEOID")
        .rename(columns={"2019": "pci"})
        .to_xarray()
    )
    pci2019 = pci2019.rename({"GEOID": "region"})
    return pci2019[["pci"]]


def clean_pop(target: str) -> xr.Dataset:
    # Read and clean pop data.
    # Rename is temporary measure to ensure var names match - need to change in data-prep step
    pop_raw = (
        pd.read_csv(target, dtype={"GEOID": str})
        .set_index("GEOID")
        .to_xarray()
        .rename(
            {
                "total_tract_population": "combined",
                "pop_lt5": "age1",
                "pop_5-64": "age2",
                "pop_65+": "age3",
            }
        )
    )

    # calculate pop shares
    for var in ["age1", "age2", "age3"]:
        pop_raw[f"{var}_share"] = pop_raw[var] / pop_raw["combined"]

    # Clean up pop data.
    pop = xr.Dataset()
    pop["pop"] = xr.concat(
        [pop_raw["age1"], pop_raw["age2"], pop_raw["age3"]],
        pd.Index(["age1", "age2", "age3"], name="age_cohort"),
    )
    pop["pop_combined"] = pop_raw["combined"]
    pop["pop_share"] = xr.concat(
        [pop_raw["age1_share"], pop_raw["age2_share"], pop_raw["age3_share"]],
        pd.Index(["age1", "age2", "age3"], name="age_cohort"),
    )
    pop = pop.rename({"GEOID": "region"})
    return pop


def clean_segment_weights(target: str) -> xr.Dataset:
    sw = pd.read_csv(
        target,
        dtype={"GEOID": str},  # Otherwise ID is read as int...
    )
    sw["longitude"] = (sw["longitude"] + 180) % 360 - 180
    sw = sw.to_xarray().rename_vars(
        {"longitude": "lon", "latitude": "lat", "GEOID": "region"}
    )
    return sw


def main():
    ds = xr.merge(
        [
            clean_income_adjusted(INCOME_PATH),
            clean_pci(PCI2019_URI),
            clean_pop(POP_URI),
        ],
        combine_attrs="drop",
    )
    # NOTE: We're dropping regions with any NAs and rechunking so it does calculations in sensible, bite-sized parts when we do impacts.
    ds = ds.dropna(dim="region").chunk({"region": 1000})
    ds.to_zarr(OUT_ZARR, mode="w")
    print(f"Written to {OUT_ZARR}") 


if __name__ == "__main__":
    main()
