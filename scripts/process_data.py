import argparse
import json
from pathlib import Path
import random

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point


ACS_VARIABLES = {
    "total_pop": "B01003_001E",
    "median_income": "B19013_001E",
    "poverty_total": "B17001_001E",
    "poverty_pop": "B17001_002E",
    "edu_total_25plus": "B15003_001E",
    "edu_ba": "B15003_022E",
    "edu_ma": "B15003_023E",
    "edu_prof": "B15003_024E",
    "edu_phd": "B15003_025E",
    "owner_occ": "B25003_002E",
    "renter_occ": "B25003_003E",
    "race_total": "B03002_001E",
    "race_white": "B03002_003E",
    "race_black": "B03002_004E",
    "race_native": "B03002_005E",
    "race_asian": "B03002_006E",
    "race_pacific": "B03002_007E",
    "race_other": "B03002_008E",
    "race_two_plus": "B03002_009E",
    "race_hispanic": "B03002_012E",
    "male_under5": "B01001_003E",
    "male_5_9": "B01001_004E",
    "male_10_14": "B01001_005E",
    "male_15_17": "B01001_006E",
    "male_18_19": "B01001_007E",
    "male_20": "B01001_008E",
    "male_21": "B01001_009E",
    "male_22_24": "B01001_010E",
    "male_25_29": "B01001_011E",
    "male_30_34": "B01001_012E",
    "male_35_39": "B01001_013E",
    "male_40_44": "B01001_014E",
    "male_45_49": "B01001_015E",
    "male_50_54": "B01001_016E",
    "male_55_59": "B01001_017E",
    "male_60_61": "B01001_018E",
    "male_62_64": "B01001_019E",
    "male_65_66": "B01001_020E",
    "male_67_69": "B01001_021E",
    "male_70_74": "B01001_022E",
    "male_75_79": "B01001_023E",
    "male_80_84": "B01001_024E",
    "male_85_plus": "B01001_025E",
    "female_under5": "B01001_027E",
    "female_5_9": "B01001_028E",
    "female_10_14": "B01001_029E",
    "female_15_17": "B01001_030E",
    "female_18_19": "B01001_031E",
    "female_20": "B01001_032E",
    "female_21": "B01001_033E",
    "female_22_24": "B01001_034E",
    "female_25_29": "B01001_035E",
    "female_30_34": "B01001_036E",
    "female_35_39": "B01001_037E",
    "female_40_44": "B01001_038E",
    "female_45_49": "B01001_039E",
    "female_50_54": "B01001_040E",
    "female_55_59": "B01001_041E",
    "female_60_61": "B01001_042E",
    "female_62_64": "B01001_043E",
    "female_65_66": "B01001_044E",
    "female_67_69": "B01001_045E",
    "female_70_74": "B01001_046E",
    "female_75_79": "B01001_047E",
    "female_80_84": "B01001_048E",
    "female_85_plus": "B01001_049E",
}

SECTOR_MAP = {
    "CNS01": "Agriculture_Forestry_Fishing",
    "CNS02": "Mining",
    "CNS03": "Utilities",
    "CNS04": "Construction",
    "CNS05": "Manufacturing",
    "CNS06": "Wholesale_Trade",
    "CNS07": "Retail_Trade",
    "CNS08": "Transportation_Warehousing",
    "CNS09": "Information",
    "CNS10": "Finance_Insurance",
    "CNS11": "Real_Estate_Rental",
    "CNS12": "Professional_Scientific_Technical",
    "CNS13": "Management_Companies",
    "CNS14": "Admin_Waste_Services",
    "CNS15": "Educational_Services",
    "CNS16": "Health_Care_Social_Assistance",
    "CNS17": "Arts_Entertainment_Recreation",
    "CNS18": "Accommodation_Food_Services",
    "CNS19": "Other_Services",
    "CNS20": "Public_Administration",
}


def pick_name_column(gdf: gpd.GeoDataFrame) -> str:
    candidates = [
        "ORG_NAME",
        "ORGANIZATION",
        "REDEV_ORG",
        "REDEV_NAME",
        "NAME",
        "ORG",
    ]
    for col in candidates:
        if col in gdf.columns:
            return col
    for col in gdf.columns:
        if gdf[col].dtype == object:
            return col
    return gdf.columns[0]


def read_acs(acs_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in sorted(acs_dir.glob("acs5_*_dfw.csv")):
        df = pd.read_csv(csv_path, dtype=str)
        df["year"] = df["year"].astype(int)
        frames.append(df)
    if not frames:
        raise RuntimeError("No ACS files found in data/raw/acs.")
    acs = pd.concat(frames, ignore_index=True)
    return acs


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def compute_age_bands(df: pd.DataFrame) -> pd.DataFrame:
    under18_cols = [
        "male_under5",
        "male_5_9",
        "male_10_14",
        "male_15_17",
        "female_under5",
        "female_5_9",
        "female_10_14",
        "female_15_17",
    ]
    age_18_64_cols = [
        "male_18_19",
        "male_20",
        "male_21",
        "male_22_24",
        "male_25_29",
        "male_30_34",
        "male_35_39",
        "male_40_44",
        "male_45_49",
        "male_50_54",
        "male_55_59",
        "male_60_61",
        "male_62_64",
        "female_18_19",
        "female_20",
        "female_21",
        "female_22_24",
        "female_25_29",
        "female_30_34",
        "female_35_39",
        "female_40_44",
        "female_45_49",
        "female_50_54",
        "female_55_59",
        "female_60_61",
        "female_62_64",
    ]
    age_65_plus_cols = [
        "male_65_66",
        "male_67_69",
        "male_70_74",
        "male_75_79",
        "male_80_84",
        "male_85_plus",
        "female_65_66",
        "female_67_69",
        "female_70_74",
        "female_75_79",
        "female_80_84",
        "female_85_plus",
    ]
    df["age_under18"] = df[under18_cols].sum(axis=1)
    df["age_18_64"] = df[age_18_64_cols].sum(axis=1)
    df["age_65_plus"] = df[age_65_plus_cols].sum(axis=1)
    return df


def compute_rates(df: pd.DataFrame) -> pd.DataFrame:
    df["poverty_rate"] = df["poverty_pop"] / df["poverty_total"]
    df["owner_rate"] = df["owner_occ"] / (df["owner_occ"] + df["renter_occ"])
    df["renter_rate"] = df["renter_occ"] / (df["owner_occ"] + df["renter_occ"])
    df["ba_plus"] = df["edu_ba"] + df["edu_ma"] + df["edu_prof"] + df["edu_phd"]
    df["ba_plus_rate"] = df["ba_plus"] / df["edu_total_25plus"]
    df["hispanic_rate"] = df["race_hispanic"] / df["race_total"]
    return df


def add_redev_to_tracts(tracts: gpd.GeoDataFrame, redev: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    name_col = pick_name_column(redev)
    redev = redev.copy()
    redev["redev_name"] = redev[name_col].astype(str)
    redev = redev[["redev_name", "geometry"]]
    joined = gpd.sjoin(tracts, redev, predicate="intersects", how="left")
    joined["redev_name"] = joined["redev_name"].fillna("None")
    return joined.drop(columns=["index_right"])


def prepare_acs_panel(raw_dir: Path, boundaries_dir: Path, processed_dir: Path) -> pd.DataFrame:
    acs = read_acs(raw_dir / "acs")
    for key, value in ACS_VARIABLES.items():
        if value not in acs.columns:
            raise RuntimeError(f"Missing ACS variable {value} in raw data.")
        acs = acs.rename(columns={value: key})

    acs["geoid"] = acs["state"] + acs["county"] + acs["tract"]
    numeric_cols = list(ACS_VARIABLES.keys())
    acs = coerce_numeric(acs, numeric_cols)
    acs = compute_age_bands(acs)
    acs = compute_rates(acs)

    tracts = gpd.read_file(boundaries_dir / "tracts_dfw.geojson")
    redev = gpd.read_file(boundaries_dir / "redev_orgs.geojson")
    tracts = add_redev_to_tracts(tracts, redev)
    tracts.to_file(boundaries_dir / "tracts_dfw.geojson", driver="GeoJSON")
    tracts[["GEOID", "redev_name"]].to_csv(processed_dir / "tract_redev.csv", index=False)

    acs = acs.merge(
        tracts[["GEOID", "redev_name"]],
        left_on="geoid",
        right_on="GEOID",
        how="left",
    ).drop(columns=["GEOID"])
    acs["redev_name"] = acs["redev_name"].fillna("None")

    keep_cols = [
        "geoid",
        "year",
        "redev_name",
        "total_pop",
        "median_income",
        "poverty_total",
        "poverty_pop",
        "poverty_rate",
        "owner_occ",
        "renter_occ",
        "owner_rate",
        "renter_rate",
        "edu_total_25plus",
        "ba_plus",
        "ba_plus_rate",
        "race_total",
        "race_white",
        "race_black",
        "race_native",
        "race_asian",
        "race_pacific",
        "race_other",
        "race_two_plus",
        "race_hispanic",
        "hispanic_rate",
        "age_under18",
        "age_18_64",
        "age_65_plus",
    ]
    panel = acs[keep_cols].copy()

    panel = panel.sort_values(["geoid", "year"]).reset_index(drop=True)
    numeric = [c for c in panel.columns if c not in ("geoid", "year", "redev_name")]
    for col in numeric:
        panel[f"delta_{col}"] = panel.groupby("geoid")[col].diff()

    panel.to_csv(processed_dir / "acs_panel.csv", index=False)

    latest_year = panel["year"].max()
    latest = panel[panel["year"] == latest_year]
    tracts_latest = tracts.merge(latest, left_on="GEOID", right_on="geoid", how="left")
    tracts_latest.to_file(processed_dir / "acs_latest.geojson", driver="GeoJSON")

    meta = {
        "latest_year": int(latest_year),
        "acs_variables": keep_cols,
        "delta_variables": [f"delta_{c}" for c in numeric],
    }
    with open(processed_dir / "acs_metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return panel


def process_lodes(raw_dir: Path, processed_dir: Path, boundaries_dir: Path) -> None:
    lodes_path = raw_dir / "lodes" / "tx_rac_S000_JT00_2023.csv.gz"
    if not lodes_path.exists():
        raise RuntimeError("LODES file not found. Run download_data.py first.")

    sector_cols = list(SECTOR_MAP.keys())
    usecols = ["h_geocode"] + sector_cols
    lodes = pd.read_csv(lodes_path, usecols=usecols, dtype=str)
    for col in sector_cols:
        lodes[col] = pd.to_numeric(lodes[col], errors="coerce").fillna(0).astype(int)
    lodes["geoid"] = lodes["h_geocode"].str.slice(0, 11)
    lodes = lodes.drop(columns=["h_geocode"])
    lodes = lodes.groupby("geoid", as_index=False)[sector_cols].sum()

    tracts = gpd.read_file(boundaries_dir / "tracts_dfw.geojson")
    tracts = tracts[["GEOID", "redev_name", "geometry"]]
    lodes = lodes.merge(tracts[["GEOID", "redev_name"]], left_on="geoid", right_on="GEOID", how="inner")
    lodes = lodes.drop(columns=["GEOID"])

    long_df = lodes.melt(
        id_vars=["geoid", "redev_name"],
        value_vars=sector_cols,
        var_name="naics_sector",
        value_name="jobs",
    )
    long_df["sector_name"] = long_df["naics_sector"].map(SECTOR_MAP)
    long_df.to_csv(processed_dir / "lodes_tract_sector.csv", index=False)

    dominant = long_df.loc[long_df.groupby("geoid")["jobs"].idxmax()].copy()
    dominant = dominant.rename(columns={"naics_sector": "dominant_sector"})
    dominant.to_csv(processed_dir / "lodes_dominant_sector.csv", index=False)

    dots = []
    jobs_per_dot = 50
    max_dots_per_tract = 250
    tracts_index = tracts.set_index("GEOID")
    random.seed(42)

    for row in long_df.itertuples(index=False):
        if row.jobs <= 0:
            continue
        tract_geom = tracts_index.loc[row.geoid, "geometry"]
        tract_bounds = tract_geom.bounds
        count = min(int(row.jobs // jobs_per_dot), max_dots_per_tract)
        for _ in range(count):
            for _attempt in range(20):
                x = random.uniform(tract_bounds[0], tract_bounds[2])
                y = random.uniform(tract_bounds[1], tract_bounds[3])
                point = Point(x, y)
                if tract_geom.contains(point):
                    dots.append(
                        {
                            "geometry": point,
                            "geoid": row.geoid,
                            "redev_name": row.redev_name,
                            "naics_sector": row.naics_sector,
                            "sector_name": row.sector_name,
                        }
                    )
                    break

    if dots:
        dots_gdf = gpd.GeoDataFrame(dots, crs=tracts.crs)
        dots_gdf.to_file(processed_dir / "lodes_dots.geojson", driver="GeoJSON")
    else:
        empty = gpd.GeoDataFrame(
            columns=["geoid", "redev_name", "naics_sector", "sector_name", "geometry"],
            geometry="geometry",
            crs=tracts.crs,
        )
        empty.to_file(processed_dir / "lodes_dots.geojson", driver="GeoJSON")


def main() -> None:
    parser = argparse.ArgumentParser(description="Process ACS and LODES data into map-ready outputs.")
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--boundaries-dir", type=str, default="data/boundaries")
    parser.add_argument("--processed-dir", type=str, default="data/processed")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    boundaries_dir = Path(args.boundaries_dir)
    processed_dir = Path(args.processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    prepare_acs_panel(raw_dir, boundaries_dir, processed_dir)
    process_lodes(raw_dir, processed_dir, boundaries_dir)


if __name__ == "__main__":
    main()
