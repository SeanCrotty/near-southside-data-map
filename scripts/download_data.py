import argparse
import datetime as dt
import json
import os
from pathlib import Path
import zipfile

import geopandas as gpd
import pandas as pd
import requests


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
    # Age (B01001) for derived bands
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


def download_file(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dest


def unzip_file(zip_path: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)
    return target_dir


def detect_years(start_year: int, end_year: int | None, census_key: str | None) -> list[int]:
    if end_year is None:
        end_year = dt.datetime.now().year
    available = []
    for year in range(start_year, end_year + 1):
        url = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME&for=us:1"
        if census_key:
            url += f"&key={census_key}"
        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200:
                available.append(year)
        except requests.RequestException:
            continue
    return available


def get_dfw_counties(boundaries_dir: Path) -> pd.DataFrame:
    cbsa_url = "https://www2.census.gov/geo/tiger/TIGER2023/CBSA/tl_2023_us_cbsa.zip"
    county_url = "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"

    cbsa_zip = download_file(cbsa_url, boundaries_dir / "cbsa_2023.zip")
    county_zip = download_file(county_url, boundaries_dir / "county_2023.zip")

    cbsa_dir = unzip_file(cbsa_zip, boundaries_dir / "cbsa_2023")
    county_dir = unzip_file(county_zip, boundaries_dir / "county_2023")

    cbsa = gpd.read_file(cbsa_dir)
    counties = gpd.read_file(county_dir)

    dfw_cbsa = cbsa[cbsa["CBSAFP"] == "19100"]
    if dfw_cbsa.empty:
        raise RuntimeError("CBSA 19100 not found in TIGER CBSA file.")

    counties = counties[counties["STATEFP"] == "48"].to_crs(dfw_cbsa.crs)
    joined = gpd.sjoin(counties, dfw_cbsa[["geometry"]], predicate="intersects")

    dfw = joined[["STATEFP", "COUNTYFP", "NAME"]].drop_duplicates()
    dfw = dfw.sort_values(["STATEFP", "COUNTYFP"]).reset_index(drop=True)
    dfw.to_csv(boundaries_dir / "dfw_counties.csv", index=False)
    return dfw


def download_tracts(boundaries_dir: Path, counties: pd.DataFrame) -> Path:
    tracts_url = "https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_48_tract.zip"
    tracts_zip = download_file(tracts_url, boundaries_dir / "tracts_tx_2023.zip")
    tracts_dir = unzip_file(tracts_zip, boundaries_dir / "tracts_tx_2023")
    tracts = gpd.read_file(tracts_dir)
    dfw_counties = set(counties["COUNTYFP"].tolist())
    tracts = tracts[tracts["COUNTYFP"].isin(dfw_counties)].copy()
    tracts = tracts.to_crs("EPSG:4326")
    out_path = boundaries_dir / "tracts_dfw.geojson"
    tracts.to_file(out_path, driver="GeoJSON")
    return out_path


def read_redev_boundaries(zip_path: Path, boundaries_dir: Path) -> Path:
    gdf = gpd.read_file(zip_path)
    gdf = gdf.to_crs("EPSG:4326")
    out_path = boundaries_dir / "redev_orgs.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    return out_path


def fetch_acs_year(year: int, counties: pd.DataFrame, raw_dir: Path, census_key: str | None) -> Path:
    variables = ",".join(ACS_VARIABLES.values())
    rows = []
    for county_fips in counties["COUNTYFP"].tolist():
        url = (
            f"https://api.census.gov/data/{year}/acs/acs5"
            f"?get=NAME,{variables}"
            f"&for=tract:*&in=state:48+county:{county_fips}"
        )
        if census_key:
            url += f"&key={census_key}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        header = payload[0]
        data_rows = payload[1:]
        rows.extend([dict(zip(header, row)) for row in data_rows])

    df = pd.DataFrame(rows)
    df["year"] = year
    out_path = raw_dir / f"acs5_{year}_dfw.csv"
    df.to_csv(out_path, index=False)
    return out_path


def download_lodes(raw_dir: Path) -> Path:
    url = "https://lehd.ces.census.gov/data/lodes/LODES8/tx/rac/tx_rac_S000_JT00_2023.csv.gz"
    dest = raw_dir / "tx_rac_S000_JT00_2023.csv.gz"
    return download_file(url, dest)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download ACS, LODES, and boundary data.")
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--census-key", type=str, default=os.getenv("CENSUS_API_KEY"))
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--boundaries-dir", type=str, default="data/boundaries")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    boundaries_dir = Path(args.boundaries_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    boundaries_dir.mkdir(parents=True, exist_ok=True)

    counties = get_dfw_counties(boundaries_dir)
    download_tracts(boundaries_dir, counties)
    read_redev_boundaries(Path("ADM_NEIGHBORHOOD_REDEV_ORGS.zip"), boundaries_dir)

    years = detect_years(args.start_year, args.end_year, args.census_key)
    if not years:
        raise RuntimeError("No ACS years detected; check your Census API key or year range.")

    acs_dir = raw_dir / "acs"
    acs_dir.mkdir(parents=True, exist_ok=True)
    for year in years:
        fetch_acs_year(year, counties, acs_dir, args.census_key)

    lodes_dir = raw_dir / "lodes"
    lodes_dir.mkdir(parents=True, exist_ok=True)
    download_lodes(lodes_dir)

    meta = {
        "acs_years": years,
        "acs_variables": ACS_VARIABLES,
    }
    with open(raw_dir / "download_metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


if __name__ == "__main__":
    main()
