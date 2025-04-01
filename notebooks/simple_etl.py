import datetime
import glob
import requests
import pandas as pd
from pathlib import Path
from typing import Generator, Any

TARGET_TABLE = "aws_spot_pricing_history"
SPOT_ADVISOR_DATA_URL = (
    r"https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
)
REGION_LIST = [
    "us-east-1",
    "eu-central-1",
]


def extract_aws_spot_bid(aws_spot_advisor_url: str) -> dict:
    response = requests.get(aws_spot_advisor_url)
    return response.json()


def _unpack_json_into_row(
    data: dict,
    region_list: list[str],
) -> Generator[tuple[str, str, int], Any, Any]:
    for region_code in data["spot_advisor"]:
        if region_code not in region_list:
            continue
        region_linux_data = data["spot_advisor"][region_code]["Linux"]
        for instance_type in region_linux_data:
            spot_price = region_linux_data[instance_type]["s"]
            yield region_code, instance_type, spot_price


def transform_dataset(
    data: dict, region_list: list[str], etl_utc_ts: datetime.datetime
) -> pd.DataFrame:
    df = pd.DataFrame(
        data=_unpack_json_into_row(data, region_list),
        columns=["region_code", "instance_type", "spot_price"],
    )
    df["etl_utc_ts"] = etl_utc_ts
    return df


def calculate_price(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["extracted_date"] = df["etl_utc_ts"].dt.date
    df = df.drop_duplicates(subset=["extracted_date", "region_code", "instance_type"])
    df["extracted_year"] = df["etl_utc_ts"].dt.year
    df["extracted_month"] = df["etl_utc_ts"].dt.month
    df = df.groupby(
        by=["extracted_year", "extracted_month", "region_code"],
        as_index=False,
    ).agg(
        avg_price=("spot_price", "mean"),
        max_price=("spot_price", "max"),
        min_price=("spot_price", "min"),
    )
    return df


def calculate_price_sql(df: pd.DataFrame) -> pd.DataFrame:
    import duckdb

    df = duckdb.query("""
select
    *
from df
""").to_df()
    return df


def write_result(df: pd.DataFrame, prefix_path: Path, etl_ts: datetime.datetime) -> int:
    output_folder = Path(__file__).parent / prefix_path
    output_folder.mkdir(parents=True, exist_ok=True)
    output_path = output_folder / (etl_ts.date().isoformat() + ".csv")
    df.to_csv(output_path, index=False)


def read_price_history(prefix_path: Path) -> pd.DataFrame:
    output_folder = Path(__file__).parent / prefix_path
    df = pd.concat(
        (
            pd.read_csv(extract_path)
            for extract_path in glob.glob(str(output_folder / "*.csv"))
        )
    )
    return df


def extract_and_load(utc_ts: datetime.datetime) -> pd.DataFrame:
    data = extract_aws_spot_bid(SPOT_ADVISOR_DATA_URL)
    df = transform_dataset(
        data=data,
        region_list=REGION_LIST,
        etl_utc_ts=utc_ts,
    )
    write_result(df, Path("data") / "extracted", utc_ts)
    return df


def transform(utc_ts: datetime.datetime) -> pd.DataFrame:
    df = read_price_history(Path("data") / "extracted")
    # can we rewrite this in SQL?
    df = calculate_price(df)
    write_result(df, Path("data") / "stats", utc_ts)
    return df


def main():
    utc_ts = datetime.datetime.now(datetime.timezone.utc)
    extracted = extract_and_load(utc_ts)
    stats = transform(utc_ts)
    return extracted, stats

if __name__ == "__main__":
    main()
