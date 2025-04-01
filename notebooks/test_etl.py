import pytest


@pytest.mark.skip(reason="Something went wrong with script")
def test_main():
    from simple_etl import main

    raw_df, processed_df = main()
    assert len(raw_df) > 1
    assert len(processed_df) > 1

@pytest.mark.skip(reason="SQL script not implemented")
def test_main_sql():
    import datetime
    from simple_etl import extract_and_load, calculate_price, calculate_price_sql

    utc_ts = datetime.datetime.now(datetime.timezone.utc)
    extracted = extract_and_load(utc_ts)
    baseline_stats = calculate_price(extracted)
    sql_stats = calculate_price_sql(extracted)
    assert set(baseline_stats.columns) == set(sql_stats.columns)
    assert len(baseline_stats) == len(sql_stats)
