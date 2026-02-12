from __future__ import annotations

from typing import Iterable

import pandas as pd

from ual.influx.Influx_db_connector import InfluxDBConnector
from ual.influx.influx_buckets import InfluxBuckets


def build_co_hourly_join_query(
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    lubw_sensor: str = "DEBW152",
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
) -> str:
    """
    Build a Flux query that joins UAL and LUBW hourly CO data on _time.

    start/stop are Flux time literals, e.g. 2024-01-01T00:00:00Z or -30d.
    """
    return f"""
ual = from(bucket: "{ual_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "measurement_data" and r.topic == "sensors/measurement/{ual_sensor}")
  |> filter(fn: (r) => r._field == "CO" or r._field == "RAW_ADC_CO_A" or r._field == "RAW_ADC_CO_W" or r._field == "sht_temp")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {{CO: "CO_ual", sht_temp: "UAL_TEMP"}})
  |> keep(columns: ["_time", "CO_ual", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "UAL_TEMP"])

lubw = from(bucket: "{lubw_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "lubw_hour_data" and r.topic == "sensors/lubw-hour/{lubw_sensor}")
  |> filter(fn: (r) => r._field == "CO" or r._field == "TEMP")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {{CO: "CO_lubw", TEMP: "LUBW_TEMP"}})
  |> keep(columns: ["_time", "CO_lubw", "LUBW_TEMP"])

join(tables: {{ual: ual, lubw: lubw}}, on: ["_time"], method: "inner")
  |> keep(columns: ["_time", "CO_ual", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "UAL_TEMP", "CO_lubw", "LUBW_TEMP"])
""".strip()


def build_co_hourly_ual_query(
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
) -> str:
    return f"""
from(bucket: "{ual_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "measurement_data" and r.topic == "sensors/measurement/{ual_sensor}")
  |> filter(fn: (r) => r._field == "CO" or r._field == "RAW_ADC_CO_A" or r._field == "RAW_ADC_CO_W")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
""".strip()


def build_co_hourly_lubw_query(
    start: str,
    stop: str,
    lubw_sensor: str = "DEBW152",
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
) -> str:
    return f"""
from(bucket: "{lubw_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "lubw_hour_data" and r.topic == "sensors/lubw-hour/{lubw_sensor}")
  |> filter(fn: (r) => r._field == "CO")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
""".strip()


def _normalize_query_result(result: pd.DataFrame | Iterable[pd.DataFrame]) -> pd.DataFrame:
    if isinstance(result, list):
        if not result:
            return pd.DataFrame()
        return pd.concat(result, ignore_index=True)
    return result


def fetch_co_hourly_ual_lubw(
    url: str,
    token: str,
    organization: str,
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    lubw_sensor: str = "DEBW152",
    output_csv_path: str = "co_hourly_ual4_lubw015.csv",
) -> pd.DataFrame:
    """
    Fetch hourly CO data from UAL + LUBW, join on _time, and save as CSV.
    """
    connector = InfluxDBConnector(url, token, organization)
    query = build_co_hourly_join_query(
        start=start,
        stop=stop,
        ual_sensor=ual_sensor,
        lubw_sensor=lubw_sensor,
    )
    result = connector.query_api.query_data_frame(query)
    df = _normalize_query_result(result)
    expected_cols = ["_time", "CO_ual", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "UAL_TEMP", "CO_lubw", "LUBW_TEMP"]
    if df.empty:
        df = pd.DataFrame(columns=expected_cols)
        df = df.set_index("_time", drop=True)
    else:
        df = df.drop(columns=["result", "table", "_start", "_stop"], errors="ignore")
        df = df.set_index("_time", drop=True)
        df = df.reindex(columns=[c for c in expected_cols if c != "_time"])
    df.to_csv(output_csv_path)
    return df


def fetch_co_hourly_ual_lubw_debug(
    url: str,
    token: str,
    organization: str,
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    lubw_sensor: str = "DEBW152",
    ual_csv_path: str = "co_hourly_ual_only.csv",
    lubw_csv_path: str = "co_hourly_lubw_only.csv",
    joined_csv_path: str = "co_hourly_ual4_lubw015.csv",
) -> pd.DataFrame:
    """
    Fetch UAL and LUBW separately (for debugging), then join and save all CSVs.
    """
    connector = InfluxDBConnector(url, token, organization)

    ual_query = build_co_hourly_ual_query(start=start, stop=stop, ual_sensor=ual_sensor)
    ual_result = connector.query_api.query_data_frame(ual_query)
    ual_df = _normalize_query_result(ual_result)
    if not ual_df.empty:
        ual_df = ual_df.drop(columns=["result", "table", "_start", "_stop", "_measurement", "topic"], errors="ignore")
        ual_df = ual_df.set_index("_time", drop=True)
    ual_df.to_csv(ual_csv_path)

    lubw_query = build_co_hourly_lubw_query(start=start, stop=stop, lubw_sensor=lubw_sensor)
    lubw_result = connector.query_api.query_data_frame(lubw_query)
    lubw_df = _normalize_query_result(lubw_result)
    if not lubw_df.empty:
        lubw_df = lubw_df.drop(columns=["result", "table", "_start", "_stop", "_measurement", "topic"], errors="ignore")
        lubw_df = lubw_df.set_index("_time", drop=True)
    lubw_df.to_csv(lubw_csv_path)

    joined_df = fetch_co_hourly_ual_lubw(
        url=url,
        token=token,
        organization=organization,
        start=start,
        stop=stop,
        ual_sensor=ual_sensor,
        lubw_sensor=lubw_sensor,
        output_csv_path=joined_csv_path,
    )
    return joined_df
