from __future__ import annotations

from typing import Iterable

import pandas as pd

from ual.influx.Influx_db_connector import InfluxDBConnector
from ual.influx.influx_buckets import InfluxBuckets


def _as_flux_or_filter(fields: list[str]) -> str:
    if not fields:
        raise RuntimeError("At least one field is required for query generation.")
    return " or ".join([f'r._field == "{field}"' for field in fields])


def _as_flux_rename_map(rename_map: dict[str, str]) -> str:
    if not rename_map:
        return ""
    parts = [f'{source}: "{target}"' for source, target in rename_map.items()]
    return ", ".join(parts)


def _normalize_df(result: pd.DataFrame | Iterable[pd.DataFrame]) -> pd.DataFrame:
    if isinstance(result, list):
        if not result:
            return pd.DataFrame()
        return pd.concat(result, ignore_index=True)
    return result


def _clean_query_df(result: pd.DataFrame | Iterable[pd.DataFrame]) -> pd.DataFrame:
    df = _normalize_df(result)
    if df.empty:
        return df
    df = df.drop(
        columns=["result", "table", "_start", "_stop", "_measurement", "topic", "host"],
        errors="ignore",
    )
    if "_time" in df.columns:
        df = df.set_index("_time", drop=True)
    return df


def _clean_query_df_with_rename(
    result: pd.DataFrame | Iterable[pd.DataFrame],
    rename_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    df = _normalize_df(result)
    if df.empty:
        return df
    df = df.drop(
        columns=["result", "table", "_start", "_stop", "_measurement", "topic", "host"],
        errors="ignore",
    )
    if rename_map:
        # pandas.rename ignores non-existing keys, so missing optional fields do not fail.
        df = df.rename(columns=rename_map)
    if "_time" in df.columns:
        df = df.set_index("_time", drop=True)
    return df


def build_hourly_source_query(
    start: str,
    stop: str,
    bucket: str,
    measurement: str,
    topic: str,
    fields: list[str],
    rename_map: dict[str, str] | None = None,
    aggregate_every: str = "1h",
) -> str:
    rename_map = rename_map or {}

    return f"""
from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "{measurement}" and r.topic == "{topic}")
  |> filter(fn: (r) => {_as_flux_or_filter(fields)})
  |> aggregateWindow(every: {aggregate_every}, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
""".strip()


def fetch_hourly_ual_lubw(
    url: str,
    token: str,
    organization: str,
    start: str,
    stop: str,
    ual_sensor: str,
    lubw_sensor: str,
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
    ual_measurement: str = "measurement_data",
    lubw_measurement: str = "lubw_hour_data",
    ual_topic_template: str = "sensors/measurement/{sensor}",
    lubw_topic_template: str = "sensors/lubw-hour/{sensor}",
    ual_fields: list[str] | None = None,
    lubw_fields: list[str] | None = None,
    ual_rename: dict[str, str] | None = None,
    lubw_rename: dict[str, str] | None = None,
    aggregate_every: str = "1h",
    join_how: str = "inner",
    output_csv_path: str = "hourly_ual_lubw.csv",
) -> pd.DataFrame:
    ual_fields = ual_fields or ["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "sht_temp", "sht_humid"]
    lubw_fields = lubw_fields or ["CO", "TEMP"]
    ual_rename = ual_rename or {}
    lubw_rename = lubw_rename or {}

    connector = InfluxDBConnector(url, token, organization)

    ual_query = build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=ual_bucket,
        measurement=ual_measurement,
        topic=ual_topic_template.format(sensor=ual_sensor),
        fields=ual_fields,
        rename_map=ual_rename,
        aggregate_every=aggregate_every,
    )
    lubw_query = build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=lubw_bucket,
        measurement=lubw_measurement,
        topic=lubw_topic_template.format(sensor=lubw_sensor),
        fields=lubw_fields,
        rename_map=lubw_rename,
        aggregate_every=aggregate_every,
    )

    ual_df = _clean_query_df_with_rename(connector.query_api.query_data_frame(ual_query), rename_map=ual_rename)
    lubw_df = _clean_query_df_with_rename(connector.query_api.query_data_frame(lubw_query), rename_map=lubw_rename)

    if ual_df.empty and lubw_df.empty:
        joined_df = pd.DataFrame()
        joined_df.index.name = "_time"
    elif ual_df.empty:
        joined_df = lubw_df.copy()
    elif lubw_df.empty:
        joined_df = ual_df.copy()
    else:
        joined_df = ual_df.join(lubw_df, how=join_how)

    joined_df.to_csv(output_csv_path)
    return joined_df


def fetch_hourly_ual_lubw_debug(
    url: str,
    token: str,
    organization: str,
    start: str,
    stop: str,
    ual_sensor: str,
    lubw_sensor: str,
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
    ual_measurement: str = "measurement_data",
    lubw_measurement: str = "lubw_hour_data",
    ual_topic_template: str = "sensors/measurement/{sensor}",
    lubw_topic_template: str = "sensors/lubw-hour/{sensor}",
    ual_fields: list[str] | None = None,
    lubw_fields: list[str] | None = None,
    ual_rename: dict[str, str] | None = None,
    lubw_rename: dict[str, str] | None = None,
    aggregate_every: str = "1h",
    join_how: str = "inner",
    ual_csv_path: str = "hourly_ual_only.csv",
    lubw_csv_path: str = "hourly_lubw_only.csv",
    joined_csv_path: str = "hourly_ual_lubw.csv",
) -> pd.DataFrame:
    ual_fields = ual_fields or ["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "sht_temp", "sht_humid"]
    lubw_fields = lubw_fields or ["CO", "TEMP"]
    ual_rename = ual_rename or {}
    lubw_rename = lubw_rename or {}

    connector = InfluxDBConnector(url, token, organization)

    ual_query = build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=ual_bucket,
        measurement=ual_measurement,
        topic=ual_topic_template.format(sensor=ual_sensor),
        fields=ual_fields,
        rename_map=ual_rename,
        aggregate_every=aggregate_every,
    )
    ual_df = _clean_query_df_with_rename(connector.query_api.query_data_frame(ual_query), rename_map=ual_rename)
    ual_df.to_csv(ual_csv_path)

    lubw_query = build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=lubw_bucket,
        measurement=lubw_measurement,
        topic=lubw_topic_template.format(sensor=lubw_sensor),
        fields=lubw_fields,
        rename_map=lubw_rename,
        aggregate_every=aggregate_every,
    )
    lubw_df = _clean_query_df_with_rename(connector.query_api.query_data_frame(lubw_query), rename_map=lubw_rename)
    lubw_df.to_csv(lubw_csv_path)

    joined_df = fetch_hourly_ual_lubw(
        url=url,
        token=token,
        organization=organization,
        start=start,
        stop=stop,
        ual_sensor=ual_sensor,
        lubw_sensor=lubw_sensor,
        ual_bucket=ual_bucket,
        lubw_bucket=lubw_bucket,
        ual_measurement=ual_measurement,
        lubw_measurement=lubw_measurement,
        ual_topic_template=ual_topic_template,
        lubw_topic_template=lubw_topic_template,
        ual_fields=ual_fields,
        lubw_fields=lubw_fields,
        ual_rename=ual_rename,
        lubw_rename=lubw_rename,
        aggregate_every=aggregate_every,
        join_how=join_how,
        output_csv_path=joined_csv_path,
    )
    return joined_df


def fetch_hourly_source(
    url: str,
    token: str,
    organization: str,
    start: str,
    stop: str,
    bucket: str,
    measurement: str,
    topic: str,
    fields: list[str],
    rename_map: dict[str, str] | None = None,
    aggregate_every: str = "1h",
    output_csv_path: str | None = None,
) -> pd.DataFrame:
    rename_map = rename_map or {}

    connector = InfluxDBConnector(url, token, organization)
    query = build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=bucket,
        measurement=measurement,
        topic=topic,
        fields=fields,
        rename_map=rename_map,
        aggregate_every=aggregate_every,
    )
    df = _clean_query_df_with_rename(connector.query_api.query_data_frame(query), rename_map=rename_map)
    if output_csv_path:
        df.to_csv(output_csv_path)
    return df


def build_co_hourly_join_query(
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    lubw_sensor: str = "DEBW152",
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
) -> str:
    """
    Kept for backwards compatibility. Returns a CO-specific join query.
    """
    return f"""
ual = {build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=ual_bucket,
        measurement="measurement_data",
        topic=f"sensors/measurement/{ual_sensor}",
        fields=["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "sht_temp", "sht_humid"],
        rename_map={"CO": "CO_ual", "sht_temp": "UAL_TEMP"},
        aggregate_every="1h",
    )}

lubw = {build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=lubw_bucket,
        measurement="lubw_hour_data",
        topic=f"sensors/lubw-hour/{lubw_sensor}",
        fields=["CO", "TEMP"],
        rename_map={"CO": "CO_lubw", "TEMP": "LUBW_TEMP"},
        aggregate_every="1h",
    )}

join(tables: {{ual: ual, lubw: lubw}}, on: ["_time"], method: "inner")
""".strip()


def build_co_hourly_ual_query(
    start: str,
    stop: str,
    ual_sensor: str = "ual-4",
    ual_bucket: str = InfluxBuckets.UAL_MINUTE_MEASUREMENT_BUCKET.value,
) -> str:
    return build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=ual_bucket,
        measurement="measurement_data",
        topic=f"sensors/measurement/{ual_sensor}",
        fields=["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W"],
        aggregate_every="1h",
    )


def build_co_hourly_lubw_query(
    start: str,
    stop: str,
    lubw_sensor: str = "DEBW152",
    lubw_bucket: str = InfluxBuckets.LUBW_HOUR_BUCKET.value,
) -> str:
    return build_hourly_source_query(
        start=start,
        stop=stop,
        bucket=lubw_bucket,
        measurement="lubw_hour_data",
        topic=f"sensors/lubw-hour/{lubw_sensor}",
        fields=["CO"],
        aggregate_every="1h",
    )


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
    Kept for backwards compatibility. Uses the generic hourly fetch.
    """
    return fetch_hourly_ual_lubw(
        url=url,
        token=token,
        organization=organization,
        start=start,
        stop=stop,
        ual_sensor=ual_sensor,
        lubw_sensor=lubw_sensor,
        ual_fields=["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "sht_temp", "sht_humid"],
        lubw_fields=["CO", "TEMP"],
        ual_rename={"CO": "CO_ual", "sht_temp": "UAL_TEMP"},
        lubw_rename={"CO": "CO_lubw", "TEMP": "LUBW_TEMP"},
        aggregate_every="1h",
        join_how="inner",
        output_csv_path=output_csv_path,
    )


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
    Kept for backwards compatibility. Uses the generic hourly debug fetch.
    """
    return fetch_hourly_ual_lubw_debug(
        url=url,
        token=token,
        organization=organization,
        start=start,
        stop=stop,
        ual_sensor=ual_sensor,
        lubw_sensor=lubw_sensor,
        ual_fields=["CO", "RAW_ADC_CO_A", "RAW_ADC_CO_W", "sht_temp", "sht_humid"],
        lubw_fields=["CO", "TEMP"],
        ual_rename={"CO": "CO_ual", "sht_temp": "UAL_TEMP"},
        lubw_rename={"CO": "CO_lubw", "TEMP": "LUBW_TEMP"},
        aggregate_every="1h",
        join_how="inner",
        ual_csv_path=ual_csv_path,
        lubw_csv_path=lubw_csv_path,
        joined_csv_path=joined_csv_path,
    )
