import os
import posixpath
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from hdbcli import dbapi


CONFIG_URI = os.getenv("CONFIG_URI", "/app/pipeline/tables.yaml")
TABLE_ID = os.environ["TABLE_ID"]
RUN_NAME = os.environ["RUN_NAME"]
RUN_DATE = os.getenv("RUN_DATE")

HANA_HOST = os.environ["HANA_HOST"]
HANA_PORT = int(os.getenv("HANA_PORT", "30015"))
HANA_USER = os.environ["HANA_USER"]
HANA_PASSWORD = os.environ["HANA_PASSWORD"]

TIMEZONE_NAME = os.getenv("TIMEZONE", "Asia/Seoul")
BUSINESS_TIMEZONE = ZoneInfo(TIMEZONE_NAME)


def read_text(uri: str) -> str:
    if uri.startswith("gs://"):
        parsed = urlparse(uri)
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")

        if not bucket_name or not blob_name:
            raise ValueError(f"Invalid GCS URI: {uri}")

        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        return blob.download_as_text(encoding="utf-8")

    return Path(uri).read_text(encoding="utf-8")


def resolve_relative_uri(config_uri: str, relative_path: str) -> str:
    if config_uri.startswith("gs://"):
        parsed = urlparse(config_uri)
        config_dir = posixpath.dirname(parsed.path.lstrip("/"))
        object_name = posixpath.normpath(
            posixpath.join(config_dir, relative_path)
        )
        return f"gs://{parsed.netloc}/{object_name}"

    return str(
        (Path(config_uri).parent / relative_path).resolve()
    )


def load_pipeline_config() -> dict:
    config = yaml.safe_load(read_text(CONFIG_URI))

    if not isinstance(config, dict):
        raise ValueError("tables.yaml must contain a YAML object")

    if config.get("version") != 1:
        raise ValueError("Unsupported tables.yaml version")

    if not isinstance(config.get("tables"), list):
        raise ValueError("tables.yaml must contain a tables list")

    return config


def find_table_and_run(config: dict) -> tuple[dict, dict, dict]:
    defaults = config.get("defaults") or {}

    table_config = next(
        (
            item
            for item in config["tables"]
            if item.get("id") == TABLE_ID
        ),
        None,
    )

    if table_config is None:
        raise ValueError(f"Unknown TABLE_ID: {TABLE_ID}")

    if not table_config.get("enabled", False):
        raise ValueError(f"TABLE_ID is disabled: {TABLE_ID}")

    run_config = next(
        (
            item
            for item in table_config.get("runs", [])
            if item.get("name") == RUN_NAME
        ),
        None,
    )

    if run_config is None:
        raise ValueError(
            f"Unknown RUN_NAME={RUN_NAME} for TABLE_ID={TABLE_ID}"
        )

    return defaults, table_config, run_config


def get_reference_date() -> date:
    if RUN_DATE:
        try:
            return date.fromisoformat(RUN_DATE)
        except ValueError as exc:
            raise ValueError(
                "RUN_DATE must use YYYY-MM-DD format"
            ) from exc

    return datetime.now(BUSINESS_TIMEZONE).date()


def get_date_window(
    reference_date: date,
    run_config: dict,
) -> tuple[str | None, str | None]:
    strategy = run_config["load_strategy"]

    if strategy == "replace":
        return None, None

    if strategy != "window_replace":
        raise ValueError(
            f"Unsupported load_strategy: {strategy}"
        )

    window = run_config.get("window") or {}
    window_type = window.get("type")

    if window_type == "rolling_days":
        days = int(window.get("days", 0))

        if days < 1:
            raise ValueError(
                "rolling_days requires days >= 1"
            )

        start_date = reference_date - timedelta(days=days - 1)
        end_date = reference_date + timedelta(days=1)

    elif window_type == "previous_month":
        first_this_month = reference_date.replace(day=1)
        last_previous_month = first_this_month - timedelta(days=1)
        start_date = last_previous_month.replace(day=1)
        end_date = first_this_month

    else:
        raise ValueError(
            f"Unsupported window type: {window_type}"
        )

    return (
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d"),
    )


def quote_hana_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid HANA identifier: {name}")

    return f'"{name}"'


def quote_bq_column(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid BigQuery column: {name}")

    return f"`{name}`"


def validate_bq_component(name: str, component_type: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise ValueError(
            f"Invalid BigQuery {component_type}: {name}"
        )

    return name


def build_target_table_id(table_config: dict) -> str:
    project = validate_bq_component(
        table_config["bq_project"],
        "project",
    )
    dataset = validate_bq_component(
        table_config["bq_dataset"],
        "dataset",
    )
    table = validate_bq_component(
        table_config["bq_table"],
        "table",
    )

    return f"{project}.{dataset}.{table}"


def build_staging_table_id(target_table_id: str) -> str:
    project, dataset, table = target_table_id.split(".", 2)
    suffix = uuid.uuid4().hex[:12]
    staging_table = f"_stg_{table}_{suffix}"

    return f"{project}.{dataset}.{staging_table}"


def build_hana_query(
    base_sql: str,
    window_column: str | None,
    start_yyyymmdd: str | None,
    end_yyyymmdd: str | None,
) -> tuple[str, list[str]]:
    base_sql = base_sql.strip().rstrip(";")

    if not base_sql:
        raise ValueError("Query file is empty")

    if start_yyyymmdd is None or end_yyyymmdd is None:
        return base_sql, []

    if not window_column:
        raise ValueError(
            "window_column is required for window_replace"
        )

    quoted_window_column = quote_hana_identifier(window_column)

    sql = f"""
SELECT *
FROM (
{base_sql}
) SRC
WHERE SRC.{quoted_window_column} >= ?
  AND SRC.{quoted_window_column} < ?
""".strip()

    return sql, [start_yyyymmdd, end_yyyymmdd]


def connect_hana():
    return dbapi.connect(
        address=HANA_HOST,
        port=HANA_PORT,
        user=HANA_USER,
        password=HANA_PASSWORD,
    )


def validate_source_columns(columns: list[str]) -> None:
    if len(columns) != len(set(columns)):
        raise ValueError(
            "HANA query returned duplicate column names"
        )

    reserved_columns = {
        "_loaded_at",
        "_run_name",
        "_run_date",
        "_load_start_yyyymmdd",
        "_load_end_yyyymmdd",
    }

    collisions = reserved_columns.intersection(columns)

    if collisions:
        raise ValueError(
            f"HANA columns conflict with metadata columns: "
            f"{sorted(collisions)}"
        )


def build_staging_schema(
    source_columns: list[str],
) -> list[bigquery.SchemaField]:
    schema = [
        bigquery.SchemaField(column, "STRING")
        for column in source_columns
    ]

    schema.extend(
        [
            bigquery.SchemaField(
                "_loaded_at",
                "TIMESTAMP",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                "_run_name",
                "STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                "_run_date",
                "STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                "_load_start_yyyymmdd",
                "STRING",
            ),
            bigquery.SchemaField(
                "_load_end_yyyymmdd",
                "STRING",
            ),
        ]
    )

    return schema


def create_staging_table(
    bq_client: bigquery.Client,
    staging_table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    table = bigquery.Table(
        staging_table_id,
        schema=schema,
    )

    # 실패 시에도 staging 테이블이 영구히 남지 않도록 하루 뒤 만료
    table.expires = datetime.now(timezone.utc) + timedelta(days=1)

    bq_client.create_table(table)


def to_raw_string(value):
    if value is None:
        return None

    return str(value)


def load_chunk_to_staging(
    bq_client: bigquery.Client,
    dataframe: pd.DataFrame,
    staging_table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    bq_client.load_table_from_dataframe(
        dataframe,
        staging_table_id,
        job_config=job_config,
    ).result()


def count_table_rows(
    bq_client: bigquery.Client,
    table_id: str,
) -> int:
    query = f"""
SELECT COUNT(*) AS row_count
FROM `{table_id}`
"""

    rows = list(bq_client.query(query).result())
    return int(rows[0]["row_count"])


def table_exists(
    bq_client: bigquery.Client,
    table_id: str,
) -> bool:
    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False


def schema_signature(
    schema: list[bigquery.SchemaField],
) -> list[tuple[str, str]]:
    return [
        (field.name, field.field_type)
        for field in schema
    ]


def validate_target_schema(
    bq_client: bigquery.Client,
    target_table_id: str,
    staging_table_id: str,
) -> None:
    target = bq_client.get_table(target_table_id)
    staging = bq_client.get_table(staging_table_id)

    if schema_signature(target.schema) != schema_signature(staging.schema):
        raise ValueError(
            "Target and staging schemas are different. "
            "Run a reviewed schema migration or initial_full."
        )


def apply_replace(
    bq_client: bigquery.Client,
    target_table_id: str,
    staging_table_id: str,
) -> None:
    query = f"""
CREATE OR REPLACE TABLE `{target_table_id}` AS
SELECT *
FROM `{staging_table_id}`
"""

    bq_client.query(query).result()


def apply_window_replace(
    bq_client: bigquery.Client,
    target_table_id: str,
    staging_table_id: str,
    window_column: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> None:
    if not table_exists(bq_client, target_table_id):
        query = f"""
CREATE TABLE `{target_table_id}` AS
SELECT *
FROM `{staging_table_id}`
"""
        bq_client.query(query).result()
        return

    validate_target_schema(
        bq_client,
        target_table_id,
        staging_table_id,
    )

    date_column = quote_bq_column(window_column)

    query = f"""
BEGIN TRANSACTION;

DELETE FROM `{target_table_id}`
WHERE {date_column} >= @start_date
  AND {date_column} < @end_date;

INSERT INTO `{target_table_id}`
SELECT *
FROM `{staging_table_id}`;

COMMIT TRANSACTION;
"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "start_date",
                "STRING",
                start_yyyymmdd,
            ),
            bigquery.ScalarQueryParameter(
                "end_date",
                "STRING",
                end_yyyymmdd,
            ),
        ]
    )

    bq_client.query(
        query,
        job_config=job_config,
    ).result()


def main() -> None:
    config = load_pipeline_config()
    defaults, table_config, run_config = find_table_and_run(config)

    schema_mode = table_config.get(
        "schema_mode",
        defaults.get("schema_mode", "raw_string"),
    )

    if schema_mode != "raw_string":
        raise ValueError(
            f"Unsupported schema_mode: {schema_mode}"
        )

    chunk_size = int(
        table_config.get(
            "chunk_size",
            defaults.get("chunk_size", 50000),
        )
    )

    allow_empty = bool(
        run_config.get(
            "allow_empty",
            table_config.get(
                "allow_empty",
                defaults.get("allow_empty", False),
            ),
        )
    )

    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    reference_date = get_reference_date()
    start_yyyymmdd, end_yyyymmdd = get_date_window(
        reference_date,
        run_config,
    )

    query_uri = resolve_relative_uri(
        CONFIG_URI,
        table_config["query_file"],
    )
    base_sql = read_text(query_uri)

    window_column = table_config.get("window_column")

    hana_sql, hana_params = build_hana_query(
        base_sql,
        window_column,
        start_yyyymmdd,
        end_yyyymmdd,
    )

    target_table_id = build_target_table_id(table_config)
    staging_table_id = build_staging_table_id(target_table_id)

    bq_client = bigquery.Client(
        project=table_config["bq_project"]
    )

    print(f"TABLE_ID={TABLE_ID}")
    print(f"RUN_NAME={RUN_NAME}")
    print(f"RUN_DATE={reference_date.isoformat()}")
    print(f"CONFIG_URI={CONFIG_URI}")
    print(f"QUERY_URI={query_uri}")
    print(f"TARGET={target_table_id}")
    print(f"STAGING={staging_table_id}")
    print(f"ALLOW_EMPTY={allow_empty}")

    if start_yyyymmdd and end_yyyymmdd:
        print(
            f"WINDOW={start_yyyymmdd} "
            f"<= {window_column} < {end_yyyymmdd}"
        )
    else:
        print("WINDOW=FULL")

    hana_connection = connect_hana()
    staging_created = False
    apply_succeeded = False

    try:
        cursor = hana_connection.cursor()

        try:
            cursor.execute(hana_sql, hana_params)

            source_columns = [
                column[0]
                for column in cursor.description
            ]

            validate_source_columns(source_columns)

            staging_schema = build_staging_schema(source_columns)

            create_staging_table(
                bq_client,
                staging_table_id,
                staging_schema,
            )
            staging_created = True

            total_rows = 0

            while True:
                rows = cursor.fetchmany(chunk_size)

                if not rows:
                    break

                dataframe = pd.DataFrame(
                    rows,
                    columns=source_columns,
                )

                for column in source_columns:
                    dataframe[column] = dataframe[column].map(
                        to_raw_string
                    )

                loaded_at = datetime.now(timezone.utc)

                dataframe["_loaded_at"] = loaded_at
                dataframe["_run_name"] = RUN_NAME
                dataframe["_run_date"] = reference_date.isoformat()
                dataframe["_load_start_yyyymmdd"] = start_yyyymmdd
                dataframe["_load_end_yyyymmdd"] = end_yyyymmdd

                load_chunk_to_staging(
                    bq_client,
                    dataframe,
                    staging_table_id,
                    staging_schema,
                )

                total_rows += len(dataframe)
                print(f"STAGING_ROWS_LOADED={total_rows}")

        finally:
            cursor.close()

        verified_rows = count_table_rows(
            bq_client,
            staging_table_id,
        )

        if verified_rows != total_rows:
            raise RuntimeError(
                f"Row count mismatch: "
                f"python={total_rows}, staging={verified_rows}"
            )

        if verified_rows == 0 and not allow_empty:
            raise RuntimeError(
                "HANA query returned 0 rows and allow_empty=false"
            )

        load_strategy = run_config["load_strategy"]

        if load_strategy == "replace":
            apply_replace(
                bq_client,
                target_table_id,
                staging_table_id,
            )

        elif load_strategy == "window_replace":
            if not start_yyyymmdd or not end_yyyymmdd:
                raise ValueError(
                    "window_replace requires a date window"
                )

            apply_window_replace(
                bq_client,
                target_table_id,
                staging_table_id,
                window_column,
                start_yyyymmdd,
                end_yyyymmdd,
            )

        else:
            raise ValueError(
                f"Unsupported load_strategy: {load_strategy}"
            )

        apply_succeeded = True

        print(f"STATUS=SUCCESS")
        print(f"LOADED_ROWS={verified_rows}")
        print(f"TARGET={target_table_id}")

    finally:
        hana_connection.close()

        if staging_created and apply_succeeded:
            bq_client.delete_table(
                staging_table_id,
                not_found_ok=True,
            )
            print(f"STAGING_DELETED={staging_table_id}")

        elif staging_created:
            print(
                f"STATUS=FAILED, staging retained for debugging: "
                f"{staging_table_id}"
            )


if __name__ == "__main__":
    main()
