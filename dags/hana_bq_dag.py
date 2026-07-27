from datetime import timedelta
from pathlib import Path
import re

import pendulum
import yaml

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)


PROJECT_ID = "ga4-bigquery-431807"
REGION = "asia-northeast3"
CLOUD_RUN_JOB = "hana-bq-pipeline-loader"

CONFIG_DIR_PATH = (
    Path(__file__).parent
    / "hana_bq"
    / "configs"
)

CONFIG_GCS_DIR = (
    "gs://asia-northeast3-hana-bq-com-2c023c7f-bucket"
    "/dags/hana_bq/configs"
)

SEOUL = pendulum.timezone("Asia/Seoul")


def normalize_name(value: str) -> str:
    normalized = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        value,
    )
    return normalized.strip("_").lower()


def load_enabled_runs() -> list[
    tuple[str, str, str | None, str]
]:
    if not CONFIG_DIR_PATH.exists():
        raise FileNotFoundError(
            f"Config directory not found: {CONFIG_DIR_PATH}"
        )

    config_files = sorted(
        CONFIG_DIR_PATH.glob("*.yaml")
    )

    if not config_files:
        raise ValueError(
            f"No YAML files found: {CONFIG_DIR_PATH}"
        )

    enabled_runs = []

    for config_path in config_files:
        with config_path.open(
            "r",
            encoding="utf-8",
        ) as config_file:
            config = yaml.safe_load(config_file)

        if not isinstance(config, dict):
            raise ValueError(
                f"{config_path.name} must contain a YAML object"
            )

        if config.get("version") != 1:
            raise ValueError(
                f"Unsupported version: {config_path.name}"
            )

        tables = config.get("tables")

        if not isinstance(tables, list) or len(tables) != 1:
            raise ValueError(
                f"{config_path.name} must contain exactly one table"
            )

        table_config = tables[0]

        if not table_config.get("enabled", False):
            continue

        table_id = table_config["id"]

        if config_path.stem.upper() != table_id.upper():
            raise ValueError(
                f"Filename and table id are different: "
                f"{config_path.name} / {table_id}"
            )

        config_uri = (
            f"{CONFIG_GCS_DIR}/{config_path.name}"
        )

        runs = sorted(
            table_config.get("runs", []),
            key=lambda item: item.get("name", ""),
        )

        for run_config in runs:
            run_name = run_config["name"]
            schedule = run_config.get("schedule")

            enabled_runs.append(
                (
                    table_id,
                    run_name,
                    schedule,
                    config_uri,
                )
            )

    return enabled_runs


def create_loader_dag(
    table_id: str,
    run_name: str,
    schedule: str | None,
    config_uri: str,
) -> DAG:
    dag_id = (
        f"hana_bq_{normalize_name(table_id)}_"
        f"{normalize_name(run_name)}"
    )

    with DAG(
        dag_id=dag_id,
        description=(
            f"SAP HANA to BigQuery: "
            f"{table_id} / {run_name}"
        ),
        start_date=pendulum.datetime(
            2026,
            7,
            24,
            tz=SEOUL,
        ),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args={
            "owner": "data-platform",
            "retries": 2,
            "retry_delay": timedelta(minutes=10),
        },
        tags=[
            "hana",
            "bigquery",
            "cloud-run",
            normalize_name(table_id),
            normalize_name(run_name),
        ],
    ) as generated_dag:
        CloudRunExecuteJobOperator(
            task_id=(
                f"load_{normalize_name(table_id)}_"
                f"{normalize_name(run_name)}"
            ),
            project_id=PROJECT_ID,
            region=REGION,
            job_name=CLOUD_RUN_JOB,
            overrides={
                "container_overrides": [
                    {
                        "env": [
                            {
                                "name": "CONFIG_URI",
                                "value": config_uri,
                            },
                            {
                                "name": "TABLE_ID",
                                "value": table_id,
                            },
                            {
                                "name": "RUN_NAME",
                                "value": run_name,
                            },
                            {
                                "name": "RUN_DATE",
                                "value": (
                                    "{{ data_interval_end"
                                    ".in_timezone('Asia/Seoul')"
                                    ".strftime('%Y-%m-%d') }}"
                                ),
                            },
                        ],
                    }
                ],
                "task_count": 1,
                "timeout": "3600s",
            },
            pool="hana_extract_pool",
            deferrable=True,
            timeout_seconds=3700,
        )

    return generated_dag


def register_dags() -> None:
    for (
        table_id,
        run_name,
        schedule,
        config_uri,
    ) in load_enabled_runs():
        dag_object = create_loader_dag(
            table_id=table_id,
            run_name=run_name,
            schedule=schedule,
            config_uri=config_uri,
        )

        globals()[dag_object.dag_id] = dag_object


register_dags()
