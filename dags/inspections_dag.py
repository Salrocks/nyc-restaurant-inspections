"""Daily pipeline: pull NYC inspection data, clean it, load to Postgres."""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

DEFAULT_ARGS = {
    "owner": "sal",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="nyc_inspections_pipeline",
    description="Extract NYC restaurant inspections, clean, load to Postgres",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",              # 6am daily
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "inspections"],
)
def inspections_pipeline():

    @task
    def extract() -> str:
        """Pull from the Socrata API into a dated raw partition."""
        from extract import main as extract_main
        return str(extract_main())

    @task
    def load(raw_path: str) -> dict:
        """Transform the raw partition and load it into Postgres."""
        import pandas as pd
        from transform import transform
        from load import (connection_status, load_tables,
                          prepare_quarantine, verify)

        raw = pd.read_parquet(raw_path)
        dim, insp, viol, quarantine = transform(raw)

        tables = {
            "dim_restaurant": dim,
            "fct_inspection": insp,
            "fct_violation": viol,
            "quarantine": prepare_quarantine(quarantine),
        }

        engine = connection_status()
        load_tables(engine, tables)
        verify(engine, tables)

        return {name: len(df) for name, df in tables.items()}

    load(extract())


inspections_pipeline()