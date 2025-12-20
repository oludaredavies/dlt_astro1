"""
## Open Meteo to PostgreSQL with dlt

This DAG ingests weather data from the Open Meteo API every 5 seconds
and loads it into PostgreSQL using dlt (data load tool).

dlt handles everything: extraction, schema inference, table creation, and loading.
"""

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta
import requests
import dlt
import hashlib
import json


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(seconds=5),
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["weather", "dlt", "postgres", "open-meteo"],
)
def open_meteo_to_postgres_dlt():
    @task
    def run_dlt_pipeline(latitude: float = 52.52, longitude: float = 13.41):
        """
        Single task that extracts from Open Meteo API and loads to PostgreSQL.
        dlt handles schema inference, table creation, and data loading automatically.
        """
        from airflow.sdk.bases.hook import BaseHook

        # Define data source - dlt will handle extraction
        def get_weather_data():
            """Generator function that yields weather data."""
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": "temperature_2m,windspeed_10m,winddirection_10m,weathercode",
                "timezone": "auto",
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Yield weather record - dlt will infer schema from this
            weather_record = {
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "time": data["current"]["time"],
                "temperature": data["current"]["temperature_2m"],
                "windspeed": data["current"]["windspeed_10m"],
                "winddirection": data["current"]["winddirection_10m"],
                "weathercode": data["current"]["weathercode"],
                "timezone": data.get("timezone"),
                "timezone_abbreviation": data.get("timezone_abbreviation"),
                "elevation": data.get("elevation"),
            }

            # Add CDC hash
            hash_data = json.dumps(weather_record, sort_keys=True)
            weather_record["data_hash"] = hashlib.sha256(hash_data.encode()).hexdigest()

            yield weather_record

        # Get PostgreSQL connection
        conn = BaseHook.get_connection("davies_rds_virgina")
        postgres_url = (
            f"postgresql://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port or 5432}/{conn.schema or 'postgres'}"
        )

        # Configure and run dlt pipeline - one line does everything!
        pipeline = dlt.pipeline(
            pipeline_name="open_meteo_weather",
            destination=dlt.destinations.postgres(credentials=postgres_url),
            dataset_name="weather",
        )

        # Run pipeline - dlt extracts, infers schema, creates table, and loads
        load_info = pipeline.run(
            get_weather_data(),
            table_name="weather_data_cdc",
            write_disposition="merge",
            primary_key=["latitude", "longitude", "time"],
        )

        print(
            f"Loaded {len(load_info.load_packages[0].jobs['completed_jobs'])} records"
        )
        return load_info.asdict()

    run_dlt_pipeline()


# Instantiate the DAG
open_meteo_to_postgres_dlt()
