"""
## Open Meteo to PostgreSQL with dlt

This DAG ingests weather data from the Open Meteo API every 5 seconds
and loads it into PostgreSQL using dlt (data load tool).

The pipeline:
1. Fetches current weather data from Open Meteo API for Berlin, Germany
2. Uses dlt to automatically infer schema and create/update table
3. Loads data into weather.weather_data_cdc table in PostgreSQL
"""

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta
import requests
import dlt
import hashlib
import json


# DAG configuration - runs every 5 seconds
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(seconds=5),
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["weather", "dlt", "postgres", "open-meteo", "cdc"],
)
def open_meteo_to_postgres_dlt():
    @task
    def extract_weather_data(latitude: float = 52.52, longitude: float = 13.41) -> dict:
        """
        Extract current weather data from Open Meteo API.
        Default location: Berlin, Germany

        Args:
            latitude: Latitude coordinate (default: Berlin)
            longitude: Longitude coordinate (default: Berlin)

        Returns:
            Dictionary containing weather data with CDC fields
        """
        # Open Meteo API endpoint (no API key required)
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

        # Create weather record matching the CDC table structure
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

        # Generate data hash for CDC (Change Data Capture)
        hash_data = json.dumps(weather_record, sort_keys=True)
        data_hash = hashlib.sha256(hash_data.encode()).hexdigest()
        weather_record["data_hash"] = data_hash

        return weather_record

    @task
    def load_to_postgres_with_dlt(weather_data: dict) -> dict:
        """
        Load weather data to PostgreSQL using dlt.
        dlt automatically handles schema inference and table creation.

        Args:
            weather_data: Dictionary containing weather data

        Returns:
            Load information from dlt
        """
        from airflow.sdk.bases.hook import BaseHook

        # Get PostgreSQL connection from Airflow
        conn = BaseHook.get_connection("davies_rds_virgina")

        # Build PostgreSQL connection string for dlt
        postgres_url = (
            f"postgresql://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port or 5432}/{conn.schema or 'postgres'}"
        )

        # Configure dlt pipeline for PostgreSQL
        pipeline = dlt.pipeline(
            pipeline_name="open_meteo_weather",
            destination=dlt.destinations.postgres(credentials=postgres_url),
            dataset_name="weather",  # This will be the schema in PostgreSQL
        )

        # Load data - dlt will automatically create table and infer schema
        # Using merge write disposition for CDC - upsert based on unique constraint
        load_info = pipeline.run(
            [weather_data],
            table_name="weather_data_cdc",
            write_disposition="merge",  # Upsert mode - updates if exists, inserts if not
            primary_key=["latitude", "longitude", "time"],  # Match unique constraint
        )

        return {
            "dataset_name": load_info.dataset_name,
            "first_run": load_info.first_run,
            "started_at": str(load_info.started_at),
            "finished_at": str(load_info.finished_at),
            "packages": [pkg.asdict() for pkg in load_info.load_packages],
        }

    # Define task dependencies
    weather_data = extract_weather_data()
    load_to_postgres_with_dlt(weather_data)


# Instantiate the DAG
open_meteo_to_postgres_dlt()
