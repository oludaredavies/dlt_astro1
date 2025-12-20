"""
## Open Meteo to Snowflake with dlt

This DAG ingests weather data from the Open Meteo API every 5 seconds
and loads it into Snowflake using dlt (data load tool).

The pipeline:
1. Fetches current weather data from Open Meteo API for a configurable location
2. Uses dlt to automatically handle schema creation and data loading
3. Loads data into DEMO.DAVIES.WEATHERDATA in Snowflake
"""

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta
import requests
import dlt


# DAG configuration - runs every 5 seconds
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["weather", "dlt", "snowflake", "open-meteo"],
)
def open_meteo_to_snowflake():
    @task
    def extract_weather_data(latitude: float = 52.52, longitude: float = 13.41) -> dict:
        """
        Extract current weather data from Open Meteo API.
        Default location: Berlin, Germany

        Args:
            latitude: Latitude coordinate (default: Berlin)
            longitude: Longitude coordinate (default: Berlin)

        Returns:
            Dictionary containing weather data
        """
        # Open Meteo API endpoint (no API key required)
        url = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,cloud_cover,wind_speed_10m,wind_direction_10m",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        # Transform to flat structure for easier table loading
        weather_record = {
            "timestamp": data["current"]["time"],
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "timezone": data["timezone"],
            "temperature_2m": data["current"]["temperature_2m"],
            "relative_humidity_2m": data["current"]["relative_humidity_2m"],
            "apparent_temperature": data["current"]["apparent_temperature"],
            "precipitation": data["current"]["precipitation"],
            "weather_code": data["current"]["weather_code"],
            "cloud_cover": data["current"]["cloud_cover"],
            "wind_speed_10m": data["current"]["wind_speed_10m"],
            "wind_direction_10m": data["current"]["wind_direction_10m"],
        }

        return weather_record

    @task
    def load_to_snowflake_with_dlt(weather_data: dict) -> dict:
        """
        Load weather data to Snowflake using dlt.

        Args:
            weather_data: Dictionary containing weather data

        Returns:
            Load information from dlt
        """
        # Get Snowflake connection details from Airflow connection
        from airflow.sdk.bases.hook import BaseHook
        import os

        conn = BaseHook.get_connection("snowflake")
        extra = conn.extra_dejson

        # Set credentials via environment variables for dlt
        # Extract account identifier and remove .snowflakecomputing.com if present
        account = extra.get("account", "")
        if ".snowflakecomputing.com" in account:
            account = account.replace(".snowflakecomputing.com", "")

        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"] = extra.get(
            "database", "DEMO"
        )
        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"] = (
            conn.password or ""
        )
        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"] = conn.login or ""
        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"] = account
        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"] = extra.get(
            "warehouse", ""
        )
        os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"] = extra.get("role", "")

        # Configure dlt pipeline for Snowflake
        pipeline = dlt.pipeline(
            pipeline_name="open_meteo_weather",
            destination="snowflake",
            dataset_name="DAVIES",  # This will be the schema in Snowflake
        )

        # Load data - dlt will create table WEATHERDATA if it doesn't exist
        # Wrap single record in a list for dlt
        load_info = pipeline.run(
            [weather_data],
            table_name="WEATHERDATA",
            write_disposition="append",  # Append new records to existing table
        )

        # Return summary of the load
        return {
            "dataset_name": load_info.dataset_name,
            "destination_name": load_info.destination.destination_name,
            "destination_type": load_info.destination.destination_type,
            "first_run": load_info.first_run,
            "started_at": str(load_info.started_at),
            "finished_at": str(load_info.finished_at),
        }

    # Define task dependencies
    weather_data = extract_weather_data()
    load_to_snowflake_with_dlt(weather_data)


# Instantiate the DAG
open_meteo_to_snowflake()
