"""
Simple weather data pipeline using dlt.
Fetches current weather and appends to PostgreSQL.
"""

from airflow.sdk import dag, task
from pendulum import datetime
import dlt
import requests


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    tags=["weather", "dlt"],
)
def open_meteo_to_postgres_dlt_v2():

    @task
    def load_weather():
        """Fetch weather data and load to PostgreSQL using dlt."""
        from airflow.sdk.bases.hook import BaseHook

        # Fetch weather data
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 52.50,
                "longitude": 13.41,
                "current": "temperature_2m,windspeed_10m",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        # Simple record
        weather_record = {
            "time": data["current"]["time"],
            "temperature": data["current"]["temperature_2m"],
            "windspeed": data["current"]["windspeed_10m"],
        }

        # Get postgres connection
        conn = BaseHook.get_connection("davies_rds_virgina")
        postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 5432}/{conn.schema or 'postgres'}"

        # Run dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name="weather_simple",
            destination=dlt.destinations.postgres(credentials=postgres_url),
            dataset_name="weather",
        )

        load_info = pipeline.run(
            [weather_record],
            table_name="weather_hourly",
            write_disposition="merge",  # Upsert to avoid duplicates
            primary_key="time",
        )
        print(f"Loaded: {load_info}")
        return {"status": "success"}

    load_weather()


open_meteo_to_postgres_dlt_v2()

