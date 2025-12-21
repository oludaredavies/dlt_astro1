"""
Simple Weather Data Pipeline using dlt (data load tool)

This DAG demonstrates a minimal ETL pipeline that:
1. Extracts current weather data from the Open Meteo API
2. Loads it directly into PostgreSQL using dlt

dlt handles the heavy lifting: schema inference, table creation, 
type mapping, and incremental loading — all with minimal code.

Target: weather.weather_hourly table in PostgreSQL
Schedule: Every 5 minutes
"""

from airflow.sdk import dag, task
from pendulum import datetime
import dlt
import requests


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["weather", "dlt"],
)
def open_meteo_to_postgres_dlt_v2():
    """Main DAG function. The @dag decorator converts this into an Airflow DAG."""

    @task
    def load_weather():
        """
        Single task that extracts weather data and loads it to PostgreSQL.
        
        We import BaseHook inside the task (not at module level) because
        Airflow connections are only available at runtime, not at DAG parse time.
        """
        from airflow.sdk.bases.hook import BaseHook

        # =================================================================
        # STEP 1: EXTRACT - Fetch weather data from Open Meteo API
        # =================================================================
        # Open Meteo is a free weather API that requires no API key.
        # We request current weather for Berlin (lat: 52.50, lon: 13.41)
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 52.50,       # Berlin's latitude
                "longitude": 13.41,      # Berlin's longitude
                "current": "temperature_2m,windspeed_10m",  # Which metrics we want
            },
            timeout=30,  # Fail if API doesn't respond within 30 seconds
        )
        
        # Raise an exception if the API returns an error (4xx, 5xx status codes)
        # This will cause the Airflow task to fail and potentially retry
        response.raise_for_status()
        
        # Parse the JSON response into a Python dictionary
        data = response.json()

        # =================================================================
        # STEP 2: TRANSFORM - Shape the data into our desired format
        # =================================================================
        # We extract only the fields we care about.
        # dlt will use these keys as column names in PostgreSQL.
        weather_record = {
            "time": data["current"]["time"],           # ISO timestamp, e.g. "2025-12-20T14:00"
            "temperature": data["current"]["temperature_2m"],  # Temperature in Celsius
            "windspeed": data["current"]["windspeed_10m"],     # Wind speed in km/h
        }

        # =================================================================
        # STEP 3: LOAD - Send data to PostgreSQL using dlt
        # =================================================================
        
        # Retrieve the PostgreSQL connection details from Airflow's connection store.
        # This keeps credentials out of the code and makes the DAG portable.
        conn = BaseHook.get_connection("davies_rds_virgina")
        
        # Build the connection URL that dlt expects
        # Format: postgresql://username:password@host:port/database
        postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 5432}/{conn.schema or 'postgres'}"

        # Create a dlt pipeline. This is the core abstraction in dlt.
        # - pipeline_name: Identifier for tracking state (stored locally in ~/.dlt/)
        # - destination: Where to load data (PostgreSQL in our case)
        # - dataset_name: The schema name in PostgreSQL (creates "weather" schema)
        pipeline = dlt.pipeline(
            pipeline_name="weather_simple",
            destination=dlt.destinations.postgres(credentials=postgres_url),
            dataset_name="weather",
        )

        # Execute the pipeline!
        # - [weather_record]: The data to load (dlt accepts lists, generators, or dataframes)
        # - table_name: Creates/updates the "weather_hourly" table
        # - write_disposition="merge": Upsert behavior — update if exists, insert if not
        # - primary_key="time": The column(s) used to detect duplicates
        #
        # On first run: dlt creates the schema and table automatically
        # On subsequent runs: dlt merges new data based on the primary key
        load_info = pipeline.run(
            [weather_record],
            table_name="weather_hourly",
            write_disposition="merge",
            primary_key="time",
        )
        
        # Log what happened (visible in Airflow task logs)
        print(f"Pipeline completed: {load_info}")
        
        # Return value is stored in Airflow's XCom for downstream tasks to use
        return {"status": "success"}

    # Call the task to add it to the DAG
    load_weather()


# Instantiate the DAG by calling the decorated function
# This is required for Airflow to discover and register the DAG
open_meteo_to_postgres_dlt_v2()
