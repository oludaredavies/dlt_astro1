"""
Open Meteo to PostgreSQL Blueprint Template

This Blueprint allows users to create weather data pipelines by simply
providing a YAML configuration file. No Python knowledge required!

Users can select which weather metrics to ingest from the available options.

Example YAML config:
    blueprint: open_meteo_postgres
    job_id: berlin_weather
    latitude: 52.52
    longitude: 13.41
    metrics:
      - temperature_2m
      - relative_humidity_2m
      - wind_speed_10m
"""

from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from airflow.decorators import task
from pendulum import datetime
from typing import List, Optional


# All available metrics from Open Meteo Current Weather API
# Users can choose any combination of these in their YAML files
AVAILABLE_METRICS = [
    # Temperature
    "temperature_2m",           # Air temperature at 2m height (°C)
    "apparent_temperature",     # Feels-like temperature (°C)
    
    # Humidity & Precipitation
    "relative_humidity_2m",     # Relative humidity at 2m height (%)
    "precipitation",            # Total precipitation (mm)
    "rain",                     # Rain amount (mm)
    "showers",                  # Showers amount (mm)
    "snowfall",                 # Snowfall amount (cm)
    
    # Atmospheric
    "weather_code",             # WMO weather code (see docs)
    "cloud_cover",              # Total cloud cover (%)
    "pressure_msl",             # Sea level pressure (hPa)
    "surface_pressure",         # Surface pressure (hPa)
    
    # Wind
    "wind_speed_10m",           # Wind speed at 10m height (km/h)
    "wind_direction_10m",       # Wind direction at 10m height (°)
    "wind_gusts_10m",           # Wind gusts at 10m height (km/h)
    
    # Other
    "is_day",                   # 1 = day, 0 = night
]

# Default metrics if user doesn't specify any
DEFAULT_METRICS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]


class OpenMeteoConfig(BaseModel):
    """
    Configuration schema for Open Meteo to PostgreSQL pipeline.
    
    Users specify these values in their YAML files.
    Blueprint validates all values before creating the DAG.
    """
    
    job_id: str = Field(
        description="Unique DAG identifier (e.g., 'berlin_weather', 'london_weather')"
    )
    
    latitude: float = Field(
        default=52.52,
        ge=-90,
        le=90,
        description="Location latitude (-90 to 90)"
    )
    
    longitude: float = Field(
        default=13.41,
        ge=-180,
        le=180,
        description="Location longitude (-180 to 180)"
    )
    
    # Configurable metrics - users choose which weather data to ingest
    metrics: List[str] = Field(
        default=DEFAULT_METRICS,
        description="List of weather metrics to ingest. See template for all options."
    )
    
    schedule: str = Field(
        default="*/5 * * * *",
        description="Cron schedule or preset (@hourly, @daily, @weekly)"
    )
    
    postgres_conn_id: str = Field(
        default="postgres_default",
        description="Airflow connection ID for PostgreSQL"
    )
    
    table_name: str = Field(
        default="weather_data",
        description="Target table name in PostgreSQL"
    )
    
    schema_name: str = Field(
        default="weather",
        description="Target schema (dataset) name in PostgreSQL"
    )
    
    owner: str = Field(
        default="data-team",
        description="DAG owner for Airflow UI and alerts"
    )
    
    retries: int = Field(
        default=2,
        ge=0,
        le=5,
        description="Number of task retries on failure (0-5)"
    )


class OpenMeteoPostgres(Blueprint[OpenMeteoConfig]):
    """
    Blueprint for ingesting weather data from Open Meteo API to PostgreSQL.
    
    This blueprint creates a DAG that:
    1. Fetches current weather data from Open Meteo API (free, no API key needed)
    2. Loads the data into PostgreSQL using dlt (data load tool)
    3. Uses merge/upsert to avoid duplicate records
    
    Users can configure which metrics to ingest via the 'metrics' field.
    """
    
    def render(self, config: OpenMeteoConfig) -> DAG:
        """
        Render the DAG from the provided configuration.
        """
        
        # Validate that all requested metrics are valid
        invalid_metrics = [m for m in config.metrics if m not in AVAILABLE_METRICS]
        if invalid_metrics:
            raise ValueError(
                f"Invalid metrics: {invalid_metrics}. "
                f"Available metrics: {AVAILABLE_METRICS}"
            )
        
        @task
        def load_weather(
            lat: float,
            lon: float,
            conn_id: str,
            table: str,
            schema: str,
            dag_id: str,
            metrics: List[str],
        ):
            """
            Extract weather data from Open Meteo and load to PostgreSQL.
            
            Dynamically fetches only the metrics specified in the config.
            """
            import dlt
            import requests
            from airflow.hooks.base import BaseHook
            
            # Build the metrics string for the API request
            metrics_param = ",".join(metrics)
            
            # Fetch weather data from Open Meteo API
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": metrics_param,
                    "timezone": "auto",
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            
            # Build weather record dynamically based on selected metrics
            weather_record = {
                "time": data["current"]["time"],
                "latitude": lat,
                "longitude": lon,
                "timezone": data.get("timezone"),
            }
            
            # Add each selected metric to the record
            for metric in metrics:
                # API returns metric values in the "current" object
                if metric in data["current"]:
                    weather_record[metric] = data["current"][metric]
            
            # Build PostgreSQL connection URL from Airflow connection
            conn = BaseHook.get_connection(conn_id)
            postgres_url = (
                f"postgresql://{conn.login}:{conn.password}"
                f"@{conn.host}:{conn.port or 5432}/{conn.schema or 'postgres'}"
            )
            
            # Configure and run dlt pipeline
            pipeline = dlt.pipeline(
                pipeline_name=f"open_meteo_{dag_id}",
                destination=dlt.destinations.postgres(credentials=postgres_url),
                dataset_name=schema,
            )
            
            # Load with merge to avoid duplicates
            load_info = pipeline.run(
                [weather_record],
                table_name=table,
                write_disposition="merge",
                primary_key=["time", "latitude", "longitude"],
            )
            
            print(f"Successfully loaded {len(metrics)} metrics: {metrics}")
            print(f"Load info: {load_info}")
            return {"status": "success", "metrics_loaded": metrics}
        
        # Build and return the DAG
        metrics_doc = "\n".join([f"  - {m}" for m in config.metrics])
        
        with DAG(
            dag_id=config.job_id,
            start_date=datetime(2025, 1, 1),
            schedule=config.schedule,
            catchup=False,
            default_args={
                "owner": config.owner,
                "retries": config.retries,
            },
            tags=["weather", "dlt", "blueprint", "open-meteo"],
            doc_md=f"""
## {config.job_id}

Weather data pipeline for location ({config.latitude}, {config.longitude}).

**Source:** Open Meteo API  
**Destination:** PostgreSQL `{config.schema_name}.{config.table_name}`  
**Schedule:** `{config.schedule}`

**Metrics being ingested:**
{metrics_doc}

*Generated by Blueprint from YAML configuration.*
            """,
        ) as dag:
            load_weather(
                lat=config.latitude,
                lon=config.longitude,
                conn_id=config.postgres_conn_id,
                table=config.table_name,
                schema=config.schema_name,
                dag_id=config.job_id,
                metrics=config.metrics,
            )
        
        return dag
