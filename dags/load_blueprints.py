"""
Blueprint DAG Loader

This file auto-discovers all .dag.yaml files in the dags/configs/ directory
and generates Airflow DAGs from them using Blueprint templates.

How it works:
1. Airflow parses this file on startup and every 30 seconds
2. Blueprint scans dags/configs/ for *.dag.yaml files
3. Each YAML file is validated against its Blueprint template
4. Valid configs become fully-functional DAGs

To add a new DAG:
1. Create a new YAML file in dags/configs/ (e.g., my_city_weather.dag.yaml)
2. Specify the blueprint and parameters
3. The DAG appears automatically in Airflow UI

Example YAML (dags/configs/berlin_weather.dag.yaml):
    blueprint: open_meteo_postgres
    job_id: berlin_weather
    latitude: 52.52
    longitude: 13.41
    postgres_conn_id: my_postgres
"""

from blueprint import discover_yaml_dags

# Discover all YAML configs and generate DAGs
# This returns a dict of {dag_id: DAG} which we merge into globals()
# so Airflow can find and register them
generated_dags = discover_yaml_dags("dags/configs/")

# Expose all generated DAGs to Airflow
globals().update(generated_dags)

