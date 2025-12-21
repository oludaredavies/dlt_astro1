# Weather Data Pipelines with Airflow + dlt

Ingest weather data from [Open Meteo API](https://open-meteo.com/) into PostgreSQL using Apache Airflow and [dlt](https://dlthub.com/).

**No Python required!** Create new weather pipelines by simply adding a YAML file.

---

## 🚀 Quick Start

### 1. Start Airflow

```bash
astro dev start
```

Open http://localhost:8080 (username: `admin`, password: `admin`)

### 2. Create a New Weather Pipeline

Copy the template and customize it:

```bash
cp dags/configs/_TEMPLATE.dag.yaml dags/configs/tokyo_weather.dag.yaml
```

Edit the file with your city's coordinates and desired metrics. That's it! The DAG appears automatically in Airflow.

---

## 📁 Project Structure

```
dlt_astro1/
├── .astro/
│   └── templates/
│       └── open_meteo_postgres.py   # Blueprint template (don't edit unless extending)
├── dags/
│   ├── configs/                      # ⭐ Add your YAML files here
│   │   ├── _TEMPLATE.dag.yaml        # Copy this to create new pipelines
│   │   ├── berlin_weather.dag.yaml
│   │   ├── lagos_weather.dag.yaml
│   │   └── ...
│   └── load_blueprints.py            # Auto-discovers YAML configs
├── requirements.txt
└── README.md
```

---

## 📝 Creating a New DAG

### Step 1: Copy the Template

```bash
cp dags/configs/_TEMPLATE.dag.yaml dags/configs/my_city_weather.dag.yaml
```

### Step 2: Edit the YAML File

```yaml
blueprint: open_meteo_postgres

# Required: Unique name for your DAG
job_id: paris_weather

# Your city's coordinates (find at https://www.latlong.net/)
latitude: 48.85
longitude: 2.35

# How often to fetch data
schedule: "@hourly"

# Which weather metrics to ingest (uncomment what you need)
metrics:
  - temperature_2m
  - relative_humidity_2m
  - wind_speed_10m
  - precipitation
  # - apparent_temperature
  # - cloud_cover
  # ... see template for all options

# PostgreSQL connection
postgres_conn_id: my_postgres_connection
table_name: paris_weather
schema_name: weather
```

### Step 3: Done!

Airflow automatically discovers the new file and creates the DAG. Refresh the UI to see it.

---

## 🌡️ Available Metrics

Choose any combination of these metrics in your YAML:

| Category | Metric | Description |
|----------|--------|-------------|
| **Temperature** | `temperature_2m` | Air temperature at 2m (°C) |
| | `apparent_temperature` | Feels-like temperature (°C) |
| **Humidity** | `relative_humidity_2m` | Relative humidity (%) |
| **Precipitation** | `precipitation` | Total precipitation (mm) |
| | `rain` | Rain amount (mm) |
| | `showers` | Showers amount (mm) |
| | `snowfall` | Snowfall amount (cm) |
| **Atmospheric** | `weather_code` | WMO weather code |
| | `cloud_cover` | Cloud cover (%) |
| | `pressure_msl` | Sea level pressure (hPa) |
| | `surface_pressure` | Surface pressure (hPa) |
| **Wind** | `wind_speed_10m` | Wind speed at 10m (km/h) |
| | `wind_direction_10m` | Wind direction (degrees) |
| | `wind_gusts_10m` | Wind gusts (km/h) |
| **Other** | `is_day` | 1 = day, 0 = night |

---

## ⏰ Schedule Options

| Schedule | Meaning |
|----------|---------|
| `"*/5 * * * *"` | Every 5 minutes |
| `"*/15 * * * *"` | Every 15 minutes |
| `"@hourly"` | Every hour |
| `"0 */6 * * *"` | Every 6 hours |
| `"@daily"` | Once per day (midnight) |
| `"@weekly"` | Once per week |

---

## 🗄️ Database Output

Each pipeline creates a table in PostgreSQL with:

- **Schema:** As specified in `schema_name` (default: `weather`)
- **Table:** As specified in `table_name`
- **Columns:** `time`, `latitude`, `longitude`, `timezone`, + your selected metrics

**Deduplication:** Uses `time + latitude + longitude` as primary key. Running the same pipeline multiple times won't create duplicates.

---

## 🔧 Configuration Reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `blueprint` | ✅ | - | Must be `open_meteo_postgres` |
| `job_id` | ✅ | - | Unique DAG identifier |
| `latitude` | ❌ | 52.52 | Location latitude (-90 to 90) |
| `longitude` | ❌ | 13.41 | Location longitude (-180 to 180) |
| `metrics` | ❌ | Basic set | List of metrics to ingest |
| `schedule` | ❌ | `*/5 * * * *` | Cron expression or preset |
| `postgres_conn_id` | ❌ | `postgres_default` | Airflow connection ID |
| `table_name` | ❌ | `weather_data` | Target table name |
| `schema_name` | ❌ | `weather` | Target schema name |
| `owner` | ❌ | `data-team` | DAG owner |
| `retries` | ❌ | 2 | Retries on failure (0-5) |

---

## 🛠️ Setting Up PostgreSQL Connection

1. Go to Airflow UI → Admin → Connections
2. Click "+" to add a new connection
3. Fill in:
   - **Connection Id:** `my_postgres_connection` (use this in your YAML)
   - **Connection Type:** Postgres
   - **Host:** Your database host
   - **Schema:** Database name
   - **Login:** Username
   - **Password:** Password
   - **Port:** 5432

---

## 📚 Examples

### Minimal Config

```yaml
blueprint: open_meteo_postgres
job_id: simple_weather
postgres_conn_id: my_postgres
```

### Full Config

```yaml
blueprint: open_meteo_postgres
job_id: comprehensive_weather
latitude: 35.68
longitude: 139.69
schedule: "@hourly"
metrics:
  - temperature_2m
  - apparent_temperature
  - relative_humidity_2m
  - precipitation
  - rain
  - weather_code
  - cloud_cover
  - wind_speed_10m
  - wind_direction_10m
  - wind_gusts_10m
  - pressure_msl
postgres_conn_id: production_postgres
table_name: tokyo_weather
schema_name: weather_data
owner: analytics-team
retries: 3
```

---

## 🔗 Resources

- [Open Meteo API Docs](https://open-meteo.com/en/docs)
- [dlt Documentation](https://dlthub.com/docs)
- [Astronomer Blueprint](https://github.com/astronomer/blueprint)
- [Airflow Documentation](https://airflow.apache.org/docs/)

---

## 💡 Tips

- **Find coordinates:** Use [latlong.net](https://www.latlong.net/) to find any city's coordinates
- **Test locally:** Run `astro dev start` to test before deploying
- **Check logs:** If a DAG fails, check the task logs in Airflow UI
- **Validate YAML:** Make sure your YAML syntax is correct (proper indentation, no tabs)

---

## 🚀 Deploy to Production

```bash
astro deploy
```

See [Astronomer docs](https://www.astronomer.io/docs/astro/deploy-code/) for deployment instructions.
