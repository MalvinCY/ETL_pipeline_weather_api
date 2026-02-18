# Weather ETL Pipeline

An end-to-end **Extract, Transform, Load** pipeline that pulls real-time weather data from the OpenWeatherMap API, transforms it into a structured analytics-ready format, and loads the results into an **AWS S3** bucket — all orchestrated by **Apache Airflow** running on an EC2 instance.

## Architecture

```
OpenWeatherMap API  ──►  Extract (Python)  ──►  Transform  ──►  Load  ──►  AWS S3
                              │                    │               │
                              └────── Apache Airflow DAG ──────────┘
                                     (EC2 Instance)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow |
| Compute | AWS EC2 |
| Storage | AWS S3 |
| Language | Python 3 |
| Data Source | OpenWeatherMap API |

## How It Works

1. **Extract** — A scheduled Airflow DAG triggers a Python task that calls the OpenWeatherMap API to retrieve current weather data (temperature, humidity, wind speed, pressure, conditions, etc.).

2. **Transform** — The raw JSON response is parsed and transformed into a flat, structured format with derived metrics such as temperature in Celsius/Fahrenheit, human-readable timestamps, and weather condition categories.

3. **Load** — The cleaned dataset is serialised to CSV and uploaded to a designated S3 bucket, partitioned by date for efficient downstream querying.

## Project Structure

```
ETL_pipeline_weather_api/
├── weather_dag.py       # Airflow DAG defining the ETL pipeline
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- AWS account with S3 access
- OpenWeatherMap API key

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/MalvinCY/ETL_pipeline_weather_api.git
   ```

2. Set your environment variables:
   ```bash
   export OPENWEATHER_API_KEY="your_api_key"
   export AWS_ACCESS_KEY_ID="your_access_key"
   export AWS_SECRET_ACCESS_KEY="your_secret_key"
   ```

3. Copy `weather_dag.py` into your Airflow DAGs folder:
   ```bash
   cp weather_dag.py ~/airflow/dags/
   ```

4. Start Airflow and trigger the DAG from the web UI or CLI.

## Author

**Malvin Siew** — [GitHub](https://github.com/MalvinCY) · [LinkedIn](https://www.linkedin.com/in/malvin-siew/)
