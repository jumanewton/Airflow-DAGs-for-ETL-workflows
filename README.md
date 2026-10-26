# AutoDSB

A data pipeline project combining Apache Airflow DAGs for ETL workflows and a FastAPI application, containerized with Docker.

## Overview

This project contains:
- **Airflow DAGs**: ETL pipelines for fetching and processing data
- **FastAPI App**: A simple web application
- **PostgreSQL**: Database for storing processed data
- **Docker Setup**: Containerized development environment

## Project Structure

```
autodsb/
├── dags/                   # Airflow DAG definitions
│   ├── etlweather.py       # ETL DAG for OpenWeatherMap data
│   └── exampledag.py       # Example DAG from Astronomer
├── tests/                  # DAG tests
├── main.py                 # FastAPI application
├── docker-compose.yml      # Docker services configuration
├── Dockerfile              # Astro Runtime image
├── requirements.txt        # Python dependencies
└── airflow_settings.yaml   # Local Airflow configuration
```

## Features

### Weather ETL Pipeline (`etl_weather`)
The `etlweather.py` DAG performs a daily ETL job that:
1. **Extract**: Fetches current weather data from OpenWeatherMap API for New York City (lat: 40.7128, lon: -74.0060)
2. **Transform**: Extracts temperature, humidity, pressure, weather description, wind speed, and timestamp
3. **Load**: Inserts transformed data into a PostgreSQL `weather` table

### FastAPI Application
A basic FastAPI app running on port 8000 that prints "Hello from autodsb!".

## Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) (optional, for local Airflow development)

## Setup

### 1. Configure Environment Variables

Create a `.env` file in the root directory with your OpenWeatherMap API key:

```env
YOUR_API_KEY=your_openweathermap_api_key
```

### 2. Start the Services

Using Docker Compose:

```bash
docker-compose up --build
```

This will start:
- PostgreSQL on `localhost:5432` (database: `autodsb`, user: `postgres`, password: `postgres`)
- FastAPI app on `localhost:8000`

### 3. Set Up Airflow Connections

For the weather ETL DAG to work, you'll need to configure the following Airflow connections:

- `openweathermap_api`: HTTP connection to OpenWeatherMap API
- `postgres_default`: PostgreSQL connection to the database

You can configure these via Airflow UI or `airflow_settings.yaml`.

### 4. Create the Weather Table

In PostgreSQL, create the `weather` table:

```sql
CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    humidity INTEGER,
    pressure INTEGER,
    weather_description VARCHAR(255),
    wind_speed FLOAT,
    timestamp TIMESTAMP
);
```

## Usage

- **FastAPI**: Visit [http://localhost:8000](http://localhost:8000)
- **Airflow**: If using Astro CLI, run `astro dev start` to launch the Airflow UI

## Development

This project uses [Astronomer Runtime](https://www.astronomer.io/docs/astro/runtime-image-architecture) 3.3-7 as the base image.

### Running Tests

```bash
pytest
```

## Dependencies

- `apache-airflow-providers-http`
- `apache-airflow-providers-postgres`

## Notes

- The weather DAG runs daily (`@daily`) with `catchup=False`
- Coordinates are set to New York City by default - modify `LATITUDE` and `LONGITUDE` in `dags/etlweather.py` as needed
- The API key should be stored securely in Airflow connections or environment variables
