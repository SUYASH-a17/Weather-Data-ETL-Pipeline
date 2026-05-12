# Weather-Data-ETL-Pipeline

Built an automated ETL pipeline using Apache Airflow to ingest real-time data from the Open-Meteo API into a PostgreSQL database.

## Project Overview

This project was developed using the Astronomer CLI. It leverages the TaskFlow API for efficient data processing and containerized orchestration.

### Project Contents

- **dags**: Contains Python files for Airflow DAGs, including the weather ETL logic.
- **Dockerfile**: Versioned Astro Runtime image for the Airflow environment.
- **packages.txt**: OS-level packages required for the project.
- **requirements.txt**: Python dependencies (e.g., `requests`, `psycopg2`).
- **airflow_settings.yaml**: Local configuration for Airflow Connections and Variables.

## Local Development

1. **Start Airflow**: Run `astro dev start` to spin up the Docker containers (Postgres, Scheduler, Webserver, etc.).
2. **Access UI**: Once healthy, the Airflow UI is available at `http://localhost:8080/`.
3. **Database**: The metadata and target Postgres database can be accessed at `localhost:5432/postgres`.

---
*Maintained as part of a Data Engineering workflow.*