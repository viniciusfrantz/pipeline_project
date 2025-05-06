# Weather Forecast Data Pipeline (Snowflake, dbt, Airflow)
Hi! I'm building a weather forecast data pipeline using modern tools like **dbt** with **Apache Airflow** to orchestrate data transformations in **Snowflake**. I utilized Astronomer Cosmos to run dbt models within Airflow DAGs.

This project automates the orchestration, transformation, and analysis of weather forecast data with the goal of evaluating the accuracy of weather predictions by comparing them to actual data from a local weather station.

### Technologies
- **Airflow (Astronomer Cosmos)**: Orchestrates the pipeline and schedules tasks.
- **dbt**: Transforms and tests data in Snowflake.
- **Snowflake**: Stores and processes weather forecast data.
- **AWS S3**: Stores raw weather data before ingestion.
- **Docker**: Runs Airflow and dbt in isolated, consistent environments.
- **Git**: Tracks and manages project code.

Visual Studio Code: The development environment used for writing and managing project code.
##   Project Structure

```
├── dags/
│   └── weather_dag.py
├── dbt_airflow_snowflake/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── raw/
│   │   │   └── raw_weather_forecast.sql
│   │   ├── staging/
│   │   │   └── stg_weather_forecast.sql
│   │   └── marts/
│   │       └── agg_precipitation_forecast.sql
├── include/
│   ├── constants.py
│   └── weather_utils.py
├── plugins/
├── tests/
├── Dockerfile
├── requirements.txt
└── README.md
```

### Pipeline Workflow
- **Extract**: R**Extract**: New weather forecast data is fetched daily via API and saved to an AWS S3 bucket, orchestrated by Airflow.
- **Load** (Airflow with Astronomer Cosmos): Airflow automates the process of loading the raw data from S3 into Snowflake.
- **Transform** (dbt): dbt cleans and transforms the raw data into structured tables in Snowflake.
  - **Staging**: The data is cleaned and loaded into the staging table `stg_weather_forecast`.
  - **Aggregation**: Precipitation data is aggregated into the `agg_precipitation_forecast` table for analysis.
- **Test** (dbt): dbt runs tests to ensure data quality and identify issues early.
- **Analyze**: The processed data is analyzed by comparing forecasted precipitation with actual measurements to evaluate prediction accuracy.


### What I'm Working On
The project is still evolving, and right now, I’m focusing on integrating real-world weather station data to compare actual weather conditions with forecasts.

#### Upcoming Work:
- Integrating real weather station data into the pipeline.
- Evaluating forecast accuracy based on actual weather measurements.
- Improving forecast models to increase prediction accuracy.
