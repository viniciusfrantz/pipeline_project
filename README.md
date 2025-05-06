___
# Weather Forecast Data Pipeline (Snowflake, dbt, Airflow)
This project demonstrates a solution for automating the ingestion, transformation, and analysis of weather forecast data. Using advanced tools such as dbt with Apache Airflow, I orchestrate data transformations within Snowflake. Through Astronomer Cosmos, I run dbt models within Airflow DAGs, facilitating the automation of the entire process.
___
### Technologies
- **Airflow (Astronomer Cosmos)**: Orchestrates the pipeline and schedules tasks.
- **dbt**: Transforms and tests data in Snowflake.
- **Snowflake**: Stores and processes weather forecast data.
- **AWS S3**: Stores raw weather data before ingestion.
- **Docker**: Runs Airflow and dbt in isolated, consistent environments.
- **Git**: Tracks and manages project code.
- **Visual Studio Code**: The development environment used for writing and managing project code.
___
**Motivation:**
- In agriculture, weather plays a crucial role in crop production. A good rainfall at the right time can significantly impact yields, while poor weather conditions can bring challenges. For instance, after planting soybeans, immediate rainfall can cause issues like seedling failure or seed rot. Excessive rainfall can lead to localized flooding and root rot. Additionally, more accurate weather forecasts can help optimize irrigation decisions, preventing unnecessary irrigation when rain is expected, or ensuring timely irrigation when rainfall does not materialize. This project automates the orchestration, transformation, and analysis of weather forecast data, with the goal of evaluating the accuracy of weather predictions by comparing them to actual data from a local weather station.
___
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
___
### Pipeline Workflow
- **Extract**: New weather forecast data is fetched daily via API and saved to an AWS S3 bucket, orchestrated by Airflow.
- **Load** (Airflow with Astronomer Cosmos): Airflow automates the process of loading the raw data from S3 into Snowflake.
- **Transform** (dbt): dbt cleans and transforms the raw data into structured tables in Snowflake.
  - **Staging**: The data is cleaned and loaded into the staging table `stg_weather_forecast`.
  - **Aggregation**: Precipitation data is aggregated into the `agg_precipitation_forecast` table for analysis.
- **Test** (dbt): dbt runs tests to ensure data quality and identify issues early.
- **Analyze**: The processed data is analyzed by comparing forecasted precipitation with actual measurements to evaluate prediction accuracy.

___
### What I'm Working On
The project is still evolving, and right now, I’m focusing on integrating real-world weather station data to compare actual weather conditions with forecasts.

#### Upcoming Work:
- Integrating real weather station data into the pipeline.
- Evaluating forecast accuracy based on actual weather measurements.
- Improving forecast models to increase prediction accuracy.