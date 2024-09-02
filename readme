# Brazil E-commerce ETL Pipeline

This repository contains a fully dockerized ETL pipeline for extracting, transforming, and loading Brazil e-commerce data from a MySQL database into a data warehouse using Apache Hive. The pipeline utilizes Apache Spark (written in Scala) to handle data processing and Apache Airflow to orchestrate and schedule the workflow.

## Project Overview

The pipeline performs the following steps:

1. **Data Extraction**: Extracts raw data from a MySQL database.
2. **Data Lake Storage**: Saves the extracted data into a data lake on HDFS (Hadoop Distributed File System).
3. **Data Transformation**: Uses Apache Spark to transform the data from the data lake into a fact table (`fact_sales`) and multiple dimension tables.
4. **Data Loading**: Loads the transformed data into a data warehouse managed by Apache Hive.
5. **Orchestration and Scheduling**: Uses Apache Airflow to orchestrate and schedule the entire ETL workflow.

![pipeline](./photos/pipeline.jpg)

Warehouse with star schema:

![schema](./photos/star_schema.png)

## Architecture

The architecture of the pipeline is built using the following technologies:

- **Apache Spark**: For data extraction, transformation, and loading.
- **Scala**: The programming language used for developing the Spark jobs.
- **HDFS (Hadoop Distributed File System)**: To store raw data as a data lake.
- **Apache Hive**: To serve as a data warehouse for storing the processed data.
- **Apache Airflow**: To orchestrate and schedule the ETL pipeline.
- **Docker**: To containerize all components and provide a reproducible environment.
- **Dataset**: [brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)
## Prerequisites

Before running the pipeline, ensure you have the following installed:

- **Docker** and **Docker Compose**: To run the containers.
- **Scala** and **SBT**: For compiling and building the Spark jobs.

## Getting Started

Follow these steps to set up and run the ETL pipeline:

1. **Clone the Repository**:
   ```sh
   git clone <your-repo-url>
   cd <repo-name>
   ```

2. **Build and Start the Docker Containers**:
   Use Docker Compose to build and start all services:
   ```sh
   docker-compose up --build
   ```

3. **Access Airflow Web UI**:
   Open your web browser and go to [http://localhost:8080](http://localhost:8080). Use the default credentials (`airflow` / `airflow`) to log in.

   Add connection:
   ![airflow-conn](./photos/add-spark-conn.png)

4. **Trigger the ETL Pipeline**:
   In the Airflow UI, trigger the DAG to start the ETL process.

## How the Pipeline Works

1. **Extract**: Airflow schedules a Spark job to extract data from the MySQL database.
2. **Load to Data Lake**: The extracted data is saved to HDFS as the raw data lake.
3. **Transform**: Another Spark job transforms the raw data into a fact table (`fact_sales`) and various dimension tables.
4. **Load to Data Warehouse**: The transformed tables are loaded into Hive for querying and analysis.

## Troubleshooting

If you encounter issues, check the logs available in the Airflow web interface or directly within the containers.

