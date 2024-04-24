# AWS_Airflow_Data_Pipelines

## Project Overview

In this project, sample music streaming data is extracted from Amazon S3, loaded into Amazon Redshift, and validated with data quality checks using Apache Airflow.

## Project Contents

### dags

#### File: final_project.py

- Description: Sets the parameters for the Airflow DAG including start date, number of retries upon failure, and schedule interval. Operators and SQL queries below are imported as tasked and dependencies between tasks are created to create the flow of the DAG.

### Helpers

#### File: sql_queries.py

 - Description: Contains necessary SQL queries to create a schema as a a future destination for the S3 data, stage data from S3 to Redshift, and load the staged data into Redshift dimension tables.

### Operators

#### File: stage_redshift.py

- Description: Loads JSON-formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided.

#### File: load_dimension.py

- Description: Using the SQL queries in sql_queries.py, staged data from staged_redshift.py is loaded into Redshift dimension tables.

#### File: load_fact.py

- Description: Using the SQL queries in sql_queries.py, staged data from staged_redshift.py is loaded into Redshift fact tables.

#### File: data_quality.py

- Description: Performs data quality checks to validate that data has been inserted into the redshift fact and dimension tables during load_dimension.py and load_fact.py.
