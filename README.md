# air-spark
A ready to use Airflow and Spark stack

## Installation

1. `docker compose build` to build the custom image of Airflow (based on Apache Airflow 2.10.4) with all the tools needed for spark execution

2. `docker compose up` to launch the stack

3. Acess Airflow UI on localhost:8088 (8080 port being used for spark UI) user password combo is the following `airflow:airflow`

4. Put your dags in `dags/` and your spark jobs in `spark_jobs/`

## Disclaimer

This stack is only for testing purpose only and not for devlopment or production as it is not set up entirely. You can find more information about setup on the Apache Documentation.

Currently in version 2.10.4 because I was not able to make it work on version 2.10.5 nor 3.0.0 due to JWT Token problems.

