from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests
import os

default_args = {
    'start_date': datetime(2024, 4, 1),
}

def download_file(url, output_path):
    print(f"Downloading from {url}")
    r = requests.get(url)
    r.raise_for_status()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(r.content)
    print(f"File saved to {output_path}")

def check_file_exists(path):
    print(f"Checking if file exists at {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found!")
    print(f"{path} exists.")

def download_meteo():
    url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=42183&Year=2024&Month=1&Day=31&time=LST&timeframe=1&submit=Download+Data"
    output_path = "/opt/airflow/data/raw/meteo.csv"
    download_file(url, output_path)

def download_pollution():
    url = "https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/202401_MONTHLY_AQHI_ON_SiteObs.csv"
    output_path = "/opt/airflow/data/raw/pollution.csv"
    download_file(url, output_path)

with DAG(
    dag_id="fusion_env_pipeline",
    schedule=None,
    default_args=default_args,
    catchup=False,
    description="Pipeline to download and process meteo and pollution data",
    tags=["example", "spark", "data-pipeline"],
) as dag:

    t1 = PythonOperator(task_id="download_meteo", python_callable=download_meteo)
    t2 = PythonOperator(task_id="download_pollution", python_callable=download_pollution)

    t1_check = PythonOperator(
        task_id="check_meteo_file",
        python_callable=lambda: check_file_exists("/opt/airflow/data/raw/meteo.csv"),
    )

    t2_check = PythonOperator(
        task_id="check_pollution_file",
        python_callable=lambda: check_file_exists("/opt/airflow/data/raw/pollution.csv"),
    )

    t3 = SparkSubmitOperator(
        task_id="transform_with_spark",
        application="/opt/airflow/spark_jobs/transform_data.py",
        conn_id="spark_default",
        application_args=[
            "/opt/airflow/data/raw/meteo.csv",
            "/opt/airflow/data/raw/pollution.csv",
            "/opt/airflow/data/processed/final_output.csv"
        ],
        conf={"spark.master": "spark://spark-master:7077"},
    )

    t3_check = PythonOperator(
        task_id="check_spark_job",
        python_callable=lambda: check_file_exists("/opt/airflow/data/processed/final_output.csv/_SUCCESS"),
    )

    # Task dependencies
    t1 >> t1_check
    t2 >> t2_check
    [t1_check, t2_check] >> t3 >> t3_check
