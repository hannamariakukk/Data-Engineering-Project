from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import zipfile
import pandas as pd

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Function to download a Kaggle dataset
def extract_csv(dataset: str, file_name: str, path: str):

    # Ensure the target path exists
    os.makedirs(path, exist_ok=True)

    zip_file_path = os.path.join(path, f"{dataset}.zip")

    # Extract the required file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        if file_name in zip_ref.namelist():
            zip_ref.extract(file_name, path)
            print(f"Extracted {file_name} to {path}")
        else:
            print(f"File {file_name} not found in the dataset ZIP.")

    os.remove(zip_file_path)
    print("Cleaned up ZIP file.")

# Function to process the data (simple example)
def process_csv_file(file_path: str, output_path: str):
    # Ensure the output folder exists
    os.makedirs(output_path, exist_ok=True)

    # Read CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Perform basic transformations
    df = df.dropna()

    # Construct output file path
    file_name = os.path.basename(file_path)
    output_file_path = os.path.join(output_path, f"processed_{file_name}")

    # Save the processed file
    df.to_csv(output_file_path, index=False)
    print(f"Processed {file_name} and saved to {output_file_path}")

# Define the DAG
with DAG(
    dag_id="kaggle_ingest_dag",
    default_args=default_args,
    description="A DAG to ingest data from a Kaggle dataset",
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Download the Kaggle dataset
    download_task = BashOperator(
        task_id="download_dataset",
        bash_command="""
        export KAGGLE_USERNAME=xubanarrieta
        export KAGGLE_KEY=fa3e758f30a59a47b42792436c2e2e6d
        kaggle datasets download daveianhickey/2000-16-traffic-flow-england-scotland-wales -p /opt/airflow/kaggle/accidents
        """,
    )
    
    # Task 2: Get the csv file
    
    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv,
        op_kwargs={
            "dataset": "2000-16-traffic-flow-england-scotland-wales",
            "file_name": "accidents_2012_to_2014.csv",
            "path": "/opt/airflow/kaggle/accidents",
        },
    )

    # Task 3: Process the data
    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process_csv_file,
        op_kwargs={
            "file_path": "/opt/airflow/kaggle/accidents/accidents_2012_to_2014.csv",
            "output_path": "/opt/airflow/kaggle/accidents/processed",
        },
    )

    # Define task dependencies
    download_task >> extract_task >> process_task
