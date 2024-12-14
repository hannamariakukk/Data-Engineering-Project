from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from geopy.distance import geodesic
import duckdb
import os
import zipfile
import pandas as pd
import requests

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

# Function to download the metadata file from CEDA
def download_ceda(url: str, output_path: str):
    os.makedirs(output_path, exist_ok=True)
    file_name = url.split("/")[-1]
    output_file_path = os.path.join(output_path, file_name)
    
    login_payload = {
        "username": "axuban",
        "password": "dataeng1",
    }

    with requests.Session() as session:
        # Log in to the website
        response = session.post(url, data=login_payload)

        if response.status_code == 200:
            print("Login successful!")

            # Download the file
            response = session.get(url, stream=True)
            if response.status_code == 200:
                with open(output_file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)
                print(f"Downloaded file to {output_file_path}")
            else:
                print(f"Failed to download the file. Status code: {response.status_code}")
                raise Exception(f"Download failed with status code {response.status_code}")
        else:
            print(f"Login failed. Status code: {response.status_code}")
            raise Exception(f"Login failed with status code {response.status_code}")
            
# Function to filter stations active during 2012-2014
def filter_stations(metadata_file: str, filtered_output: str):
    stations_df = pd.read_csv(metadata_file)
    filtered_stations = stations_df[
        (stations_df["first_year"] <= 2012) & (stations_df["last_year"] >= 2014)
    ]
    filtered_stations = filtered_stations[
        ["src_id", "station_latitude", "station_longitude", "station_file_name"]
    ]
    filtered_stations.to_csv(filtered_output, index=False)
    print(f"Filtered stations saved to {filtered_output}")
    
def match_accidents_with_stations(kaggle_file: str, stations_file: str, output_file: str):
    accidents_df = pd.read_csv(kaggle_file)
    stations_df = pd.read_csv(stations_file)

    def find_nearest_station(accident):
        accident_coords = (accident["Latitude"], accident["Longitude"])
        stations_df["Distance"] = stations_df.apply(
            lambda station: geodesic(
                accident_coords, 
                (station["station_latitude"], station["station_longitude"])
            ).kilometers,
            axis=1,
        )
        nearest_station = stations_df.loc[stations_df["Distance"].idxmin()]
        return pd.Series({
            "Nearest_Station_ID": nearest_station["src_id"],
            "Station_File_Name": nearest_station["station_file_name"],
            "Distance": nearest_station["Distance"]
        })

    matched_data = accidents_df.apply(find_nearest_station, axis=1)
    result_df = pd.concat([accidents_df, matched_data], axis=1)
    result_df.to_csv(output_file, index=False)
    print(f"Accidents matched with stations saved to {output_file}")

def load_csv_to_duckdb(csv_file_path: str, db_path: str):

    # Ensure the directory for the DuckDB database exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # Connect to DuckDB (creates the database file if it doesn't exist)
    conn = duckdb.connect(database=db_path, read_only=False)
    
    # Load CSV into DuckDB
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS dataset AS SELECT * FROM read_csv_auto('{csv_file_path}');
    """)
    conn.close()

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

    # Task 3: Download the metadata file from CEDA
    #download_task_ceda = PythonOperator(
    #    task_id="download_metadata",
    #    python_callable=download_ceda,
    #    op_kwargs={
    #        "url": "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202407/midas-open_uk-hourly-weather-obs_dv-202407_station-metadata.csv",
    #        "output_path": "/opt/airflow/ceda/weather_data",
    #    },
    #)
    
    # Task 4: Filter stations active during 2012-2014
    filter_stations_task = PythonOperator(
        task_id="filter_stations",
        python_callable=filter_stations,
        op_kwargs={
            "metadata_file": "/opt/airflow/ceda/weather_data/midas-open_uk-hourly-weather-obs_dv-202407_station-metadata.csv",
            "filtered_output": "/opt/airflow/ceda/weather_data/stations_2012_2014.csv",
        },
    )
    
    # Task 5: Match accidents to the nearest weather station
    match_accidents_task = PythonOperator(
        task_id="match_accidents",
        python_callable=match_accidents_with_stations,
        op_kwargs={
            "kaggle_file": "/opt/airflow/kaggle/accidents/accidents_2012_to_2014.csv",
            "stations_file": "/opt/airflow/ceda/weather_data/stations_2012_2014.csv",
            "output_file": "/opt/airflow/ceda/weather_data/matched_accidents_with_stations.csv",
        },
    )

    #Task 6: convert csv file to DuckDB database
    database_task = PythonOperator(
        task_id="create_db",
        python_callable=load_csv_to_duckdb,
        op_kwargs={
            "csv_file_path": "/opt/airflow/ceda/weather_data/matched_accidents_with_stations.csv",
            "db_path": "/opt/airflow/db/accidents_2012_to_2014.duckdb",
        },
    )

    # Define task dependencies
    download_task >> extract_task >> filter_stations_task >> match_accidents_task >> database_task
