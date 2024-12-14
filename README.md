# Data-Engineering-Project

## Data Engineering (LTAT.02.007) 

Group 4: Xuban Arrieta Mendiola, Hanna-Maria Kukk, Helena Sokk 

 

## Which datasets will we use? 

1.6 million UK traffic accidents, Kaggle, Dave Fisher-Hickey, https://www.kaggle.com/datasets/daveianhickey/2000-16-traffic-flow-england-scotland-wales/data?select=accidents_2012_to_2014.csv 

MIDAS OPEN: UK hourly weather observation data version 202407, CEDA Archive, Met Office, https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202407 

 

## Which questions will we want to find an answer to? 

1. What is the weather and location for which the most number of accidents have happened? 

2. Do some roads/locations have less traffic accidents even on days when the weather is dangerous? 

3. Does wind speed impact traffic accidents and if so what is the threshold of dangerously too high speed of wind? 

4. Does the seasonal changes impact traffic accidents and if so then what is the most dangerous month? 


## Project walkthrough

This project consists on ingesting two different datasets, joining them, and creating a DuckDB database using an Apache Airflow dag. We will use Docker to get Airflow nodes working. To do this, first we have to build the image, by using the next command:

`` docker compose build ``

This will build the Airflow image and take the necessary steps written in the Dockerfile file.

Next we have to create various directories, which will be used as volumes for the containers. These are the directories we will need:

`` mkdir config`

``mkdir dags``

``mkdir logs``

``mkdir plugins``

``mkdir kaggle``

``mkdir ceda``

``mkdir db ``

After creating the directories, we will have to create the directory ``weather_data`` inside the ``ceda`` directory. Since the code to download 

Now we can run the container with the command:

`` docker compose up ``

This will launch the containers, and after some time, we can open our browser and search `` localhost:8080 ``, which will open the Airflow GUI. 
If we scroll down in the DAGs tab, we will find the DAG ``create_db_dag.py``. We can trigger this DAG manually, and the process will start.

