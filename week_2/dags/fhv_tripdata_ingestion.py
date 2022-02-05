from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pandas as pd
from sqlalchemy import create_engine



USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB = os.getenv("DB")
TABLE_NAME = "fhv_data"

ENGINE = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')
FILENAME = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# FILENAME = 'for_hire_vehicle_tripdata_2021-01.csv'
OUTPUT_FILEPATH = '/opt/airflow/output_{FILENAME}'


def extract_data(filename, output_filepath):
    logging.info(f"--------------------------{filename}----------------")

    df = pd.read_csv(f'https://nyc-tlc.s3.amazonaws.com/trip+data/{filename}')
    df.to_csv(output_filepath, index=False)


def load_data(output_filepath):
    df_chunks = pd.read_csv(output_filepath, iterator=True, chunksize=100000)

    for i, df in enumerate(df_chunks):
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropoff_datetime = pd.to_datetime(df.dropoff_datetime)

        if i == 0:
            # Create table if required
            df.head(n=0).to_sql(name=TABLE_NAME, con=ENGINE, if_exists='append')

        df.to_sql(name=TABLE_NAME, con=ENGINE, if_exists='append')


local_workflow = DAG(
    "taxi_fhv_data",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 1)
)


with local_workflow:
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_kwargs=dict(
            filename=FILENAME,
            output_filepath=OUTPUT_FILEPATH
        ),
    )
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs=dict(
            output_filepath=OUTPUT_FILEPATH,
        ),
    )

    extract_data >> load_data