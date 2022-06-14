import json
import os
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable, BaseOperator
import time
from slack import WebClient
from slack.errors import SlackApiError
import pandas as pd

# from iserikov_airflow import PG_CONN_ID, PG_TABLE, select_something, check_table_exist, select_query

DAG_ID = 'funcs_dag'
config = {
    DAG_ID: {"start_date": days_ago(1)},
}
path = Variable.get('slack_token', default_var='/home/igor/run')

dag = DAG(DAG_ID,
          catchup=False,
          schedule_interval=None,  # '@daily',
          default_args=config[DAG_ID])

PG_CONN_ID = 'postgres_airflow'
PG_TABLE = 'air_test_table'

with dag:


    @dag.task(multiple_outputs=True)
    def csv_down_f(url):
        # cont = requests.get(url).content
        # df = pd.read_csv(io.StringIO(cont.decode('utf-8')))
        print (os. getcwd() )

        df = pd.read_csv(url, encoding='windows-1252')
        name = 'tmp.csv'
        df.to_csv(name, index=False)
        print(df[:10])
        # df = pd.DataFrame(resp.content)
        return {'df_csv':name}

    csv_down = csv_down_f('https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv')


    @dag.task(multiple_outputs=True)
    def csv_count_f(file_name):
        df = pd.read_csv(file_name)
        df_count = df.groupby('Year')['Year'].count()
        print (df_count)
        return {'years_count':df_count.to_dict()}  #json.dumps(df_count.to_dict())}


    csv_count = csv_count_f(csv_down['df_csv'])


    @dag.task(multiple_outputs=True)
    def csv_print_f(years_dict):
        print(years_dict)
        return {'oll_good)': 'OK'}  # json.dumps(df_count.to_dict())}


    csv_print = csv_print_f(csv_count['years_count'])

    csv_down >> csv_count >> csv_print