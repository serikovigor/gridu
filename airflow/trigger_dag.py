from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import time
from slack import WebClient
from slack.errors import SlackApiError

from airflow.sensors.filesystem import FileSensor
from airflow.utils.decorators import apply_defaults
from typing import Any, Optional, Collection, Iterable, Callable

DAG_ID = 'trigger_dag'
config = {
    DAG_ID: {"start_date": days_ago(1)},
}

TRIGGER_FILE = '/home/igor/run'

path = Variable.get('name_path_variable', default_var='/home/igor/run')
slack_token = Variable.get('slack_token', default_var='54321')


# ------------------------------
class SmartFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')  # <- Required

    @apply_defaults
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):  # <- Required
        result = (
                not self.soft_fail
                and super().is_smart_sensor_compatible()
        )
        return result


# --------------------------------
def print_process_start_f(**kwargs):
    global execution_date_slave
    print('----------------------------')
    print('Hello World-->')
    print('ARGS:==' + str(kwargs))



def print_result_f(**kwargs):
    print('----------------------------')
    print('Hello World-->')
    print('ARGS:==' + str(kwargs))
    ret_status = kwargs['ti'].xcom_pull(task_ids='PostgreSQLCountRows-query_task', key='return_key')
    print('ret_status from PostgreSQLCountRows-query_task=' + str(ret_status))


from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun


@provide_session
def _get_execution_date_of_dag_a(exec_date, session=None, **kwargs):
    # global execution_date_slave
    print(str(kwargs))
    # dag_a_last_run = execution_date_slave
    execution_date_jobs = kwargs['ti'].xcom_pull(dag_id='dag_id_1',
                                                 task_ids='print_process_start',
                                                 key='execution_date_jobs',
                                                 include_prior_dates=True)
    ret_val = datetime.fromisoformat(execution_date_jobs)
    print('_get_execution_date_of_dag_a=' + str(ret_val))
    return ret_val


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )
    with dag_subdag:
        external_task_sensor = ExternalTaskSensor(
            task_id='external_task_sensor',
            poke_interval=5,
            timeout=180,
            # soft_fail=False,
            ##retries=2,
            #external_task_id=None,
            external_task_id='PostgreSQLCountRows-query_task',
            # execution_delta=timedelta(minutes=2),
            execution_date_fn=_get_execution_date_of_dag_a,
            external_dag_id='dag_id_1',
            dag=dag_subdag)

        print_result = PythonOperator(
            task_id='print_process_start',
            python_callable=print_result_f)

        # bash_task_del = BashOperator(task_id='bash_task_del', bash_command='rm {}'.format(path))
        bash_task_del = BashOperator(task_id='bash_task_del',
                                     bash_command="echo '123'")

        bash_task_time = BashOperator(task_id='bash_task_timestamp',
                                      bash_command="echo '123' > /home/igor/dif/dag_ended_{{ts_nodash}}.txt")

        external_task_sensor >> print_result >> bash_task_del >> bash_task_time

    return dag_subdag


def send_to_slack(txt):
    global slack_token  # = 'xoxb-3629295791028-...'
    client = WebClient(token=slack_token)
    try:
        response = client.chat_postMessage(
            channel="general",
            text=txt)
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


dag = DAG(DAG_ID,
          # schedule_interval=schedule,
          catchup=False,
          schedule_interval=None,
          default_args=config[DAG_ID]
          )

with dag:
    '''t1 = FileSensor(task_id="file_sensor_task", poke_interval=5,
                    filepath=TRIGGER_FILE,
                    timeout=60)'''
    '''# . 0
    print_process_start = PythonOperator(
        task_id='print_process_start',
        python_callable=print_process_start_f)'''

    # 1. SENSOR TASK
    sensor = SmartFileSensor(
        task_id=f'waiting_for_file_1',
        filepath=TRIGGER_FILE,
        # fs_conn_id='fs_default'
    )

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dag_id_1",
        wait_for_completion=True,
        reset_dag_run=True,
        execution_date='{{ds}}'
    )
    # trigger_dependent_dag = DummyOperator(task_id='trigger_dependent_dag')

    # delete_file = BashOperator(task_id='delete_file', bash_command="rm {file}".format(file=TRIGGER_FILE))
    '''external_task_sensor = ExternalTaskSensor(
        task_id='external_task_sensor',
        poke_interval=5,
        timeout=180,
        # soft_fail=False,
        ##retries=2,
        external_task_id='PostgreSQLCountRows-query_task',
        # execution_delta=timedelta(minutes=2),
        execution_date_fn=_get_execution_date_of_dag_a,
        external_dag_id='dag_id_1',
        dag=dag)'''

    process_result = SubDagOperator(
        task_id="process_result",
        subdag=load_subdag(
            parent_dag_name=DAG_ID,
            child_dag_name="process_result",
            args=config[DAG_ID]
        ),
        default_args=config[DAG_ID],
        dag=dag,
    )
    slack_task = PythonOperator(task_id='slack_task',
                                python_callable=send_to_slack,
                                op_kwargs={'txt': '123'})

    sensor >> trigger_dependent_dag  >> process_result >> slack_task
