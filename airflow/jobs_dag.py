# Serikov IO
import uuid
from datetime import datetime
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import time


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": days_ago(1), "table_name": "table_name_1"},
    'dag_id_2': {'schedule_interval': None, "start_date": days_ago(1), "table_name": "table_name_2"},
    'dag_id_3': {'schedule_interval': None, "start_date": days_ago(1), "table_name": "table_name_3"}
}

PG_CONN_ID = 'postgres_default'
PG_TABLE = 'air_test_table2'

def select_query(sql_to_get, postgres_conn_id):
    """ callable function to get schema name and after that check if table exist """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print(sql_to_get)
    # get schema name
    try:
        query = hook.get_records(sql=sql_to_get)
        print(query[0])
        return query
    except Exception as e:
        print(e)
        return None

def query_task_func(sql_to_get, **kwargs):
    print('sql_to_get=' + str(sql_to_get))
    print ('query_task_func='+str(kwargs))
    kwargs['ti'].xcom_push(key='query_task_result', value=1)
    #time.sleep(15)
    for key in kwargs:
        print(key)


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, name: str, postgres_conn_id: str, schema: str, table:str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table

    def execute(self, context):
        print('context=' + str(context))
        # hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        sql = 'SELECT COUNT(*) FROM {}.{};'.format(self.schema, self.table)
        # result = hook.get_first(self.sql)
        ret = select_query(sql, self.postgres_conn_id)[0][0]
        print('ret=' + str(ret))
        task_instance = context['task_instance']
        task_instance.xcom_push(self.name, ret)
        task_instance.xcom_push("return_key", "{{run_id}} ended")
        #task_instance.xcom_push("ex_date", "{{run_id}} ended")


def create_dag(dag_id):
    def print_process_start_f(**kwargs):
        print('----------------------------')
        print('Hello World-->')
        print('ARGS:==' + str(kwargs))
        execution_date = kwargs['execution_date'].isoformat()
        print ('execution_date_jobs='+str(execution_date))
        kwargs['ti'].xcom_push(key='execution_date_jobs', value=execution_date)

    def check_table_exist():
        """ method to check that table exists """
        #time.sleep(1)
        exist = select_query('SELECT COUNT(*) FROM {};'.format(PG_TABLE), PG_CONN_ID)
        if exist:
            return 'insert_row'
        return 'create_table'

    dag = DAG(dag_id,
              schedule_interval=None,  # '@daily',
              # schedule_interval=schedule,
              catchup=False,
              default_args=config[dag_id])

    with dag:
        print_process_start = PythonOperator(
            task_id='print_process_start',
            python_callable=print_process_start_f)

        bash_task = BashOperator(task_id='get_current_user',
                                 bash_command="whoami")

        check_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist,
            provide_context=True,
            dag=dag)

        #create_table = DummyOperator(task_id='create_table')
        create_table = PostgresOperator(
            task_id='create_table',
            postgres_conn_id=PG_CONN_ID,
            sql='''CREATE TABLE {}(
                        custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
                        );'''.format(PG_TABLE),
        )
        #create_table_false = DummyOperator(task_id='create_table_false')
        insert_row = PostgresOperator(
            task_id='insert_row',
            postgres_conn_id=PG_CONN_ID,
            sql='INSERT INTO {} VALUES(%s, %s, %s)'.format(PG_TABLE),
            trigger_rule=TriggerRule.ALL_DONE,
            parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
        )
        #### old version
        '''query_task = PythonOperator(task_id='query_task',
                    python_callable=query_task_func,
                                    op_args=['SELECT COUNT(*) FROM {};'.format(PG_TABLE),])
        '''
        query_task_spec = PostgreSQLCountRows(task_id="PostgreSQLCountRows-query_task", name="qwe", postgres_conn_id=PG_CONN_ID,
                                    schema='public',
                                    table=PG_TABLE)

        # ---------------------------
        print_process_start.set_downstream(bash_task)
        bash_task.set_downstream(check_table_exist)
        check_table_exist.set_downstream(create_table)
        create_table.set_downstream(insert_row)
        insert_row.set_downstream(query_task_spec)
        ##
        check_table_exist.set_downstream(insert_row)

    return dag


# build a dag for each number in range(10)
for dag_id in config:
    print(dag_id, config[dag_id])

    globals()[dag_id] = create_dag(dag_id)
    break
