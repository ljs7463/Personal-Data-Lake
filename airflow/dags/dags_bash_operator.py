from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
# dag test
with DAG(
    dag_id = 'dags_bash_operator',
    schedule = "0 0 * * *",
    start_date = pendulum.datetime(2023,3,1,tz = 'Asia/Seoul'),
    catchup = False,
) as dag:
    bash_1 = BashOperator(
        task_id = 'bash_t1',
        bash_comman = 'echo 1'
    )
    bash_2 = BashOperator(
        task_id = 'bash_t2',
        bash_comman = 'echo $HOSTNAME'
    )

    bash_1 >> bash_2