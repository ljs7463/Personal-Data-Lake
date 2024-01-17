import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 커스텀 모듈
from etl.etl_kb_price_index import task

# ========================================================================================
# 시간대 설정
# ========================================================================================
KST = pendulum.timezone('Asia/Seoul')

# ========================================================================================
# DAG 정의
# ========================================================================================
dag = DAG(
    dag_id = 'KB부동산_가격지수_프로세스1',
    description = 'KB부동산 api 가격지수1',
    start_date = pendulum.datetime(2024,1,17, tzinfo = KST),
    catchup = False,
    tag = "KB",
    schedule_interval = "0 16 17 * *", # 매월 17일 16시 월간배치
)

# ========================================================================================
# TASK 정의
# ========================================================================================
task = PythonOperator(
    task_id = 'ETL_일괄_처리',
    python_callable = task,
    dag = dag
)

# ========================================================================================
# DAG 구조 정의
# ========================================================================================

# 단일 task구조
task