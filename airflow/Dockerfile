# Airflow 도커 이미지
FROM apache/airflow:2.8.1

# 파이썬 의존성 목록 파일 현재 경로에 복사
COPY requirements.txt .
# 파이썬 의존성 목록 설치
RUN pip install -r requirements.txt
# ENV PYTHONPATH="$PYTHONPATH:/plugins"
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow"
# ENV PYTHONPATH="$PYTHONPATH:/plugins"
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/plugins"
# ENV PYTHONPATH="$PYTHONPATH:core/config"
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/core/config"
# ENV BigQuery oauth info
ENV BIGQUERY_AUTH_JSON_PATH=/mnt/c/Users/ljs74/Documents/GitHub/Personal-Data-Lake/airflow/core/config/gcp_oauth.json
