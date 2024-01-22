# Airflow 도커 이미지
FROM apache/airflow:2.8.0

# 파이썬 의존성 목록 파일 현재 경로에 복사
COPY requirements.txt .
# 파이썬 의존성 목록 설치
RUN pip install -r requirements.txt
# 커스텀 모듈 사용을 위한 환경변수 설정
ENV PATH="$PATH:/plugins"
# ENV PYTHONPATH="$PYTHONPATH:/plugins"
ENV PYTHONPATH="$PYTHONPATH:/opt/Personal-DataLake/airflow/plugins"