## INDEX
1. 프로젝트 목적
2. 개발환경 구조
3. 현재 진행상황


## 1.프로젝트 목적
1. **최초 목적** : KB부동산에서 제공하는 API들이 많이 공개되어있는데, 해당 API들의 수많은 파라미터에 따라 각각 중요한 인사이트를 찾을수 있는것을 알게되었다. 이를 한번에 단일테이블화 시키는 과정을 통해 인사이트 확보를 위함
2. **ETL 작업 수행** : 추출(Extract), 가공(Transform, Processing), 적재(Load) 과정을 일괄 수행함으로서 협업시 혹은 추가 작업수행시 능동적인 업무수행을 위함.
3. **대시보드를 통한 인사이트공유** : 대시보드를 통하여 인사이트를 전달하는것에 중요성을 알고있기에, 해당작업을 반드시 포함
4. **웹 배포를 통한 작품공유** : Streamlit에 배포하여 모두가 능동적인 분석을 할 수 있게 하기위함.

## 2.개발환경 구조
### A. 그림
<img src="https://github.com/ljs7463/Personal-Data-Lake/assets/66814045/27d83549-a6c0-4362-a995-323c14b6b787" align="left" width="1000" height="450">

### B. 폴더구조
(작성중)
### C. Airflow UI
![image](https://github.com/ljs7463/Personal-Data-Lake/assets/66814045/efbc6260-544a-4613-b0ee-e8c8c4e727df)


## 3. 진행상황 체크
~1. KB부동산 API 추출~

~2. KB부동산 단일테이블 가공작업~

~3. GOOGLE CLOUD BIGQUERY 적재~

~4. Airflow 도커 컨테이너 환경 구성~

~5. Airflow 로컬 파일 컨테이너 환경 마운트 설정~

~6. Airflow 월간 배치 파이프라인 DAG 세팅 및 스케쥴링 자동화 작업~

7. 대시보드 작업

8. Streamlit 컨테이너 작업 

9. Streamlit 배포작업
