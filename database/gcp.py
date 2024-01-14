from google.cloud import bigquery
from google.oauth2 import service_account
from path import GOOGLE_AUTH

# GCP 인증정보 객체
credentials = service_account.Credentials.from_service_account_file(GOOGLE_AUTH)

# 빅쿼리 클라이언트 객체
bigquery_client = bigquery.Client(credentials = credentials)

print(bigquery_client)

# 빅쿼리 핸들러 클래스

class BigqueryHandler:
    """
    빅쿼리 핸들러 클래스
    """

    def __init__(self):
        pass

    def execute_query(self, sql):
        """
        빅쿼리 쿼리 실행
        """
        try:
            query_job = bigquery_client.query(sql)
            result = query_job.result()
            return result
        except Exception as e:
            return e

    def read_table(self, sql):
        """
        빅쿼리 조회 -> pandas.DataFrame
        """
        try:
            return bigquery_client.query(sql).to_dataframe()
        except Exception as e:
            print(e)

    def read_list_of_dict(self, sql):
        """
        빅쿼리 조회 -> list of dictionary
        """
        try:
            return [dict(row) for row in bigquery_client.query(sql).result()]
        except Exception as e:
            print(e)

    def insert_rows_to_bigquery(self, table_id, rows):
        """
        빅쿼리 테이블 인서트
        - List of dictionary
        """
        try:
            errors = bigquery_client.insert_rows_json(table_id, rows)
            return errors
        except Exception as e:
            print(e)