from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient import discovery
from oauth_path import GOOGLE_AUTH
import time

# GCP 인증정보 객체
credentials = service_account.Credentials.from_service_account_file(GOOGLE_AUTH)

# 빅쿼리 클라이언트 객체
bigquery_client = bigquery.Client(credentials = credentials)

# 스토리지 클라이언트 객체
# storage_client = storage.Client(credentials=credentials)

# 클라우드 SQL 클라이언트 객체
sql_client = discovery.build("sqladmin", "v1beta4", credentials=credentials)


# 빅쿼리 핸들러 클래스
class BigqueryHandler:
    """
    빅쿼리 핸들러 클래스
    """

    def __init__(self):
        pass

    # def create_bucket(self, bucket_name):
    #     """
    #     버킷 생성
    #     """
    #     # 버킷 객체 정의
    #     bucket = storage_client.bucket(bucket_name)
    #     # 스토리지 클래스 - 스탠다드
    #     bucket.storage_class = "STANDARD"
    #     # 버킷 생성
    #     self.bucket = storage_client.create_bucket(bucket, location="asia-northeast3")
    #     self.logger.debug(f"버킷 생성 완료 - {bucket_name}")

    # def select_bucket(self, bucket_name):
    #     """
    #     버킷 선택
    #     """
    #     # 버킷 선택
    #     self.bucket = storage_client.get_bucket(bucket_name)
    #     self.logger.debug(f"버킷 선택 완료 - {bucket_name}")

    def upload_to_gcs(self, file_path):
        """
        CSV 데이터를 GCS Bucket에 적재
        """
        # CSV 파일 경로 목록
        target_path_list = glob.glob(f"{file_path}/*.csv")
        for target_path in target_path_list:
            # 버킷 Blob 이름
            file_name = target_path.split("\\")[-1]
            blob_name = file_name
            # 업로드
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(target_path)
            time.sleep(5)
        self.logger.debug(f"버킷 업로드 완료 - {file_name}")

    def upload_to_gbq(self, dataset_id, table_id_name, job_config):
        """
        GCS Bucket의 Blob을 GBQ Table에 적재
        - dataset_id: 데이터세트 아이디
        - table_id_name: 테이블 아이디
        - job_config: 테이블 스키마 정보
        """
        # 버킷 내 블랍 리스트
        blobs = self.bucket.list_blobs()
        # 카테고리별 블랍 GBQ Table 업로드 수행
        for blob in blobs:
            # GCS에서 데이터 URI 가져오기
            uri = f"gs://{self.bucket.name}/{blob.name}"
            # 빅쿼리 테이블ID
            table_id = f"aidepartners.{dataset_id}.{table_id_name}"
            # GBQ 적재 인스턴스 정의
            load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
            # GCS Bucket 내 Blob을 GBQ Table에 적재
            load_job.result()
            # 테이블 요약 정보 가져오기
            destination_table = bigquery_client.get_table(table_id)

    def read_table(self, sql):
        """
        빅쿼리 테이블을 판다스 데이터프레임으로 반환
        """
        try:
            return bigquery_client.query(sql).to_dataframe()
        except:
            self.logger.debug(f"Error")
            pass

    def gbq_to_gbq(self, sql, dataset_id, table_id, location):
        """
        빅쿼리 결과를 빅쿼리 테이블로 저장
        - sql: 쿼리
        - dataset_id: 빅쿼리 데이터세트 ID
        - table_id: 빅쿼리 테이블 ID
        - location: 빅쿼리 테이블 위치
        """
        # 쿼리 결과를 새로운 테이블로 저장
        job_config = bigquery.QueryJobConfig()
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)
        job_config.destination = table_ref
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        query_job = bigquery_client.query(sql, location=location, job_config=job_config)
        return query_job.result()

    def gbq_to_gcs(self, project, dataset_id, table_id, bucket_name, location, blob_name):
        """
        빅쿼리 결과를 스토리지에 저장
        - project: 빅쿼리 프로젝트 명
        - dataset_id: 빅쿼리 데이터세트 ID
        - table_id: 빅쿼리 테이블 ID
        - bucket_name: 스토리지 버킷 명
        - location: 스토리지 위치
        - blob_name: 블랍 명
        """
        # 테이블을 GCS로 내보내기
        file_type = "*.csv"
        destination_uri = f"gs://{bucket_name}/{blob_name}/{file_type}"
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.job.ExtractJobConfig()
        # job_config.compression = bigquery.Compression.GZIP     # 압축 설정
        job_config.print_header = False  # Header 제외
        extract_job = bigquery_client.extract_table(
            table_ref, destination_uri, location=location, job_config=job_config
        )
        extract_job.result()

    def gcs_to_cloud_sql(
        self, project, bucket_name, blob_name, instance, database_name, table_name,
    ):
        """
        스토리지 버킷 블랍을 클라우드 SQL DB 테이블에 적재
        - project
        - bucket_name
        - blob_name
        - instance
        - database_name
        - table_name
        """
        # 블랍 하위 목록
        blob_name_list = self.get_blob_name_list(bucket_name, blob_name)

        # 블랍 하위 목록 순회하며 테이블 적재 수행
        for blob in blob_name_list:

            self.logger.debug(f"DB 적재 시작 - 블랍: {blob}")

            # Request Body
            instances_import_request_body = {
                "importContext": {
                    "fileType": "CSV",
                    "uri": f"gs://{bucket_name}/{blob}",
                    "database": database_name,
                    "csvImportOptions": {"table": table_name,},
                }
            }

            request = sql_client.instances().import_(
                project=project, instance=instance, body=instances_import_request_body
            )
            response = request.execute()

            self.logger.debug(f"DB 적재 완료 - 블랍: {blob}")
            time.sleep(60)  # Pause
        return True

    def get_blob_name_list(self, bucket_name, blob_name):
        """
        블랍 하위 파일 목록 리스트 조회
        """
        # 버킷 선택
        bucket = storage_client.get_bucket(bucket_name)
        # 버킷 내 블랍 리스트
        blobs = bucket.list_blobs(prefix=blob_name)
        # 블랍 하위 파일 목록 리스트
        blob_name_list = []
        for blob in blobs:
            blob_name_list.append(blob.name)
        return blob_name_list

    # def delete_blob(self, bucket_name, blob_name):
    #     """
    #     블랍 삭제
    #     """
    #     # 버킷 선택
    #     bucket = storage_client.get_bucket(bucket_name)
    #     # 버킷 내 블랍 리스트
    #     blobs = bucket.list_blobs(prefix=blob_name)
    #     # 블랍 삭제
    #     for blob in blobs:
    #         self.logger.debug(f"블랍 삭제 시작 - {blob.name}")
    #         blob.delete()
    #         self.logger.debug(f"블랍 삭제 완료 - {blob.name}")

    def get_gbq_table_schema(self, project, dataset_id, table_id):
        """
        빅쿼리 테이블 스키마 정보 조회
        """
        dataset_ref = bigquery_client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)
        table = bigquery_client.get_table(table_ref)
        return table.schema

    def get_mysql_ddl_query(self, schema, table_id):
        """
        빅쿼리 스키마를 MySQL 테이블 정의 쿼리로 변환
        """
        data_type_dict = {
            "STRING": "TEXT",
            "INTEGER": "INT",
            "FLOAT": "DOUBLE",
            "NUMERIC": "DOUBLE",
            "DATETIME": "DATETIME",
            "DATE": "DATE",
            "GEOGRAPHY": "LONGTEXT",
        }
        mode_dict = {"REQUIRED": "NOT NULL", "NULLABLE": "NULL"}
        schema_string = ""
        for s in schema:
            string = f"""`{s.name}` {data_type_dict[s.field_type]} {mode_dict[s.mode]},\n"""
            schema_string += string
        sql = f"""
CREATE TABLE `{table_id}` (
{schema_string[:-2]}
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
        return sql

    def df_to_gbq(self, df, dataset_id, table_id, datatype_dict):
        """
        데이터프레임을 빅쿼리 테이블에 적재
        """
        project = bigquery_client.project
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        schema = list(
            map(lambda x: bigquery.SchemaField(x[0][0], x[0][1]), zip(datatype_dict.items()))
        )
        table = bigquery.Table(table_ref, schema=schema)
        try:
            table = bigquery_client.create_table(table)
        except:
            pass
        job = bigquery_client.load_table_from_dataframe(df, table)
