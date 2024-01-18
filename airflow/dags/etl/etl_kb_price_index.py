from PublicDataReader import Kbland
import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
# path = 'C:\\Users\\ljs74\\Documents\\GitHub\\Personal-Data-Lake\\airflow\\database'
sys.path.append('../../database')
# sys.path.append(path)
# sys.path.append(str(Path(os.getcwd())))
from gcp import BigqueryHandler, bigquery_client
from google.cloud import bigquery




# ========================================================================================
# 빅쿼리 헨들러 모듈 및 클라이언트 객체 생성
# ========================================================================================
BH = BigqueryHandler()
bq = bigquery_client


# ========================================================================================
# KB부동산 api객체 생성
# ========================================================================================
api = Kbland()


# ========================================================================================
# KB부동산 매매가격지수1 유형별 테이블 호출(Extract) 및 전처리(Transform)
# ========================================================================================
def make_price_index():
    """
    kB부동산 매매가격지수 모든 유형별 단일 테이블화 
    
    building_category
        '01':'아파트',
        '08':'연립',
        '09':'단독',
        '98':'주택종합'
        
    trade_category
        '01':'매매',
        '02':'전세'
        
    """
    building_category = ['01','08','09','98']
    trade_category = ['01','02']

    df_price_index = pd.DataFrame()
    itercount = 0

    for i in building_category:
        for j in trade_category:
            params = {
                "월간주간구분코드": "01",
                "매물종별구분": i,
                "매매전세코드": j,
                "지역코드": "",
                "기간": "",
            }
            df = api.get_price_index(**params)

            # api별로 컬럼명 변경을 위한 단어추출
            first_word = df[df.columns[:3]].iloc[0].values[0]
            second_word = df[df.columns[:3]].iloc[0].values[1]
            third_word = df[df.columns[:3]].iloc[0].values[2]
            last_word = df.columns[-1]
            new_col_name = first_word + ' ' + second_word + ' ' + third_word + ' ' + last_word

            # 지수컬럼명 변경
            df = df.rename(columns = {
                        last_word : new_col_name
                    })

            # 필요한 컬럼만 남기기
            df = df[df.columns[3:]]

            if itercount == 0:
                df_price_index = pd.concat([df_price_index, df])
            else:
                df_price_index = pd.merge(df_price_index, df, on = ['지역코드','지역명','날짜'], how = 'left')

            itercount += 1

    df_price_index = df_price_index.rename(columns = {
                                "지역코드":'regionCode',
                                '지역명':'regionName',
                                '날짜':'date',
                                '월간 아파트 매매 가격지수':"monthlyAptTradeIndex",
                                '월간 아파트 전세 가격지수':'monthlyAptJeonseIndex',
                                '월간 연립 매매 가격지수':'monthlyYeonripTradingIndex',
                                '월간 연립 전세 가격지수':'monthlyYeonripJeonseIndex',
                                '월간 단독 매매 가격지수':'monthlyDandokTradingIndex',
                                '월간 단독 전세 가격지수':'monthlyDandokJeonseIndex',
                                '월간 주택종합 매매 가격지수':'monthlyTotalTradingIndex',
                                '월간 주택종합 전세 가격지수':'monthlyTotalJeonseIndex'
                            })
    return df_price_index


# ========================================================================================
# task 코어 함수
# ========================================================================================
def task():
    """
    테스크 코어 함수
    """
    # 추출(Extract), 전처리(Transform) 함수 호출
    df = make_price_index()

    # 빅쿼리에 데이터셋 적재
    project = bq.project
    dataset_ref = bigquery.DatasetReference(project, 'kb_real_estate')
    new_table_id = 'etl_kb_price_index'

    table_ref = dataset_ref.table(new_table_id)
    schema = [
        bigquery.SchemaField('regionCode',"STRING"),
        bigquery.SchemaField('regionName',"STRING"),
        bigquery.SchemaField('date',"DATE"),
        bigquery.SchemaField('monthlyAptTradeIndex',"FLOAT"),
        bigquery.SchemaField('monthlyAptJeonseIndex',"FLOAT"),
        bigquery.SchemaField('monthlyYeonripTradingIndex',"FLOAT"),
        bigquery.SchemaField('monthlyYeonripJeonseIndex',"FLOAT"),
        bigquery.SchemaField('monthlyDandokTradingIndex',"FLOAT"),
        bigquery.SchemaField('monthlyDandokJeonseIndex',"FLOAT"),
        bigquery.SchemaField('monthlyTotalTradingIndex',"FLOAT"),
        bigquery.SchemaField('monthlyTotalJeonseIndex',"FLOAT"),
    ]
    table = bigquery.Table(table_ref, schema = schema)

    # 기존 테이블 생성여부에 따른 분기
    try:
        table = bq.create_table(table)
    except:
        pass

    job = bq.load_table_from_dataframe(df, table)
    job.result()
    return True
    



