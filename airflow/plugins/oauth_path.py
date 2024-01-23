import glob
import sys
import os
# GOOGLE_AUTH = '../core/config/gcp_oauth.json'
# 환경 변수에서 GOOGLE_AUTH 값을 가져옵니다. 환경 변수가 설정되어 있지 않으면 기본 경로를 사용합니다.
GOOGLE_AUTH = os.environ.get("BIGQUERY_AUTH_JSON_PATH", '../core/config/gcp_oauth.json')
