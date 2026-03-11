"""
TMDB API 공통 설정 모듈

이 파일은 TMDB API를 호출할 때 필요한 공통 설정을 담고 있습니다.
- HTTP 세션 생성 (실패 시 자동 재시도)
- API 키 로드 (.env 파일에서)
- 공통 유틸리티 함수

다른 collector 파일들(id_collector, movie_collector, tv_collector)이
이 파일을 import하여 공통 설정을 사용합니다.
"""

from requests.adapters import HTTPAdapter  # HTTP 연결 어댑터 (재시도, 연결 풀 관리)
from urllib3.util.retry import Retry       # 요청 실패 시 자동 재시도 설정
import requests                            # HTTP 요청 라이브러리
from dotenv import load_dotenv             # .env 파일에서 환경변수 읽기
import os
import json
from tqdm import tqdm                      # 진행률 표시 바
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # 월 단위 날짜 계산
from concurrent.futures import ThreadPoolExecutor, as_completed  # 멀티스레드 병렬 처리

# API 응답 형식을 JSON으로 지정하는 헤더
HEADERS = {"accept": "application/json"}


def create_session():
    """
    재시도 로직과 연결 풀이 있는 HTTP 세션을 생성합니다.

    - 서버 오류(500, 502, 503, 504) 발생 시 최대 3번 자동 재시도
    - 재시도 간격은 0.3초씩 증가 (backoff_factor)
    - 최대 50개의 동시 연결을 유지하는 연결 풀 사용

    Returns:
        requests.Session: 설정이 완료된 HTTP 세션 객체
    """
    session = requests.Session()
    # 재시도 정책: 서버 에러 시 최대 3번, 0.3초 간격으로 재시도
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    # 연결 풀: 동시에 최대 50개 연결 유지 (대량 요청 시 성능 향상)
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ============================================================
# 기본 준비: 환경변수에서 API_KEY 읽어오기
# ============================================================
# .env 파일에 TMDB_API_KEY=your_key 형태로 저장되어 있어야 합니다
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

# 전역 HTTP 세션 (모든 collector가 이 세션을 공유)
session = create_session()


def list_to_str(lst, key="name"):
    """
    딕셔너리 리스트에서 특정 키의 값을 추출하여 쉼표로 연결한 문자열을 반환합니다.

    예시:
        [{"name": "Action"}, {"name": "Drama"}] → "Action, Drama"

    Args:
        lst: 딕셔너리들의 리스트
        key: 추출할 키 이름 (기본값: "name")

    Returns:
        str: 쉼표로 연결된 문자열 (리스트가 비어있으면 빈 문자열)
    """
    return ", ".join([str(item.get(key, "")) for item in lst]) if lst else ""
