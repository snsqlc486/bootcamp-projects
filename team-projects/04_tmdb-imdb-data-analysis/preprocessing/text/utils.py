"""
전처리 유틸리티 함수 모음

main_processor.py가 사용하는 데이터 파싱 및 정규화 헬퍼 함수들입니다.

주요 기능:
- OTT 제공사 문자열 파싱 (JSON → 리스트)
- 테이블 정규화 (1행 = 1값 형태로 분해)
- 쉼표 구분 컬럼 파싱
- TMDB API에서 장르 ID ↔ 이름 매핑 조회
"""

import pandas as pd
import json
from dotenv import load_dotenv
import ast        # 문자열을 파이썬 자료구조로 안전하게 변환
import itertools  # 중첩 리스트 평탄화
import os
import requests
import numpy as np

load_dotenv()


def flatten_and_to_set(nested_lists):
    """
    중첩 리스트를 평탄화하고 중복을 제거한 리스트를 반환합니다.

    예시:
        [["Netflix", "Disney+"], ["Netflix", "Hulu"]]
        → ["Netflix", "Disney+", "Hulu"]  (중복 제거)

    Args:
        nested_lists: 리스트들의 리스트

    Returns:
        list: 중복 없이 평탄화된 리스트
    """
    return list(set(list(itertools.chain.from_iterable(nested_lists))))


def optimized_provider_parse(data, column):
    """
    JSON 문자열로 저장된 OTT 제공사 정보를 파싱합니다.

    수집 단계에서 OTT 제공사는 국가별로 다음처럼 저장됩니다:
        '{"KR": ["Netflix"], "US": ["Netflix", "Hulu"]}'

    이 함수는 국가 구분 없이 모든 제공사를 하나의 리스트로 합칩니다.
    예: '{"KR": ["Netflix"], "US": ["Hulu"]}' → ["Netflix", "Hulu"]

    Args:
        data (pd.DataFrame): 원본 데이터프레임
        column (str): JSON 문자열이 저장된 컬럼명

    Returns:
        pd.Series: 각 행의 제공사 리스트
    """
    # 문자열 → dict로 변환 (ast.literal_eval은 eval보다 안전)
    dict_series = data[column].apply(ast.literal_eval)

    # dict의 값(국가별 제공사 리스트)만 추출
    nested_lists_series = dict_series.apply(lambda x: list(x.values()))

    # 중첩 리스트 평탄화 + 중복 제거
    result = nested_lists_series.apply(flatten_and_to_set)

    return result


def table_normalization(data, column_list):
    """
    리스트 형태의 컬럼을 정규화 테이블로 변환합니다 (1행 = 1값).

    데이터베이스 정규화와 유사한 개념입니다.
    예: imdb_id=tt001, genres=["Action", "Drama"]
      → (tt001, Action), (tt001, Drama) 두 행으로 분해

    Args:
        data (pd.DataFrame): 원본 데이터 (imdb_id 컬럼 필요)
        column_list (list[str]): 정규화할 컬럼명 리스트

    Returns:
        list[pd.DataFrame]: 각 컬럼에 대한 정규화 테이블 리스트
    """
    nom_data = []
    for column in column_list:
        # imdb_id와 해당 컬럼만 선택
        data_parsed = data[['imdb_id', column]]
        # 리스트를 행으로 분해 (explode)
        nom_data.append(data_parsed.explode(column).reset_index(drop=True))

    return nom_data


def parsing_columns(data, columns):
    """
    쉼표로 구분된 문자열 컬럼을 리스트로 변환합니다.

    예시:
        "Action, Drama" → ["Action", "Drama"]

    Args:
        data (pd.DataFrame): 원본 데이터프레임 (in-place 수정)
        columns (list[str]): 변환할 컬럼명 리스트

    Returns:
        pd.DataFrame: 수정된 데이터프레임
    """
    for column in columns:
        data[column] = data[column].str.split(', ')
    return data


def get_genre_mapping(media_type='tv'):
    """
    TMDB API에서 장르 ID와 장르 이름의 매핑 정보를 가져옵니다.

    수집 단계에서는 장르가 숫자 ID로 저장됩니다 (예: 28, 35).
    이 함수로 ID를 이름으로 변환하는 테이블을 얻습니다.
    예: 28 → "Action", 35 → "Comedy"

    Args:
        media_type (str): 'movie' 또는 'tv' (장르 목록이 다름)

    Returns:
        pd.DataFrame: genre_id, genre 두 컬럼을 가진 데이터프레임
                      (API 호출 실패 시 빈 데이터프레임)
    """
    api_key = os.getenv("TMDB_API_KEY")

    url = f"https://api.themoviedb.org/3/genre/{media_type}/list?api_key={api_key}&language=en"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        genres = data.get('genres', [])

        if genres:
            # [{"id": 28, "name": "Action"}, ...] → DataFrame
            genres_df = pd.DataFrame(genres).rename(columns={'id': 'genre_id', 'name': 'genre'})
            return genres_df
        else:
            print("장르 데이터를 찾을 수 없거나 응답이 비어 있습니다.")
            return pd.DataFrame()

    except requests.exceptions.HTTPError as err:
        print(f"HTTP 오류 발생: {err}")
        print("API 키를 확인하거나 요청 URL을 확인해 보세요.")
        return pd.DataFrame()
    except requests.exceptions.RequestException as err:
        print(f"요청 오류 발생: {err}")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print("응답을 JSON으로 디코딩하는 데 실패했습니다.")
        return pd.DataFrame()
