import pandas as pd
import json
from dotenv import load_dotenv
import ast
import itertools
import os
import requests
import numpy as np

load_dotenv()

# 리스트 평탄화
def flatten_and_to_set(nested_lists):
    return list(set(list(itertools.chain.from_iterable(nested_lists))))

# 데이터 파싱
def optimized_provider_parse(data, column):

    dict_series = data[column].apply(ast.literal_eval)

    nested_lists_series = dict_series.apply(lambda x: list(x.values()))

    result = nested_lists_series.apply(flatten_and_to_set)

    return result

def table_normalization(data, column_list):
    nom_data = []
    for column in column_list:
        data_parsed = data[['imdb_id', column]]
        nom_data.append(data_parsed.explode(column).reset_index(drop=True))

    return nom_data

def parsing_columns(data, columns):
    for column in columns:
        data[column] = data[column].str.split(', ')
    return data


def get_genre_mapping(media_type='tv'):
    """
    TMDB API에서 장르 매핑 정보 가져오기

    Parameters:
    -----------
    api_key : str
        TMDB API 키
    media_type : str
        'movie' 또는 'tv'

    Returns:
    --------
    pd.DataFrame
        genre_id와 genre 이름이 매핑된 데이터프레임
    """

    # TMDB API에서 장르 매핑 정보 가져오기
    api_key = os.getenv("TMDB_API_KEY")

    url = f"https://api.themoviedb.org/3/genre/{media_type}/list?api_key={api_key}&language=en"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        genres = data.get('genres', [])

        if genres:
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

