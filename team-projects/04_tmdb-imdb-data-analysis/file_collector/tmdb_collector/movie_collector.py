"""
TMDB 영화 상세 정보 수집 모듈

TMDB API를 사용하여 개별 영화의 상세 정보를 가져옵니다.
한 번의 API 호출로 기본 정보 + 크레딧 + OTT 제공사 + 키워드를
동시에 가져오는 append_to_response 기능을 활용합니다.

사용 예시:
    from file_collector.tmdb_collector.movie_collector import fetch_movie_details
    movie = fetch_movie_details(550)  # Fight Club의 TMDB ID
"""

from .config import *


def list_to_str(lst, key="name"):
    """
    딕셔너리 리스트에서 특정 키의 값을 쉼표로 연결한 문자열로 변환합니다.

    예시:
        [{"name": "Action"}, {"name": "Drama"}] → "Action, Drama"

    Args:
        lst: 딕셔너리들의 리스트
        key: 추출할 키 이름 (기본값: "name")

    Returns:
        str: 쉼표로 연결된 문자열
    """
    return ", ".join([str(item.get(key, "")) for item in lst]) if lst else ""


def fetch_movie_details(movie_id):
    """
    TMDB API에서 특정 영화의 상세 정보를 가져옵니다.

    하나의 API 호출로 영화의 기본 정보, 출연진(credits),
    OTT 제공사(watch/providers), 키워드(keywords)를 한꺼번에 수집합니다.

    Args:
        movie_id (int): TMDB 영화 ID

    Returns:
        dict: 영화 상세 정보 딕셔너리 (실패 시 None)
              - id, title, overview, genres 등 기본 정보
              - providers_flatrate/rent/buy: 국가별 OTT 제공사 정보
    """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    # append_to_response: 한 번의 요청으로 여러 데이터를 동시에 가져오는 TMDB 기능
    # credits(출연진), watch/providers(OTT 정보), keywords(키워드)를 추가 요청
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "append_to_response": "credits,watch/providers,keywords",
    }

    # API 호출 및 에러 처리
    try:
        response = session.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()  # 4xx, 5xx 에러 발생 시 예외 처리
        data = response.json()

    except Exception as e:
        print(f"Error fetching details for {movie_id}: {e}")
        return None

    # ============================================================
    # 장르 추출: [{"id": 28, "name": "Action"}, ...] → "28, 35" / "Action, Drama"
    # ============================================================
    genres_data = data.get("genres", [])
    genre_ids = list_to_str(genres_data, key="id")

    # ============================================================
    # 키워드 추출: 영화의 태그/키워드 목록
    # ============================================================
    keywords_block = data.get("keywords", {})
    if isinstance(keywords_block, dict):
        keywords_list = [k.get("name") for k in keywords_block.get("keywords", [])]
    else:
        keywords_list = []

    # ============================================================
    # OTT 제공사 추출: 국가별로 구독형/대여/구매 서비스를 분리
    # 예: {"KR": ["Netflix", "Disney Plus"], "US": ["Hulu"]}
    # ============================================================
    providers_data = data.get("watch/providers", {})
    providers_results = providers_data.get("results", {}) if isinstance(providers_data, dict) else {}

    # 구독형(flatrate): 월 구독으로 볼 수 있는 서비스 (Netflix, Disney+ 등)
    providers_flatrate = {}
    for country, info in providers_results.items():
        flatrate = info.get("flatrate", [])
        provider_names = [p["provider_name"] for p in flatrate]
        if provider_names:
            providers_flatrate[country] = provider_names

    # 대여형(rent): 건당 대여 서비스 (Google Play, Apple TV 등)
    providers_rent = {}
    for country, info in providers_results.items():
        rent = info.get("rent", [])
        provider_names = [p["provider_name"] for p in rent]
        if provider_names:
            providers_rent[country] = provider_names

    # 구매형(buy): 건당 구매 서비스
    providers_buy = {}
    for country, info in providers_results.items():
        buy = info.get("buy", [])
        provider_names = [p["provider_name"] for p in buy]
        if provider_names:
            providers_buy[country] = provider_names
    # -------------------------------------------------------------------

    # 최종 결과를 하나의 딕셔너리로 정리
    record = {
        # === 식별 정보 ===
        "id": data.get("id"),                    # TMDB 고유 ID
        "type": "movie",                          # 콘텐츠 유형 (영화)
        "imdb_id": data.get("imdb_id"),          # IMDb ID (리뷰/평점 수집에 사용)
        # === 포스터 ===
        "poster_path": data.get("poster_path"),
        # === 제목 ===
        "title": data.get("title"),              # 영어 제목
        "original_title": data.get("original_title"),  # 원어 제목
        # === 언어 ===
        "original_language": data.get("original_language"),
        "spoken_languages": list_to_str(data.get("spoken_languages", [])),
        # === 줄거리 ===
        "overview": data.get("overview"),        # 영화 줄거리 (분석의 핵심 텍스트)
        "tagline": data.get("tagline"),
        # === 상영 정보 ===
        "status": data.get("status"),            # Released, Post Production 등
        "release_date": data.get("release_date"),
        "runtime": data.get("runtime"),          # 상영 시간 (분)
        "adult": data.get("adult"),
        # === 재무 정보 ===
        "budget": data.get("budget"),            # 제작비 (USD)
        "revenue": data.get("revenue"),          # 수익 (USD)
        # === 평가 정보 ===
        "vote_count": data.get("vote_count"),    # TMDB 투표 수
        "vote_average": data.get("vote_average"),  # TMDB 평균 평점
        "popularity": data.get("popularity"),    # TMDB 인기도 점수
        # === 장르 & 키워드 ===
        "genres": list_to_str(genres_data),      # "Action, Drama"
        "genre_ids": genre_ids,                  # "28, 18"
        "keywords": ", ".join(keywords_list),
        # === 제작사 및 제작 국가 ===
        "production_companies": list_to_str(data.get("production_companies", [])),
        "production_countries": list_to_str(data.get("production_countries", [])),
        # === OTT 제공사 (국가별 딕셔너리) ===
        "providers_flatrate": providers_flatrate,  # 구독형
        "providers_rent": providers_rent,          # 대여형
        "providers_buy": providers_buy,            # 구매형
    }

    return record
