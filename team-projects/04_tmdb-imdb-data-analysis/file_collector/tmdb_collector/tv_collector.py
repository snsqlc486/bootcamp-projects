"""
TMDB TV 시리즈 및 시즌 상세 정보 수집 모듈

TMDB API를 사용하여 TV 시리즈의 상세 정보를 수집합니다.
영화와 달리 TV 시리즈는 시즌/에피소드 구조가 있어서,
시리즈 정보와 시즌 정보를 따로 수집한 뒤 합칩니다.

주요 기능:
- fetch_tv_series_details(): 시리즈 기본 정보 (출연진, OTT, 키워드 포함)
- fetch_tv_season_details(): 개별 시즌 정보 (에피소드 수, 평균 런타임)
- fetch_tv_series_and_seasons(): 시리즈 + 모든 시즌을 한꺼번에 수집
"""

from .config import *
import json


def list_to_str(lst, key="name"):
    """
    딕셔너리 리스트에서 특정 키의 값을 쉼표로 연결한 문자열로 변환합니다.

    예시: [{"name": "Drama"}, {"name": "Thriller"}] → "Drama, Thriller"
    """
    return ", ".join([str(item.get(key, "")) for item in lst]) if lst else ""


# ============================================================
# TV Series 수집
# ============================================================
def fetch_tv_series_details(tv_id):
    """
    TMDB API에서 TV 시리즈의 상세 정보를 수집합니다.

    append_to_response를 사용하여 한 번의 요청으로
    출연진, OTT 제공사, 키워드, 외부 ID(IMDb), 리뷰를 동시에 가져옵니다.

    Args:
        tv_id (int): TMDB TV 시리즈 ID

    Returns:
        dict: 시리즈 상세 정보 (실패 시 None)
    """
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    # aggregate_credits: TV 전용 출연진 정보 (여러 시즌에 걸친 통합 크레딧)
    # external_ids: IMDb ID 등 외부 서비스 ID
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "append_to_response": "aggregate_credits,watch/providers,keywords,external_ids,reviews",
    }

    try:
        response = session.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching TV series {tv_id}: {e}")
        return None

    # === IMDb ID 추출 (리뷰/평점 수집에 필요) ===
    imdb_id_data = data.get("external_ids", {})
    imdb_id = imdb_id_data.get("imdb_id", "")

    # === 장르 추출 ===
    genres_data = data.get("genres", [])
    genre_ids = list_to_str(genres_data, key="id")

    # === 키워드 추출 (TV는 "results" 키 사용, 영화는 "keywords" 키 사용) ===
    keywords_block = data.get("keywords", {})
    if isinstance(keywords_block, dict):
        keywords_list = [k.get("name") for k in keywords_block.get("results", [])]
    else:
        keywords_list = []

    # === 출연진/제작진 추출 (aggregate_credits: TV 전용 통합 크레딧) ===
    aggregate_credits = data.get("aggregate_credits", {})
    cast_list = aggregate_credits.get("cast", [])
    crew_list = aggregate_credits.get("crew", [])

    # 인기순으로 정렬된 상위 배우 5명
    top_cast = [c.get("name") for c in cast_list[:5] if c.get("name")]

    # 감독과 작가를 제작진 목록에서 분류
    # TV는 여러 에피소드에 다른 감독/작가가 있으므로 jobs 배열을 확인
    directors = set()
    writers = set()
    for c in crew_list:
        name = c.get("name")
        if name:
            jobs = c.get("jobs", [])
            for job in jobs:
                job_name = job.get("job", "")
                if "Director" in job_name:
                    directors.add(name)
                if job_name in ["Writer", "Screenplay", "Story"]:
                    writers.add(name)

    # === OTT 제공사 추출 (국가별로 구독/대여/구매 분리) ===
    providers_data = data.get("watch/providers", {})
    providers_results = providers_data.get("results", {}) if isinstance(providers_data, dict) else {}

    providers_flatrate = {}  # 구독형 (Netflix, Disney+ 등)
    providers_rent = {}      # 대여형
    providers_buy = {}       # 구매형

    for country, info in providers_results.items():
        flatrate = info.get("flatrate", [])
        if flatrate:
            provider_names = [p.get("provider_name") for p in flatrate if p.get("provider_name")]
            if provider_names:
                providers_flatrate[country] = provider_names

        rent = info.get("rent", [])
        if rent:
            provider_names = [p.get("provider_name") for p in rent if p.get("provider_name")]
            if provider_names:
                providers_rent[country] = provider_names

        buy = info.get("buy", [])
        if buy:
            provider_names = [p.get("provider_name") for p in buy if p.get("provider_name")]
            if provider_names:
                providers_buy[country] = provider_names

    # === TMDB 리뷰 추출 (최대 5개, 200자까지만) ===
    review_items = data.get("reviews", {}).get("results", [])
    review_text = " || ".join([
        f"{r.get('author', '')}({r.get('author_details', {}).get('rating', '')}): "
        f"{(r.get('content') or '').replace(chr(10), ' ')[:200]}"
        for r in review_items[:5]
    ])

    # === 방송 네트워크 (NBC, HBO 등) ===
    networks = data.get("networks", [])
    network_names = list_to_str(networks)
    network_ids = list_to_str(networks, key="id")

    # === 시즌 정보 요약 (예: "S1: Season 1 (10 eps); S2: Season 2 (8 eps)") ===
    seasons_data = data.get("seasons", [])
    seasons_summary = "; ".join([
        f"S{s.get('season_number')}: {s.get('name')} ({s.get('episode_count')} eps)"
        for s in seasons_data if s.get("season_number") is not None
    ])

    # 마지막으로 방영된 에피소드 정보
    last_ep = data.get("last_episode_to_air", {})

    # === 최종 결과 딕셔너리 조립 ===
    record = {
        # --- 식별 정보 ---
        "id": data.get("id"),
        "type": "tv_series",
        "imdb_id": imdb_id,
        "title": data.get("name"),

        "adult": data.get("adult"),
        "backdrop_path": data.get("backdrop_path"),

        # --- 제작 정보 ---
        "created_by": list_to_str(data.get("created_by", [])),
        "episode_run_time": ", ".join(map(str, data.get("episode_run_time", []))) or "",

        # --- 방영 정보 ---
        "first_air_date": data.get("first_air_date"),

        # --- 장르 ---
        "genres": list_to_str(genres_data),
        "genre_ids": genre_ids,

        # --- 기타 메타데이터 ---
        "homepage": data.get("homepage"),
        "in_production": data.get("in_production"),
        "languages": ", ".join(data.get("languages", [])) or "",
        "last_air_date": data.get("last_air_date"),

        # --- 마지막 에피소드 상세 정보 ---
        "last_episode_to_air_id": last_ep.get("id"),
        "last_episode_to_air_name": last_ep.get("name"),
        "last_episode_to_air_overview": last_ep.get("overview"),
        "last_episode_to_air_vote_average": last_ep.get("vote_average"),
        "last_episode_to_air_vote_count": last_ep.get("vote_count"),
        "last_episode_to_air_air_date": last_ep.get("air_date"),
        "last_episode_to_air_episode_number": last_ep.get("episode_number"),
        "last_episode_to_air_production_code": last_ep.get("production_code"),
        "last_episode_to_air_runtime": last_ep.get("runtime"),
        "last_episode_to_air_season_number": last_ep.get("season_number"),
        "last_episode_to_air_show_id": last_ep.get("show_id"),
        "last_episode_to_air_still_path": last_ep.get("still_path"),

        "next_episode_to_air": str(data.get("next_episode_to_air")) if data.get("next_episode_to_air") else "",

        # --- 네트워크 & 규모 ---
        "networks": network_names,
        "number_of_episodes": data.get("number_of_episodes"),
        "number_of_seasons": data.get("number_of_seasons"),
        "origin_country": ", ".join(data.get("origin_country", [])) or "",
        "original_language": data.get("original_language"),
        "original_name": data.get("original_name"),
        "overview": data.get("overview"),
        "popularity": data.get("popularity"),
        "production_companies": list_to_str(data.get("production_companies", [])),
        "production_countries": list_to_str(data.get("production_countries", [])),
        "seasons": seasons_summary,
        "spoken_languages": list_to_str(data.get("spoken_languages", [])),
        "status": data.get("status"),        # Returning Series, Ended, Canceled 등
        "tagline": data.get("tagline"),
        "type_detail": data.get("type"),     # Scripted, Reality 등
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),

        # --- 리뷰 ---
        "review": review_text,

        # --- 키워드 ---
        "keyword": ", ".join(keywords_list),

        # --- 출연진/제작진 ---
        "top_cast": ", ".join(top_cast),
        "directors": ", ".join(sorted(directors)),
        "writers": ", ".join(sorted(writers)),

        # --- OTT 제공사 (JSON 문자열로 저장) ---
        "providers_flatrate": json.dumps(providers_flatrate, ensure_ascii=False) if providers_flatrate else "{}",
        "providers_rent": json.dumps(providers_rent, ensure_ascii=False) if providers_rent else "{}",
        "providers_buy": json.dumps(providers_buy, ensure_ascii=False) if providers_buy else "{}",

        "poster_path": data.get("poster_path"),

        # --- 내부 전용 필드 (시즌 수집 시 네트워크 정보를 전달하기 위해 사용) ---
        "_network_names": network_names,
        "_network_ids": network_ids,
    }

    return record


# ============================================================
# TV Season 수집
# ============================================================
def fetch_tv_season_details(tv_id, season_number, series_name="", network_names="", network_ids=""):
    """
    특정 TV 시리즈의 특정 시즌 상세 정보를 수집합니다.

    에피소드별 런타임을 수집하여 평균 에피소드 런타임을 계산합니다.

    Args:
        tv_id (int): TMDB TV 시리즈 ID
        season_number (int): 시즌 번호 (1부터 시작)
        series_name (str): 시리즈 이름 (참조용)
        network_names (str): 방송 네트워크 이름 (시리즈에서 전달받음)
        network_ids (str): 방송 네트워크 ID (시리즈에서 전달받음)

    Returns:
        dict: 시즌 상세 정보 (실패 시 None)
    """
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_number}"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
    }

    try:
        response = session.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching season {season_number} for TV {tv_id}: {e}")
        return None

    # 에피소드 목록에서 런타임을 추출하여 평균 계산
    episodes = data.get("episodes", [])
    total_episodes = len(episodes)

    runtimes = [ep.get("runtime") for ep in episodes if ep.get("runtime")]
    avg_runtime = sum(runtimes) / len(runtimes) if runtimes else None

    record = {
        "_id": data.get("_id", f"{tv_id}_{season_number}"),
        "season_id": data.get("id"),
        "series_id": tv_id,                # 어떤 시리즈의 시즌인지 연결
        "series_name": series_name,
        "season_number": season_number,
        "name": data.get("name"),           # 시즌 이름 (예: "Season 1")
        "air_date": data.get("air_date"),   # 시즌 첫 방영일
        "overview": data.get("overview"),
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),
        "network_names": network_names,
        "network_ids": network_ids,
        "total_episodes": total_episodes,
        "avg_episode_runtime": round(avg_runtime) if avg_runtime else None,
    }

    return record


# ============================================================
# 시즌 일괄 수집 (단일 시리즈의 모든 시즌)
# ============================================================
def fetch_all_seasons_for_series(series_data):
    """
    한 시리즈의 모든 시즌 정보를 순서대로 수집합니다.

    시리즈 데이터에서 총 시즌 수를 확인한 뒤,
    시즌 1부터 마지막 시즌까지 하나씩 API를 호출합니다.

    Args:
        series_data (dict): fetch_tv_series_details()의 반환값

    Returns:
        list[dict]: 시즌 데이터 리스트 (시즌 데이터가 없으면 빈 리스트)
    """
    if not series_data:
        return []

    tv_id = series_data.get("id")
    number_of_seasons = series_data.get("number_of_seasons")

    if not number_of_seasons:
        return []

    # 시리즈 레벨의 정보를 시즌에 전달 (네트워크 정보 등)
    series_name = series_data.get("title", "")
    network_names = series_data.get("_network_names", "")
    network_ids = series_data.get("_network_ids", "")

    seasons = []
    for season_num in range(1, number_of_seasons + 1):
        season_data = fetch_tv_season_details(
            tv_id,
            season_num,
            series_name=series_name,
            network_names=network_names,
            network_ids=network_ids
        )
        if season_data:
            seasons.append(season_data)

    return seasons


# ============================================================
# 시리즈 + 시즌 동시 수집 (멀티스레드 호출용 통합 함수)
# ============================================================
def fetch_tv_series_and_seasons(tv_id):
    """
    시리즈 정보와 모든 시즌 정보를 한꺼번에 수집합니다.

    멀티스레드로 여러 시리즈를 병렬 수집할 때 이 함수 하나만 호출하면 됩니다.
    내부 전용 필드(_network_names, _network_ids)는 시즌 수집 후 제거합니다.

    Args:
        tv_id (int): TMDB TV 시리즈 ID

    Returns:
        tuple: (시리즈 데이터 dict, 시즌 데이터 list)
               시리즈 수집 실패 시 (None, [])
    """
    series_data = fetch_tv_series_details(tv_id)
    if not series_data:
        return None, []

    seasons_list = fetch_all_seasons_for_series(series_data)

    # 내부 전용 키 제거 (최종 결과에는 불필요)
    series_data.pop("_network_names", None)
    series_data.pop("_network_ids", None)

    return series_data, seasons_list
