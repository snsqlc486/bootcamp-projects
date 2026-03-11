from .config import *
import json


def list_to_str(lst, key="name"):
    """리스트를 문자열로 변환"""
    return ", ".join([str(item.get(key, "")) for item in lst]) if lst else ""


# ============================================================
# TV Series 수집
# ============================================================
def fetch_tv_series_details(tv_id):
    """TV 시리즈 기본 정보 수집"""
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
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

    # External IDs (IMDB ID)
    imdb_id_data = data.get("external_ids", {})
    imdb_id = imdb_id_data.get("imdb_id", "")

    # 장르
    genres_data = data.get("genres", [])
    genre_ids = list_to_str(genres_data, key="id")

    # 키워드 (TV는 keywords.results 사용)
    keywords_block = data.get("keywords", {})
    if isinstance(keywords_block, dict):
        keywords_list = [k.get("name") for k in keywords_block.get("results", [])]
    else:
        keywords_list = []

    # 크레딧 (aggregate_credits 사용 - TV에 최적화)
    aggregate_credits = data.get("aggregate_credits", {})
    cast_list = aggregate_credits.get("cast", [])
    crew_list = aggregate_credits.get("crew", [])

    # 상위 배우 5명
    top_cast = [c.get("name") for c in cast_list[:5] if c.get("name")]

    # 감독 & 작가 (jobs 배열 확인)
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

    # Providers (flatrate, rent, buy 분리)
    providers_data = data.get("watch/providers", {})
    providers_results = providers_data.get("results", {}) if isinstance(providers_data, dict) else {}

    providers_flatrate = {}
    providers_rent = {}
    providers_buy = {}

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

    # 리뷰
    review_items = data.get("reviews", {}).get("results", [])
    review_text = " || ".join([
        f"{r.get('author', '')}({r.get('author_details', {}).get('rating', '')}): "
        f"{(r.get('content') or '').replace(chr(10), ' ')[:200]}"
        for r in review_items[:5]
    ])

    # 네트워크
    networks = data.get("networks", [])
    network_names = list_to_str(networks)
    network_ids = list_to_str(networks, key="id")

    # 시즌 정보 요약
    seasons_data = data.get("seasons", [])
    seasons_summary = "; ".join([
        f"S{s.get('season_number')}: {s.get('name')} ({s.get('episode_count')} eps)"
        for s in seasons_data if s.get("season_number") is not None
    ])

    # 마지막 에피소드
    last_ep = data.get("last_episode_to_air", {})

    record = {
        # 기본 정보 (순서 중요!)
        "id": data.get("id"),
        "type": "tv_series",
        "imdb_id": imdb_id,
        "title": data.get("name"),

        # 제목
        "adult": data.get("adult"),

        # 이미지
        "backdrop_path": data.get("backdrop_path"),

        # 제작
        "created_by": list_to_str(data.get("created_by", [])),

        # 런타임
        "episode_run_time": ", ".join(map(str, data.get("episode_run_time", []))) or "",

        # 방영 정보
        "first_air_date": data.get("first_air_date"),

        # 장르
        "genres": list_to_str(genres_data),
        "genre_ids": genre_ids,

        # 기타
        "homepage": data.get("homepage"),
        "in_production": data.get("in_production"),
        "languages": ", ".join(data.get("languages", [])) or "",
        "last_air_date": data.get("last_air_date"),

        # 마지막 에피소드 (모든 필드)
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

        # 다음 에피소드
        "next_episode_to_air": str(data.get("next_episode_to_air")) if data.get("next_episode_to_air") else "",

        # 네트워크
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
        "status": data.get("status"),
        "tagline": data.get("tagline"),
        "type_detail": data.get("type"),
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),

        # 리뷰
        "review": review_text,

        # 키워드
        "keyword": ", ".join(keywords_list),

        # 크레딧
        "top_cast": ", ".join(top_cast),
        "directors": ", ".join(sorted(directors)),
        "writers": ", ".join(sorted(writers)),

        # Providers (JSON 문자열)
        "providers_flatrate": json.dumps(providers_flatrate, ensure_ascii=False) if providers_flatrate else "{}",
        "providers_rent": json.dumps(providers_rent, ensure_ascii=False) if providers_rent else "{}",
        "providers_buy": json.dumps(providers_buy, ensure_ascii=False) if providers_buy else "{}",

        # 포스터
        "poster_path": data.get("poster_path"),

        # 내부 사용 (시즌 수집용)
        "_network_names": network_names,
        "_network_ids": network_ids,
    }

    return record


# ============================================================
# TV Season 수집
# ============================================================
def fetch_tv_season_details(tv_id, season_number, series_name="", network_names="", network_ids=""):
    """특정 시즌의 상세 정보 수집"""
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

    # 에피소드 정보
    episodes = data.get("episodes", [])
    total_episodes = len(episodes)

    # 평균 런타임 계산
    runtimes = [ep.get("runtime") for ep in episodes if ep.get("runtime")]
    avg_runtime = sum(runtimes) / len(runtimes) if runtimes else None

    record = {
        "_id": data.get("_id", f"{tv_id}_{season_number}"),
        "season_id": data.get("id"),
        "series_id": tv_id,
        "series_name": series_name,
        "season_number": season_number,
        "name": data.get("name"),
        "air_date": data.get("air_date"),
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
# 시즌 일괄 수집 (단일 시리즈)
# ============================================================
def fetch_all_seasons_for_series(series_data):
    """
    한 시리즈의 모든 시즌 수집

    Args:
        series_data: fetch_tv_series_details()의 반환값

    Returns:
        list: 시즌 데이터 리스트
    """
    if not series_data:
        return []

    tv_id = series_data.get("id")
    number_of_seasons = series_data.get("number_of_seasons")

    if not number_of_seasons:
        return []

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
# 시리즈 + 시즌 동시 수집 (멀티스레드용)
# ============================================================
def fetch_tv_series_and_seasons(tv_id):
    """
    시리즈 정보 + 모든 시즌 정보를 함께 수집

    Returns:
        (series_data, seasons_list): 튜플
    """
    series_data = fetch_tv_series_details(tv_id)
    if not series_data:
        return None, []

    seasons_list = fetch_all_seasons_for_series(series_data)

    # 내부 사용 키 제거
    series_data.pop("_network_names", None)
    series_data.pop("_network_ids", None)

    return series_data, seasons_list