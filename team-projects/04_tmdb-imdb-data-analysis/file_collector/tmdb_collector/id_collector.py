"""
TMDB 영화/드라마 ID 수집 모듈

TMDB Discover API를 사용하여 특정 기간 내 영화 또는 TV 시리즈의 ID를 수집합니다.

TMDB API는 한 번에 최대 500페이지(페이지당 20개)까지만 조회 가능하므로,
긴 기간을 N개월 단위로 쪼개어 각 기간별로 ID를 수집하는 전략을 사용합니다.
또한 멀티스레드를 활용하여 여러 페이지를 동시에 요청해 수집 속도를 높입니다.

사용 예시:
    from file_collector.tmdb_collector.id_collector import collect_movie_ids, collect_tv_ids
    movie_ids = collect_movie_ids("2020-01-01", "2020-12-31", months=1)
    tv_ids = collect_tv_ids("2020-01-01", "2020-12-31", months=1)
"""

from .config import *


def generate_date_periods(start_date, end_date, months=1):
    """
    시작일과 종료일 사이를 N개월 단위로 분할하는 함수.

    TMDB API가 한 번에 최대 500페이지(=10,000건)까지만 반환하므로,
    긴 기간을 짧은 기간으로 나누어 데이터 누락을 방지합니다.

    예시:
        generate_date_periods("2020-01-01", "2020-06-30", months=2)
        → [("2020-01-01", "2020-02-29"), ("2020-03-01", "2020-04-30"), ("2020-05-01", "2020-06-30")]

    Args:
        start_date (str): 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str): 종료 날짜 (YYYY-MM-DD 형식)
        months (int): 분할 단위 (개월). 기본값 1개월

    Returns:
        list[tuple]: (시작일, 종료일) 쌍의 리스트
    """
    periods = []
    current = datetime.strptime(start_date, "%Y-%m-%d")  # 문자열 → datetime 변환
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        next_date = current + relativedelta(months=months)  # N개월 후 날짜 계산
        period_end = min(next_date - timedelta(days=1), end_dt)  # 기간의 마지막 날

        periods.append((
            current.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d")
        ))

        current = next_date

    return periods


def fetch_single_page(page, start_date, end_date, media_type="movie"):
    """
    TMDB Discover API에서 단일 페이지의 결과를 가져옵니다.

    TMDB API는 한 페이지에 최대 20개의 결과를 반환합니다.
    이 함수는 특정 페이지 번호의 영화/드라마 ID 목록을 가져옵니다.

    Args:
        page (int): 가져올 페이지 번호 (1부터 시작)
        start_date (str): 검색 시작 날짜 (YYYY-MM-DD)
        end_date (str): 검색 종료 날짜 (YYYY-MM-DD)
        media_type (str): "movie"(영화) 또는 "tv"(드라마)

    Returns:
        tuple: (ID 리스트, 전체 페이지 수, 전체 결과 수)
    """
    # 영화와 TV는 API 엔드포인트와 날짜 파라미터 이름이 다름
    if media_type == "movie":
        base_url = "https://api.themoviedb.org/3/discover/movie"
        date_param_gte = "primary_release_date.gte"   # 개봉일 기준
        date_param_lte = "primary_release_date.lte"
    elif media_type == "tv":
        base_url = "https://api.themoviedb.org/3/discover/tv"
        date_param_gte = "first_air_date.gte"         # 첫 방영일 기준
        date_param_lte = "first_air_date.lte"
    else:
        raise ValueError(f"Invalid media_type: {media_type}. Use 'movie' or 'tv'.")

    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",    # 인기도 높은 순 정렬
        date_param_gte: start_date,      # 이 날짜 이후
        date_param_lte: end_date,        # 이 날짜 이전
        "page": page,
        "include_adult": True,
    }

    try:
        response = session.get(base_url, params=params, headers=HEADERS, timeout=10)
        data = response.json()
        # 응답에서 각 항목의 ID만 추출
        ids = [item.get("id") for item in data.get("results", []) if item.get("id")]
        return ids, data.get("total_pages", 1), data.get("total_results", 0)

    except Exception as e:
        print(f"페이지 {page}에서 오류: {e}")
        return [], 1, 0


def fetch_ids_between_dates(start_date, end_date, media_type="movie"):
    """
    특정 기간 내 모든 영화/드라마 ID를 멀티스레드로 수집합니다.

    처리 흐름:
    1. 첫 페이지를 요청하여 전체 페이지 수 확인
    2. TMDB의 500페이지 제한 적용
    3. 나머지 페이지를 20개 스레드로 병렬 수집

    Args:
        start_date (str): 시작 날짜 (YYYY-MM-DD)
        end_date (str): 종료 날짜 (YYYY-MM-DD)
        media_type (str): "movie" 또는 "tv"

    Returns:
        set: 수집된 ID의 집합 (중복 자동 제거)
    """
    # 1단계: 첫 페이지로 전체 규모 파악
    results, total_pages, total_results = fetch_single_page(1, start_date, end_date, media_type)
    all_ids_set = set(results)  # set을 사용하면 중복 ID가 자동으로 제거됨

    # 2단계: TMDB API는 최대 500페이지까지만 조회 가능
    max_pages = min(total_pages, 500)

    media_name = "영화" if media_type == "movie" else "TV 시리즈"
    print(f"  [{media_name}] 총 {total_results:,}개 ({total_pages}페이지) → 수집: {max_pages}페이지")

    if total_pages > 500:
        print(f"  ⚠️  500페이지 제한으로 {(total_pages - 500) * 20:,}개 누락 가능")

    if max_pages == 1:
        return all_ids_set

    # 3단계: 나머지 페이지를 20개 스레드로 동시에 수집 (속도 향상)
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 2~max_pages까지 각 페이지를 병렬로 요청
        futures = {
            executor.submit(fetch_single_page, page, start_date, end_date, media_type): page
            for page in range(2, max_pages + 1)
        }

        # 완료된 순서대로 결과를 수집 (tqdm으로 진행률 표시)
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"  페이지 수집", leave=False):
            page_results, _, _ = future.result()
            all_ids_set.update(page_results)

    return all_ids_set


def collect_ids(start_date, end_date, media_type="movie", months=1):
    """
    전체 기간의 영화 또는 TV 시리즈 ID를 수집하는 메인 함수입니다.

    긴 기간을 months 단위로 분할한 뒤, 각 기간별로 ID를 수집하고
    모든 결과를 합쳐서 반환합니다.

    Args:
        start_date (str): 시작 날짜 (YYYY-MM-DD)
        end_date (str): 종료 날짜 (YYYY-MM-DD)
        media_type (str): "movie"(영화) 또는 "tv"(드라마)
        months (int): 기간 분할 단위 (개월). 기본값 1개월

    Returns:
        list[int]: 수집된 모든 ID의 정렬된 리스트
    """
    media_name = "영화" if media_type == "movie" else "TV 시리즈"
    print(f"\n{'=' * 60}")
    print(f"📺 {media_name} ID 수집: {start_date} ~ {end_date}")
    print(f"{'=' * 60}\n")

    # 기간을 N개월 단위로 분할
    periods = generate_date_periods(start_date, end_date, months=months)
    all_ids = set()

    # 각 기간별로 ID 수집
    for i, (period_start, period_end) in enumerate(periods, 1):
        print(f"[기간 {i}/{len(periods)}] {period_start} ~ {period_end}")
        period_ids = fetch_ids_between_dates(period_start, period_end, media_type)
        all_ids.update(period_ids)  # set에 추가 (중복 자동 제거)
        print(f"  누적 ID: {len(all_ids):,}개\n")

    print(f"{'=' * 60}")
    print(f"최종 수집: {len(all_ids):,}개 {media_name} ID")
    print(f"{'=' * 60}\n")

    return sorted(list(all_ids))  # 정렬된 리스트로 반환


# ============================================================
# 편의 함수: collect_ids를 더 쉽게 호출할 수 있는 래퍼(wrapper) 함수
# ============================================================

def collect_movie_ids(start_date, end_date, months=1):
    """영화 ID 수집 편의 함수 (media_type="movie"를 자동으로 지정)"""
    return collect_ids(start_date, end_date, media_type="movie", months=months)


def collect_tv_ids(start_date, end_date, months=1):
    """TV 시리즈 ID 수집 편의 함수 (media_type="tv"를 자동으로 지정)"""
    return collect_ids(start_date, end_date, media_type="tv", months=months)
