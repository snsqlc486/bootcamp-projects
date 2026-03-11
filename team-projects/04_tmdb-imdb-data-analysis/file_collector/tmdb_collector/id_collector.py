from .config import *

def generate_date_periods(start_date, end_date, months=1):
    """
    시작일과 종료일 사이를 N개월 단위로 분할하는 함수.
    """
    periods = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        next_date = current + relativedelta(months=months)
        period_end = min(next_date - timedelta(days=1), end_dt)

        periods.append((
            current.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d")
        ))

        current = next_date

    return periods


def fetch_single_page(page, start_date, end_date, media_type="movie"):
    """
    단일 페이지를 가져오는 함수

    Args:
        page: 페이지 번호
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)
        media_type: "movie" 또는 "tv"

    Returns:
        (ids, total_pages, total_results)
    """
    # media_type에 따라 엔드포인트와 날짜 파라미터 설정
    if media_type == "movie":
        base_url = "https://api.themoviedb.org/3/discover/movie"
        date_param_gte = "primary_release_date.gte"
        date_param_lte = "primary_release_date.lte"
    elif media_type == "tv":
        base_url = "https://api.themoviedb.org/3/discover/tv"
        date_param_gte = "first_air_date.gte"
        date_param_lte = "first_air_date.lte"
    else:
        raise ValueError(f"Invalid media_type: {media_type}. Use 'movie' or 'tv'.")

    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        date_param_gte: start_date,
        date_param_lte: end_date,
        "page": page,
        "include_adult": True,
    }

    try:
        response = session.get(base_url, params=params, headers=HEADERS, timeout=10)
        data = response.json()
        ids = [item.get("id") for item in data.get("results", []) if item.get("id")]
        return ids, data.get("total_pages", 1), data.get("total_results", 0)

    except Exception as e:
        print(f"페이지 {page}에서 오류: {e}")
        return [], 1, 0


def fetch_ids_between_dates(start_date, end_date, media_type="movie"):
    """
    TMDB Discover API를 사용하여 특정 기간 내 모든 ID 수집 (멀티스레드)

    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)
        media_type: "movie" 또는 "tv"

    Returns:
        set: 수집된 ID 집합
    """
    # 1. 첫 페이지 확인
    results, total_pages, total_results = fetch_single_page(1, start_date, end_date, media_type)
    all_ids_set = set(results)

    # 2. 500페이지 제한 적용
    max_pages = min(total_pages, 500)

    media_name = "영화" if media_type == "movie" else "TV 시리즈"
    print(f"  [{media_name}] 총 {total_results:,}개 ({total_pages}페이지) → 수집: {max_pages}페이지")

    if total_pages > 500:
        print(f"  ⚠️  500페이지 제한으로 {(total_pages - 500) * 20:,}개 누락 가능")

    if max_pages == 1:
        return all_ids_set

    # 3. 나머지 페이지 병렬 수집
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(fetch_single_page, page, start_date, end_date, media_type): page
            for page in range(2, max_pages + 1)
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc=f"  페이지 수집", leave=False):
            page_results, _, _ = future.result()
            all_ids_set.update(page_results)

    return all_ids_set


def collect_ids(start_date, end_date, media_type="movie", months=1):
    """
    전체 기간의 영화 또는 TV 시리즈 ID 수집

    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)
        media_type: "movie" 또는 "tv"
        months: 기간 분할 단위 (개월)

    Returns:
        list: 수집된 모든 ID 리스트
    """
    media_name = "영화" if media_type == "movie" else "TV 시리즈"
    print(f"\n{'=' * 60}")
    print(f"📺 {media_name} ID 수집: {start_date} ~ {end_date}")
    print(f"{'=' * 60}\n")

    periods = generate_date_periods(start_date, end_date, months=months)
    all_ids = set()

    for i, (period_start, period_end) in enumerate(periods, 1):
        print(f"[기간 {i}/{len(periods)}] {period_start} ~ {period_end}")
        period_ids = fetch_ids_between_dates(period_start, period_end, media_type)
        all_ids.update(period_ids)
        print(f"  누적 ID: {len(all_ids):,}개\n")

    print(f"{'=' * 60}")
    print(f"최종 수집: {len(all_ids):,}개 {media_name} ID")
    print(f"{'=' * 60}\n")

    return sorted(list(all_ids))


# ============================================================
# 편의 함수
# ============================================================

def collect_movie_ids(start_date, end_date, months=1):
    """영화 ID 수집 (기존 함수명 호환)"""
    return collect_ids(start_date, end_date, media_type="movie", months=months)


def collect_tv_ids(start_date, end_date, months=1):
    """TV 시리즈 ID 수집"""
    return collect_ids(start_date, end_date, media_type="tv", months=months)