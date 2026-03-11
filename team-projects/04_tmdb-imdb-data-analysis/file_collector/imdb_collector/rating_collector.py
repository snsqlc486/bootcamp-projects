"""
IMDb 평점 수집 모듈

IMDb 영화 페이지의 HTML을 직접 가져와서 평점(rating)과 투표 수(rating count)를 수집합니다.

GraphQL 대신 HTML 파싱을 사용하는 이유:
  평점 정보는 페이지 HTML의 JSON-LD 블록에 구조화된 형태로 포함되어 있어
  HTML에서 바로 추출하는 방식이 더 간단하고 안정적입니다.

체크포인트 기능:
  수집이 중간에 중단되어도 재시작 시 이미 처리된 항목을 건너뛰고
  남은 항목부터 이어서 수집할 수 있습니다.
"""

from .config import *

# 수집 진행 상황을 추적하는 통계 딕셔너리
stats = {
    "series_total": 0,    # 전체 수집 대상 수
    "series_success": 0,  # 성공 수
    "series_failed": 0,   # 실패 수
    "requests": 0,        # 총 HTTP 요청 수
    "start_time": None    # 시작 시간
}


# ==========================================================
# HTTP 호출 함수
# ==========================================================
async def get_html(session, url, retry=0):
    """
    IMDb 페이지의 HTML을 비동기로 가져옵니다.

    실패 시 지수 백오프 방식으로 재시도합니다:
    - 1회 실패: 1초 대기
    - 2회 실패: 2초 대기
    - 3회 실패: None 반환

    Args:
        session: aiohttp 세션 객체
        url (str): 요청할 URL
        retry (int): 현재 재시도 횟수 (내부 사용)

    Returns:
        str|None: 페이지 HTML 문자열 (실패 시 None)
    """
    if retry >= MAX_RETRIES:
        return None

    # 속도 제한 준수 (초당 MAX_CALLS_PER_SECOND 이하)
    await rate_limiter.acquire()
    stats["requests"] += 1

    # 브라우저로 위장하는 헤더
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429 and retry < MAX_RETRIES - 1:
                # 요청 너무 많음 → 대기 후 재시도
                wait_time = 5 * (retry + 1)
                print(f"⚠️  Rate limited, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await get_html(session, url, retry + 1)

            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** retry)  # 지수 백오프
                    return await get_html(session, url, retry + 1)
                return None

            return await resp.text()

    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_html(session, url, retry + 1)
        return None

    except Exception:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_html(session, url, retry + 1)
        return None


# ==========================================================
# IMDb 평점 추출
# ==========================================================
def parse_rating_from_html(imdb_id, html_text):
    """
    IMDb 페이지 HTML에서 평점과 투표 수를 추출합니다.

    IMDb 페이지에는 검색엔진 최적화를 위해 JSON-LD 형식의 구조화 데이터가
    <script type="application/ld+json"> 태그 안에 포함되어 있습니다.
    여기에 aggregateRating 정보(평점, 투표 수)가 들어 있습니다.

    Args:
        imdb_id (str): IMDb ID (에러 메시지 출력용)
        html_text (str): IMDb 페이지 HTML

    Returns:
        dict: {"imdb_id": ..., "imdb_rating": ..., "imdb_rating_count": ...}
    """
    imdb_rating = None
    imdb_rating_count = None

    # JSON-LD 스크립트 블록을 정규식으로 찾기
    ld_match = re.search(
        r'<script type="application/ld\+json">(.*?)</script>',
        html_text,
        re.S  # re.S: 점(.)이 줄바꿈도 포함하도록
    )
    if ld_match:
        try:
            data = json.loads(ld_match.group(1))
            # aggregateRating 객체에서 평점 추출
            agg = data.get("aggregateRating", {})
            imdb_rating = agg.get("ratingValue")      # 예: 9.3
            imdb_rating_count = agg.get("ratingCount")  # 예: 2500000
        except Exception as e:
            print(f"⚠️  JSON-LD parse error ({imdb_id}): {e}")

    return {
        "imdb_id": imdb_id,
        "imdb_rating": imdb_rating,
        "imdb_rating_count": imdb_rating_count,
    }


async def fetch_imdb_rating(session, imdb_id):
    """
    특정 IMDb ID의 평점 정보를 수집합니다.

    Args:
        session: aiohttp 세션 객체
        imdb_id (str): IMDb ID (예: "tt0111161")

    Returns:
        dict: {"imdb_id": ..., "imdb_rating": ..., "imdb_rating_count": ...}
    """
    url = f"https://www.imdb.com/title/{imdb_id}/"
    html_text = await get_html(session, url)
    if not html_text:
        print(f"⚠️  {imdb_id}: HTML 가져오기 실패")
        return {
            "imdb_id": imdb_id,
            "imdb_rating": None,
            "imdb_rating_count": None,
        }
    return parse_rating_from_html(imdb_id, html_text)


# ==========================================================
# 체크포인트 관리 (중단 후 재시작 지원)
# ==========================================================
def save_checkpoint(processed_ids):
    """
    현재까지 처리된 ID 목록을 파일에 저장합니다.
    수집이 중단되었을 때 재시작 위치를 기억하기 위해 사용합니다.
    """
    checkpoint = {
        'processed_ids': list(processed_ids),
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)


def load_checkpoint(output_csv_path):
    """
    이전에 저장된 체크포인트를 불러옵니다.
    체크포인트 파일과 기존 CSV 파일 양쪽을 확인하여
    이미 처리된 ID의 집합을 반환합니다.

    Args:
        output_csv_path (str): 수집 결과가 저장된 CSV 경로

    Returns:
        set: 이미 처리된 imdb_id 집합
    """
    processed_ids = set()

    # 1) 체크포인트 JSON 파일에서 로드
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                processed_ids.update(checkpoint.get('processed_ids', []))
                print(f"📌 체크포인트에서 {len(checkpoint.get('processed_ids', [])):,}개 ID 로드")
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️  체크포인트 파일 손상됨, 삭제하고 진행: {e}")
            try:
                Path(CHECKPOINT_FILE).unlink()
            except:
                pass

    # 2) 기존 결과 CSV에서도 로드 (체크포인트보다 더 신뢰할 수 있음)
    if Path(output_csv_path).exists():
        try:
            df_existing = pd.read_csv(output_csv_path)
            if 'imdb_id' in df_existing.columns:
                existing_ids = df_existing['imdb_id'].unique()
                processed_ids.update(existing_ids)
                print(f"📌 기존 CSV에서 {len(existing_ids):,}개 시리즈 발견")
        except Exception as e:
            print(f"⚠️  기존 CSV 로드 실패: {e}")

    return processed_ids


# ==========================================================
# 날짜 필터링
# ==========================================================
def filter_by_date_range(df, start_date='2005-01-01', end_date='2025-12-31'):
    """
    first_air_date 컬럼 기준으로 특정 날짜 범위의 데이터만 필터링합니다.

    Args:
        df (pd.DataFrame): 원본 데이터
        start_date (str): 시작 날짜 (YYYY-MM-DD)
        end_date (str): 종료 날짜 (YYYY-MM-DD)

    Returns:
        pd.DataFrame: 날짜 범위 내 데이터
    """
    if 'first_air_date' not in df.columns:
        print("⚠️  'first_air_date' 컬럼이 없습니다. 날짜 필터링을 건너뜁니다.")
        return df

    df['first_air_date'] = pd.to_datetime(df['first_air_date'], errors='coerce')
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    df_filtered = df[(df['first_air_date'] >= start) & (df['first_air_date'] <= end)]

    print(f"📅 날짜 필터링: {start_date} ~ {end_date}")
    print(f"   원본: {len(df):,}개 → 필터링 후: {len(df_filtered):,}개")

    return df_filtered


# ==========================================================
# 메인 실행 함수
# ==========================================================
async def collect_imdb_ratings(input_csv_path, output_csv_path, vote_threshold=30):
    """
    CSV 파일의 영화/드라마 목록에 대해 IMDb 평점을 일괄 수집합니다.

    처리 흐름:
    1. CSV 로드 → 날짜 및 투표 수 필터링
    2. 체크포인트 확인 → 이미 처리된 항목 스킵
    3. 10개씩 배치로 비동기 수집
    4. 50개마다 중간 저장
    5. 최종 저장 및 중복 제거

    Args:
        input_csv_path (str): 수집 대상 CSV 경로 (imdb_id 컬럼 필요)
        output_csv_path (str): 결과 저장 CSV 경로
        vote_threshold (int): 최소 투표 수 (이보다 적으면 수집 제외)
    """
    print("=" * 90)
    print("🚀 IMDB RATING COLLECTOR (2005-2015)")
    print("=" * 90)

    stats["start_time"] = datetime.now()
    t0 = datetime.now()

    # 1. 데이터 로드 및 필터링
    print("\n📂 데이터 로드 중...")
    df = pd.read_csv(input_csv_path)

    df = filter_by_date_range(df, '2005-01-01', '2025-12-31')

    # 투표 수가 너무 적거나 imdb_id 없는 항목 제외 (신뢰도 낮음)
    df_filtered = df[(df['vote_count'] >= vote_threshold) & (df['imdb_id'].notna())]
    df_filtered = df_filtered.drop_duplicates(subset=['imdb_id'])

    print(f"✅ 최종 필터링 (vote_count>={vote_threshold} & imdb_id 존재): {len(df_filtered):,}개")

    if len(df_filtered) == 0:
        print("⚠️  조건을 만족하는 데이터가 없습니다.")
        return

    # 2. 체크포인트 로드 (이미 처리된 항목 확인)
    processed_ids = load_checkpoint(output_csv_path)
    series_list = df_filtered[['imdb_id']].to_dict('records')

    if processed_ids:
        print(f"📌 이미 처리된 시리즈: {len(processed_ids):,}개")
        series_list = [s for s in series_list if s['imdb_id'] not in processed_ids]
        print(f"📌 남은 작업: {len(series_list):,}개")

    if len(series_list) == 0:
        print("✅ 모든 데이터가 이미 처리되었습니다.")
        return

    stats["series_total"] = len(series_list)

    # 3. 수집 시작
    print(f"\n🚀 크롤링 시작")
    print(f"⚙️  Rate Limit: {MAX_CALLS_PER_SECOND}회/초")

    estimated_time = len(series_list) / MAX_CALLS_PER_SECOND / 60
    print(f"⏱️  예상 시간: {estimated_time:.0f}분")

    connector = aiohttp.TCPConnector(
        limit=20,                    # 동시 연결 최대 20개
        force_close=False,
        enable_cleanup_closed=True   # 닫힌 연결 정리
    )

    all_results = []
    batch_size = 10

    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        for i in range(0, len(series_list), batch_size):
            batch = series_list[i:i + batch_size]

            # 10개씩 동시에 평점 수집
            tasks = [fetch_imdb_rating(session, s['imdb_id']) for s in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    stats["series_failed"] += 1
                    continue

                if isinstance(result, dict):
                    all_results.append(result)
                    processed_ids.add(result['imdb_id'])
                    stats["series_success"] += 1
                else:
                    stats["series_failed"] += 1

            # 50개마다 체크포인트 저장 및 CSV 중간 저장
            if (i + batch_size) % 50 == 0:
                save_checkpoint(processed_ids)

                if all_results:
                    df_batch = pd.DataFrame(all_results)
                    df_batch = df_batch.drop_duplicates(subset=['imdb_id'])
                    file_exists = Path(output_csv_path).exists()
                    df_batch.to_csv(
                        output_csv_path,
                        mode='a',
                        header=not file_exists,
                        index=False,
                        encoding='utf-8-sig'
                    )
                    all_results.clear()
                    print(f"💾 중간 저장 완료 ({len(df_batch):,}개)")

            # 진행 상황 출력
            elapsed = (datetime.now() - t0).total_seconds() / 60
            progress = stats["series_success"] + stats["series_failed"]
            rate = progress / elapsed if elapsed > 0 else 0
            eta = (stats["series_total"] - progress) / rate if rate > 0 else 0

            print(
                f"📊 진행: {progress}/{stats['series_total']} "
                f"({progress / stats['series_total'] * 100:.1f}%) | "
                f"성공: {stats['series_success']} | 실패: {stats['series_failed']} | "
                f"요청: {stats['requests']:,}회 | "
                f"속도: {rate:.1f}개/분 | ETA: {eta:.0f}분"
            )

    # 4. 최종 저장
    print("\n💾 최종 저장 중...")

    if all_results:
        df_batch = pd.DataFrame(all_results)
        df_batch = df_batch.drop_duplicates(subset=['imdb_id'])
        file_exists = Path(output_csv_path).exists()
        df_batch.to_csv(
            output_csv_path,
            mode='a',
            header=not file_exists,
            index=False,
            encoding='utf-8-sig'
        )

    # 전체 파일에서 중복 제거 (여러 번 실행 시 중복 방지)
    if Path(output_csv_path).exists():
        df_results = pd.read_csv(output_csv_path)
        df_results = df_results.drop_duplicates(subset=['imdb_id'])
        df_results.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    else:
        df_results = pd.DataFrame()

    # 체크포인트 파일 삭제 (수집 완료)
    if Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()

    # 5. 최종 통계 출력
    elapsed = (datetime.now() - t0).total_seconds() / 60

    print("\n" + "=" * 90)
    print("🎉 크롤링 완료!")
    print("=" * 90)
    print(f"📌 시리즈: {stats['series_success']:,}/{stats['series_total']:,}개 성공")

    if not df_results.empty:
        print(f"📌 총 수집: {len(df_results):,}개 (중복 제거 후)")

        has_rating = df_results['imdb_rating'].notna().sum()
        print(f"📌 Rating 보유: {has_rating:,}개 ({has_rating / len(df_results) * 100:.1f}%)")

        if has_rating > 0:
            print(f"📌 평균 Rating: {df_results['imdb_rating'].mean():.2f}")
            print(f"📌 평균 Rating Count: {df_results['imdb_rating_count'].mean():.0f}")
    else:
        print("📌 수집된 데이터 없음")

    print(f"📌 총 요청: {stats['requests']:,}회")
    print(f"⏱️  총 시간: {elapsed:.1f}분 ({elapsed / 60:.2f}시간)")

    if stats['series_success'] > 0 and elapsed > 0:
        print(f"📊 속도: {stats['series_success'] / elapsed:.1f}개/분")

    print("=" * 90)

    if not df_results.empty:
        print("\n📊 결과 샘플:")
        print(df_results.head(10).to_string())
        print(f"\n✅ 결과 파일: {output_csv_path}")
