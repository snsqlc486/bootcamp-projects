"""
IMDb 리뷰 수집 모듈

IMDb의 GraphQL API를 사용하여 영화/드라마의 사용자 리뷰를 수집합니다.

GraphQL이란?
  REST API와 달리 필요한 필드만 정확히 지정해서 요청하는 API 방식입니다.
  IMDb는 내부적으로 GraphQL을 사용하며, 이를 통해 리뷰 데이터를 가져옵니다.

비동기(async) 방식이란?
  여러 영화의 리뷰를 동시에 요청하여 수집 속도를 높입니다.
  예: 10개 영화를 순차적으로 처리하면 10분 → 동시에 처리하면 1~2분

수집 흐름:
  1. 영화 목록 CSV 로드
  2. 각 영화에 대해 GraphQL URL 생성
  3. 비동기로 여러 영화 동시 수집
  4. 10편씩 배치로 저장 (메모리 절약)
  5. 이미 수집된 영화는 자동 스킵 (재시작 지원)
"""

from .config import *


# ==========================
# GraphQL URL 생성
# ==========================

def build_graphql_url(
    imdb_id: str,
    after_cursor: str | None = None,
    first: int = REVIEWS_PER_REQUEST,
    sort_by: str = "HELPFULNESS_SCORE",
) -> str:
    """
    IMDb GraphQL API 요청 URL을 생성합니다.

    IMDb 리뷰는 페이지네이션(cursor 방식)을 사용합니다.
    - 처음 요청: after_cursor 없음 → 첫 25개 리뷰
    - 다음 페이지: 이전 응답의 endCursor 전달 → 다음 25개

    Args:
        imdb_id (str): IMDb 영화/드라마 ID (예: "tt0111161")
        after_cursor (str|None): 페이지네이션 커서 (다음 페이지 요청 시 사용)
        first (int): 한 번에 가져올 리뷰 수 (기본값: 25)
        sort_by (str): 정렬 기준 (기본값: 유용성 점수 내림차순)

    Returns:
        str: 완성된 GraphQL 요청 URL
    """
    variables = {
        "const": imdb_id,
        "first": first,
        "locale": "en-US",   # 영어 리뷰만 요청
        "sort": {"by": sort_by, "order": "DESC"},
        "filter": {},
    }
    if after_cursor:
        variables["after"] = after_cursor  # 다음 페이지 커서 추가

    # Persisted Query: 미리 등록된 쿼리의 해시를 전달 (대역폭 절약)
    extensions = {"persistedQuery": {"sha256Hash": PERSISTED_QUERY_HASH, "version": 1}}

    # URL에 안전하게 포함되도록 JSON을 URL 인코딩
    variables_json = json.dumps(variables, separators=(",", ":"))
    extensions_json = json.dumps(extensions, separators=(",", ":"))

    url = (
        f"{GRAPHQL_URL}?"
        f"operationName={OPERATION_NAME}"
        f"&variables={quote(variables_json)}"
        f"&extensions={quote(extensions_json)}"
    )
    return url


async def fetch_json(session: aiohttp.ClientSession, url: str, retry: int = 0):
    """
    GraphQL API를 호출하고 JSON 응답을 반환합니다.
    실패 시 최대 MAX_RETRIES번 자동 재시도합니다.

    HTTP 상태 코드별 처리:
    - 200: 성공 → JSON 반환
    - 429: Too Many Requests → 대기 후 재시도
    - 그 외: 지수 백오프(2^retry초) 후 재시도

    Args:
        session: aiohttp 세션 객체
        url (str): 요청할 URL
        retry (int): 현재 재시도 횟수 (내부적으로 증가)

    Returns:
        dict|None: JSON 응답 또는 실패 시 None
    """
    if retry >= MAX_RETRIES:
        return None

    # 속도 제한 준수 (초당 MAX_CALLS_PER_SECOND 이하)
    await rate_limiter.acquire()

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429:
                # 서버가 요청이 너무 많다고 응답 → 대기 후 재시도
                wait_time = 5 * (retry + 1)
                print(f"⚠️  429 Too Many Requests → {wait_time}s 대기 후 재시도")
                await asyncio.sleep(wait_time)
                return await fetch_json(session, url, retry + 1)

            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    # 지수 백오프: 1초, 2초, 4초... 간격으로 재시도
                    await asyncio.sleep(2**retry)
                    return await fetch_json(session, url, retry + 1)
                print(f"❌ HTTP {resp.status}")
                return None

            return await resp.json()

    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2**retry)
            return await fetch_json(session, url, retry + 1)
        print("❌ Timeout")
        return None

    except Exception as e:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2**retry)
            return await fetch_json(session, url, retry + 1)
        print(f"❌ 요청 에러: {e}")
        return None


# ==========================
# 리뷰 텍스트 정제
# ==========================

def clean_html(text: str | None) -> str | None:
    """
    HTML 태그와 엔티티를 제거하여 순수 텍스트로 변환합니다.

    IMDb 리뷰 텍스트는 HTML 형식으로 저장되어 있습니다.
    예: "&amp;quot;Great film&amp;quot;<br/>Loved it" → '"Great film"\nLoved it'

    Args:
        text (str|None): HTML이 포함된 원본 텍스트

    Returns:
        str|None: 정제된 순수 텍스트
    """
    if not text:
        return None
    text = html.unescape(text)                     # &amp; → &, &lt; → < 등 변환
    text = text.replace("<br/>", "\n").replace("<br>", "\n")  # 줄바꿈 태그 처리
    text = re.sub("<[^<]+?>", "", text)            # 나머지 HTML 태그 제거
    return text.strip()


def parse_review_node(node: dict, imdb_id: str) -> dict | None:
    """
    GraphQL 응답의 리뷰 노드에서 필요한 필드만 추출합니다.

    GraphQL 응답은 중첩된 구조로 되어 있어서,
    분석에 필요한 컬럼명으로 맞춰 평탄화(flatten)합니다.

    Args:
        node (dict): GraphQL 응답의 단일 리뷰 노드
        imdb_id (str): 이 리뷰가 속한 영화의 IMDb ID

    Returns:
        dict|None: 정제된 리뷰 데이터 (파싱 실패 시 None)
    """
    try:
        review_id = node.get("id")

        # 작성자 정보
        author = node.get("author", {})
        username = author.get("username", {}).get("text")
        user_id = author.get("userId")

        # 작성자가 매긴 평점 (없을 수도 있음)
        author_rating = node.get("authorRating")

        # 유용성 투표 (helpful votes): 이 리뷰가 얼마나 도움이 됐는지
        helpful = node.get("helpfulness", {})
        up = helpful.get("upVotes", 0)
        down = helpful.get("downVotes", 0)

        review_date = node.get("submissionDate")

        # 리뷰 제목
        summary = node.get("summary", {})
        review_title = summary.get("originalText")

        # 리뷰 본문 (HTML → 순수 텍스트 변환)
        text_data = node.get("text", {}).get("originalText", {})
        review_text_html = text_data.get("plaidHtml")
        review_text = clean_html(review_text_html)

        return {
            "author_rating": author_rating,
            "review_title": review_title,
            "review_text": review_text,
            "review_id": review_id,
            "username": username,
            "user_id": user_id,
            "review_date": review_date,
            "helpful_up_votes": up,
            "helpful_down_votes": down,
            "imdb_id": imdb_id,
            # 텍스트 길이는 감성분석 가중치 계산 등에서 활용
            "review_text_length": len(review_text) if review_text else 0,
        }
    except Exception as e:
        print(f"⚠️ 리뷰 파싱 에러: {e}")
        return None


# ==========================
# 한 영화의 모든 리뷰 수집
# ==========================

async def fetch_reviews_for_title(
    session: aiohttp.ClientSession,
    imdb_id: str,
    title: str = "",
    max_reviews: int | None = None,
) -> list[dict]:
    """
    특정 영화/드라마의 리뷰를 전부(또는 최대 N개) 수집합니다.

    IMDb 리뷰는 페이지네이션으로 제공됩니다 (한 번에 25개씩).
    hasNextPage가 False가 될 때까지 반복하여 모든 리뷰를 수집합니다.

    Args:
        session: aiohttp 세션 객체
        imdb_id (str): IMDb 영화/드라마 ID
        title (str): 영화 제목 (로그 출력용)
        max_reviews (int|None): 최대 수집 리뷰 수. None이면 전체 수집

    Returns:
        list[dict]: 수집된 리뷰 딕셔너리 목록
    """
    all_reviews: list[dict] = []
    after_cursor: str | None = None  # 첫 요청은 커서 없이 시작
    page = 0

    while True:
        page += 1
        url = build_graphql_url(imdb_id, after_cursor)
        data = await fetch_json(session, url)
        if not data:
            break

        # GraphQL 응답 구조: data.title.reviews.edges[].node
        title_data = data.get("data", {}).get("title", {})
        reviews_data = title_data.get("reviews", {})
        edges = reviews_data.get("edges", [])
        total = reviews_data.get("total", 0)

        if not edges:
            break

        # 각 엣지(edge)에서 리뷰 노드 추출
        for edge in edges:
            node = edge.get("node", {})
            review = parse_review_node(node, imdb_id)
            if review:
                all_reviews.append(review)

        # 최대 개수 제한에 도달하면 중단
        if max_reviews is not None and len(all_reviews) >= max_reviews:
            all_reviews = all_reviews[:max_reviews]
            break

        # 다음 페이지 커서 확인
        page_info = reviews_data.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        after_cursor = page_info.get("endCursor")

        if not has_next or not after_cursor:
            break

        # 요청 간 짧은 대기 (서버 부하 감소)
        await asyncio.sleep(0.1)

    if all_reviews:
        print(f"✅ {title} ({imdb_id})  {len(all_reviews):,}/{total}개")
    else:
        print(f"➖ {title} ({imdb_id}) 리뷰 없음")

    return all_reviews


# ==========================
# 메인 크롤링 함수
# ==========================

async def collect_imdb_reviews(
    input_csv_path: str,
    output_csv_path: str,
    max_titles: int | None = None,
    max_reviews_per_title: int | None = None,
):
    """
    CSV 파일의 영화 목록에 대해 IMDb 리뷰를 일괄 수집합니다.

    - 이미 수집된 영화는 자동으로 스킵합니다 (중단 후 재시작 지원)
    - 10편씩 배치로 처리하고 즉시 저장 (메모리 절약)
    - 비동기로 10편을 동시에 수집하여 속도 향상

    Args:
        input_csv_path (str): 영화 목록 CSV 경로 (id, title, imdb_id 컬럼 필요)
        output_csv_path (str): 수집된 리뷰를 저장할 CSV 경로
        max_titles (int|None): 테스트용 - 앞에서 N편만 처리. None이면 전체
        max_reviews_per_title (int|None): 영화당 최대 리뷰 수. None이면 전체
    """
    t0 = datetime.now()
    print("=" * 70)
    print("🚀 IMDb 리뷰 크롤링 시작")
    print(f"📂 입력: {input_csv_path}")
    print(f"💾 출력: {output_csv_path}")
    print("=" * 70)

    df = pd.read_csv(input_csv_path)
    df = df[df["imdb_id"].notna()].copy()  # imdb_id 없는 행 제외

    # === 이미 수집된 영화 스킵 (재시작 지원) ===
    done_ids: set[str] = set()
    if Path(output_csv_path).exists():
        print("📌 기존 리뷰 파일 발견 → 이미 수집된 imdb_id 스킵")
        df_done = pd.read_csv(output_csv_path, usecols=["imdb_id"])
        done_ids = set(df_done["imdb_id"].unique())
        print(f"   이미 수집된 영화 수: {len(done_ids)}")

    # 아직 수집 안 된 영화만 대상으로
    targets = df[~df["imdb_id"].isin(done_ids)].copy()

    if max_titles is not None:
        targets = targets.head(max_titles)  # 테스트 시 앞 N편만

    print(f"🎯 이번에 수집할 영화 수: {len(targets)}")

    # 동시 연결 수 제한 (20개)
    connector = aiohttp.TCPConnector(limit=20, force_close=False)
    all_results: list[dict] = []
    batch_size = 10  # 10편씩 동시 처리

    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        for start in range(0, len(targets), batch_size):
            batch = targets.iloc[start : start + batch_size]

            # 10편의 수집 작업을 동시에 시작
            tasks = [
                fetch_reviews_for_title(
                    session,
                    row["imdb_id"],
                    row.get("title", ""),
                    max_reviews=max_reviews_per_title,
                )
                for _, row in batch.iterrows()
            ]

            # 모든 비동기 작업이 완료될 때까지 대기
            batch_results = await asyncio.gather(*tasks)

            for reviews in batch_results:
                all_results.extend(reviews)

            # 배치마다 CSV에 추가 저장 (중단 시 데이터 손실 방지)
            if all_results:
                df_batch = pd.DataFrame(all_results)
                file_exists = Path(output_csv_path).exists()
                df_batch.to_csv(
                    output_csv_path,
                    mode="a",               # 추가 모드 (기존 내용 유지)
                    header=not file_exists,  # 파일이 없을 때만 헤더 작성
                    index=False,
                    encoding="utf-8-sig",
                )
                print(f"💾 배치 저장: {len(df_batch)}개 리뷰")
                all_results.clear()

            done = min(start + batch_size, len(targets))
            elapsed_min = (datetime.now() - t0).total_seconds() / 60
            print(f"📊 진행 상황: {done}/{len(targets)} (경과 {elapsed_min:.1f}분)")

    print("\n✅ 크롤링 완료")
    total_min = (datetime.now() - t0).total_seconds() / 60
    print(f"⏱️ 총 소요 시간: {total_min:.1f}분")
