"""
벡터화 전처리용 데이터 로더 모듈

메인 데이터(영화/드라마 정보)와 흥행 점수 데이터를 불러와
벡터화에 필요한 형태로 가공합니다.

주요 처리:
- imdb_id 기준으로 두 파일 조인
- 줄거리 길이 필터링 (너무 짧은 줄거리 제외)
- 흥행 레이블 생성: 상위 20% = hit(1), 하위 40% = nonhit(1)
"""

from .config import *

def load_files(main_file_path, hit_file_path=HIT_FILE_PATH, threshold=150):
    """
    메인 파일과 흥행 점수 파일을 불러와 벡터화에 적합한 형태로 가공합니다.

    처리 단계:
    1. 두 parquet 파일 로드 및 imdb_id 정규화 (앞뒤 공백 제거)
    2. 줄거리 컬럼명 통일: 'plot', 'description' 등 다양한 이름 → 'overview'로 통일
    3. 중복 imdb_id 처리: 중복이 있으면 점수 평균으로 집계
    4. 두 데이터를 imdb_id 기준으로 inner join
    5. 흥행 레이블 생성:
       - hit_label=1: hit_score 상위 20% (흥행작)
       - nonhit_label=1: hit_score 하위 40% (비흥행작)
    6. 줄거리 길이 threshold 미만 행 제거

    Args:
        main_file_path (str): 영화/드라마 메인 데이터 parquet 경로
        hit_file_path (str): 흥행 점수 데이터 parquet 경로 (기본값: 환경변수)
        threshold (int): 줄거리 최소 길이 (기본값: 150자)

    Returns:
        pd.DataFrame: imdb_id, overview, hit_score, hit_label, nonhit_label 컬럼을 가진 데이터
    """
    main = pd.read_parquet(main_file_path)
    main["imdb_id"] = main["imdb_id"].astype(str).str.strip()
    hit = pd.read_parquet(hit_file_path)
    hit["imdb_id"] = hit["imdb_id"].astype(str).str.strip()

    # 줄거리 컬럼명 통일: 데이터셋마다 이름이 다를 수 있으므로 후보 목록에서 찾아 'overview'로 통일
    overview_candidates = ["overview", "plot", "description", "summary", "storyline"]
    overview_col = next((c for c in overview_candidates if c in main.columns), None)

    if overview_col != "overview":
        main = main.rename(columns={overview_col: "overview"})
    else:
        pass

    # 중복 imdb_id 처리: 동일 작품이 여러 행으로 있을 경우 점수들의 평균을 사용
    dup_cnt = hit.duplicated("imdb_id").sum()

    if dup_cnt > 0:
        hit_agg = (
            hit.groupby("imdb_id", as_index=False)[["rating", "num_votes_log", "sentiment_score", "hit_score"]]
               .mean()
        )
    else:
        hit_agg = hit.copy()

    # inner join: 두 파일 모두에 존재하는 imdb_id만 유지
    df = main.merge(hit_agg[['imdb_id', 'hit_score']], on="imdb_id", how="inner")[['imdb_id', 'overview', 'hit_score']]

    df["len_overview"] = df["overview"].str.len()

    # 흥행 레이블 생성: 분위수(quantile) 기준으로 이진 레이블 부여
    # hit_label=1: 상위 20% (80th percentile 이상) → 흥행작
    df['hit_label'] = 0
    df.loc[df['hit_score'] >= np.quantile(df['hit_score'], 0.8), 'hit_label'] = 1
    # nonhit_label=1: 하위 40% (40th percentile 이하) → 비흥행작
    df['nonhit_label'] = 0
    df.loc[df['hit_score'] <= np.quantile(df['hit_score'], 0.4), 'nonhit_label'] = 1

    # 줄거리가 너무 짧거나 hit_score가 없는 행 제거
    return df.loc[(df["len_overview"] >= threshold) & (df["hit_score"].notna())].reset_index(drop=True)