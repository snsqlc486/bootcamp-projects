from .config import *

def load_files(main_file_path, hit_file_path= HIT_FILE_PATH, threshold= 150):
    """
    메인 파일과 흥행 점수 파일을 불러옵니다.

    파라미터
    main_file_path : 메인 파일 위치, 기본값 : MAIN_FILE_PATH
    hit_file_path : 점수 파일 위치, 기본값 : HIT_FILE_PATH
    threshold : 줄거리 길이 하한값, 기본값 : 150
    """
    main = pd.read_parquet(main_file_path)
    main["imdb_id"] = main["imdb_id"].astype(str).str.strip()
    hit = pd.read_parquet(hit_file_path)
    hit["imdb_id"] = hit["imdb_id"].astype(str).str.strip()

    overview_candidates = ["overview", "plot", "description", "summary", "storyline"]
    overview_col = next((c for c in overview_candidates if c in main.columns), None)

    if overview_col != "overview":
        main = main.rename(columns={overview_col: "overview"})
    else:
        pass

    dup_cnt = hit.duplicated("imdb_id").sum()

    if dup_cnt > 0:
        hit_agg = (
            hit.groupby("imdb_id", as_index=False)[["rating", "num_votes_log", "sentiment_score", "hit_score"]]
               .mean()
        )
    else:
        hit_agg = hit.copy()

    df = main.merge(hit_agg[['imdb_id', 'hit_score']], on="imdb_id", how="inner")[['imdb_id', 'overview', 'hit_score']]

    df["len_overview"] = df["overview"].str.len()
    df['hit_label'] = 0
    df.loc[df['hit_score'] >= np.quantile(df['hit_score'], 0.8), 'hit_label'] = 1
    df['nonhit_label'] = 0
    df.loc[df['hit_score'] <= np.quantile(df['hit_score'], 0.4), 'nonhit_label'] = 1

    return df.loc[(df["len_overview"] >= threshold) & (df["hit_score"].notna())].reset_index(drop=True)