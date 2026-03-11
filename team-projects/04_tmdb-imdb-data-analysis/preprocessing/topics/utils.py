"""
토픽 모델링 유틸리티 함수 모듈

BERTopic으로 추출된 토픽들을 후처리하고 분석하는 함수들을 제공합니다.

주요 기능:
- 토픽 클러스터링: 비슷한 토픽끼리 상위 그룹으로 묶기
- 토픽 요약: 토픽별 흥행 점수, 키워드, 샘플 작품 정보 정리
- UMAP 시각화 데이터 생성: Streamlit 대시보드용 2D 좌표 계산
"""

from .config import *

def cluster_topics(topic_model, n_groups):
    """
    BERTopic으로 추출된 개별 토픽들을 n_groups개의 상위 그룹으로 묶습니다.

    원리:
    - BERTopic이 각 토픽에 대한 임베딩 벡터(topic_embeddings_)를 제공합니다
    - 이 토픽 임베딩들을 계층적 클러스터링(AgglomerativeClustering)으로 그룹화합니다
    - 코사인 거리를 사용하므로 의미가 비슷한 토픽끼리 같은 그룹에 배정됩니다

    예시:
    - 토픽 0 "detective murder crime", 토픽 5 "police investigation suspect"
      → 둘 다 '범죄/수사' 클러스터로 묶임

    Args:
        topic_model: 학습된 BERTopic 모델
        n_groups (int): 만들 클러스터(상위 그룹) 수

    Returns:
        tuple: (topic_clusters, cluster_summary)
            topic_clusters: 토픽별 클러스터 번호와 키워드 DataFrame
            cluster_summary: 클러스터별 포함된 토픽 목록과 문서 수 요약 DataFrame
    """

    def suggest_n_clusters(n_topics):
        """
        토픽 수에 따른 적절한 클러스터 수를 경험적으로 추천합니다.
        토픽이 많을수록 클러스터도 많아지지만, 비율은 줄어듭니다.
        """
        if n_topics <= 5:
            return 2
        elif n_topics <= 10:
            return 3
        elif n_topics <= 20:
            return int(np.sqrt(n_topics))  # √n 개
        elif n_topics <= 30:
            return int(n_topics / 4)
        else:
            return int(n_topics / 5)

    # BERTopic이 학습한 토픽별 임베딩 벡터 추출
    # topic_embeddings_: (토픽 수 + 1, 임베딩 차원) 형태 (인덱스 0 = outlier 토픽)
    topic_embeddings = topic_model.topic_embeddings_

    # 이상치 토픽(-1)을 제외한 유효 토픽 목록
    topic_info = topic_model.get_topic_info()
    valid_topics = topic_info[topic_info['Topic'] != -1]['Topic'].tolist()

    # 인덱스 0은 outlier(-1)이므로, 실제 토픽 임베딩은 인덱스 1부터 시작
    valid_embeddings = topic_embeddings[1:len(valid_topics) + 1]

    # 계층적 클러스터링: 코사인 거리 기준으로 토픽들을 n_groups개 그룹으로 분류
    # linkage='average': 그룹 간 거리를 모든 점 쌍의 평균으로 계산 (안정적인 결과)
    clustering = AgglomerativeClustering(
        n_clusters=n_groups,
        metric='cosine',
        linkage='average'
    )
    cluster_labels = clustering.fit_predict(valid_embeddings)

    # 결과를 DataFrame으로 정리 (토픽 번호, 클러스터 번호, 문서 수, 대표 키워드)
    topic_clusters = pd.DataFrame({
        'topic_num': valid_topics,
        'cluster': cluster_labels,
        'cnt': [topic_info[topic_info['Topic'] == t]['Count'].values[0] for t in valid_topics],
        'keyword': [', '.join([w for w, s in topic_model.get_topic(t)[:5]]) for t in valid_topics]
    })

    # 클러스터별 요약: 포함된 토픽 목록과 전체 문서 수
    cluster_summary = topic_clusters.groupby('cluster').agg({
        'topic_num': lambda x: list(x),
        'cnt': 'sum'
    }).reset_index()

    cluster_summary.columns = ['cluster', 'topic_num', 'cnt']

    print(f"\n[클러스터 요약]")
    print(cluster_summary.to_string(index=False))

    return topic_clusters, cluster_summary


def create_topic_summary(topic_model, df_subset, label):
    """
    토픽별 분석 요약 정보를 생성합니다.

    각 토픽에 대해 다음 정보를 수집합니다:
    - 해당 토픽에 속한 작품 수
    - 평균 흥행 점수 (hit_score)
    - 대표 키워드 5개
    - 샘플 작품명 3개

    Args:
        topic_model: 학습된 BERTopic 모델
        df_subset (pd.DataFrame): 토픽 할당 결과 DataFrame (topic, hit_score, title 컬럼 필요)
        label (str): 그룹 식별자 (예: '흥행작', '비흥행작')

    Returns:
        list[dict]: 토픽별 요약 정보 딕셔너리 목록
    """
    topic_info = topic_model.get_topic_info()
    results = []

    for topic_id in sorted(topic_info['Topic'].unique()):
        if topic_id != -1:  # 이상치 토픽 제외
            keywords = topic_model.get_topic(topic_id)
            top_keywords = [word for word, score in keywords[:5]]  # 상위 5개 키워드
            topic_dramas = df_subset[df_subset['topic'] == topic_id]

            # 해당 토픽 작품들의 평균 흥행 점수
            avg_hit_score = topic_dramas['hit_score'].mean() if len(topic_dramas) > 0 else 0

            results.append({
                'label': label,
                'topic_id': topic_id,
                'drama_count': len(topic_dramas),
                'avg_hit_score': round(avg_hit_score, 4) if not pd.isna(avg_hit_score) else 0,
                'keywords': ', '.join(top_keywords),
                'sample_dramas': ', '.join(
                    topic_dramas['title'].head(3).tolist()) if 'title' in topic_dramas.columns else ''
            })

    return results


def get_all_keywords(topic_model):
    """
    모든 토픽에서 키워드를 수집하고, 같은 키워드가 여러 토픽에 있으면 최고 점수를 유지합니다.

    용도: 전체 모델에서 어떤 단어들이 중요한지 빠르게 파악할 때 사용

    Args:
        topic_model: 학습된 BERTopic 모델

    Returns:
        dict: {단어: 최고 점수} 형태의 딕셔너리
    """
    all_keywords = {}
    topic_info = topic_model.get_topic_info()

    for topic_id in topic_info['Topic'].values:
        if topic_id != -1:  # 이상치 토픽 제외
            keywords = topic_model.get_topic(topic_id)
            for word, score in keywords:
                if word in all_keywords:
                    # 같은 단어가 여러 토픽에 있으면 가장 높은 점수 유지
                    all_keywords[word] = max(all_keywords[word], score)
                else:
                    all_keywords[word] = score

    return all_keywords

def save_topic_keywords(topic_model, output_path):
    """
    모든 토픽의 키워드와 점수를 CSV 파일로 저장합니다.

    각 행은 (토픽 ID, 키워드, 점수)로 구성됩니다.
    이상치 토픽(-1)은 제외합니다.

    Args:
        topic_model: 학습된 BERTopic 모델
        output_path (str): 저장할 CSV 파일 경로
    """
    rows = []
    topic_info = topic_model.get_topic_info()

    for topic_id in topic_info["Topic"]:
        if topic_id == -1:
            continue

        for word, score in topic_model.get_topic(topic_id):
            rows.append({
                "topic_id": topic_id,
                "keyword": word,
                "score": round(score, 6)
            })

    # encoding="utf-8-sig": Windows Excel에서 한글이 깨지지 않도록 BOM 포함
    pd.DataFrame(rows).to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"  ✓ 키워드 파일 저장: {output_path}")

def create_drama_umap_map(df_topics, df_embeddings, topic_clusters, label=""):
    """
    각 작품의 2D UMAP 좌표를 생성합니다 (Streamlit 대시보드 시각화용).

    BERTopic의 UMAP은 10차원으로 축소하지만, 시각화를 위해서는 2차원이 필요합니다.
    이 함수에서 임베딩을 2차원으로 다시 축소하여 x, y 좌표를 생성합니다.

    처리 흐름:
    1. 토픽 데이터와 임베딩 데이터를 imdb_id로 매칭
    2. 매칭된 임베딩을 2D UMAP으로 축소
    3. 토픽 번호를 클러스터 번호와 키워드로 매핑
    4. 결과를 Streamlit용 DataFrame으로 반환

    Args:
        df_topics (pd.DataFrame): 토픽 할당 결과 (imdb_id, topic, title, hit_score 필요)
        df_embeddings (pd.DataFrame): 임베딩 데이터 (imdb_id, embedding 필요)
        topic_clusters (pd.DataFrame): cluster_topics() 결과 (토픽번호, 클러스터, 키워드 컬럼 필요)
        label (str): 출력용 레이블 (예: '영화', '드라마')

    Returns:
        pd.DataFrame: umap_x, umap_y, cluster, keywords 컬럼이 추가된 결과
    """
    print(f"\n{label} 드라마별 UMAP 맵 생성 중...")

    embeddings_list = []
    valid_rows = []

    # imdb_id로 토픽 데이터와 임베딩 매칭
    for _, row in df_topics.iterrows():
        imdb_id = row['imdb_id']
        emb_row = df_embeddings[df_embeddings['imdb_id'] == imdb_id]

        if len(emb_row) > 0:
            embeddings_list.append(emb_row['embedding'].values[0])
            valid_rows.append(row)

    if len(embeddings_list) == 0:
        print(f"⚠ {label}: 유효한 임베딩 없음")
        return pd.DataFrame()

    embeddings = np.vstack(embeddings_list)
    df_valid = pd.DataFrame(valid_rows).reset_index(drop=True)

    print(f"  - 유효 드라마 수: {len(df_valid)}")

    # 작품 단위 2D UMAP 학습: 시각화를 위해 2차원으로 축소
    n_neighbors = min(15, len(embeddings) - 1)
    umap_model = UMAP(
        n_neighbors=n_neighbors,
        n_components=2,    # 2D 좌표 생성
        min_dist=0.1,
        metric='cosine',
        random_state=42
    )

    umap_coords = umap_model.fit_transform(embeddings)

    # 토픽 번호 → 클러스터/키워드 매핑 딕셔너리 생성
    topic_to_cluster = dict(
        zip(topic_clusters['토픽번호'], topic_clusters['클러스터'])
    )
    topic_to_keywords = dict(
        zip(topic_clusters['토픽번호'], topic_clusters['키워드'])
    )

    result = pd.DataFrame({
        'imdb_id': df_valid['imdb_id'].values,
        'title': df_valid['title'].values,
        'topic': df_valid['topic'].values,
        'hit_score': df_valid['hit_score'].values,
        'umap_x': umap_coords[:, 0],   # 2D 좌표 x
        'umap_y': umap_coords[:, 1],   # 2D 좌표 y
    })

    # 토픽 번호를 클러스터 번호로 변환 (매핑 안 되면 -1)
    result['cluster'] = (
        result['topic']
        .map(topic_to_cluster)
        .fillna(-1)
        .astype(int)
    )

    result['keywords'] = result['topic'].map(topic_to_keywords).fillna("")

    print(f"  ✓ 완료: {len(result)}개 드라마")
    print(f"  - 클러스터 분포: {result['cluster'].value_counts().to_dict()}")

    return result

