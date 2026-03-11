from .config import *

def cluster_topics(topic_model, n_groups):
    """
    토픽 간 거리를 계산하고 유사한 토픽끼리 그룹화

    Args:
        topic_model: 학습된 BERTopic 모델
        n_groups: 원하는 그룹 수
        label: 출력 시 표시할 라벨 (흥행작/비흥행작)

    Returns:
        topic_clusters: 토픽별 클러스터 정보 DataFrame
    """

    def suggest_n_clusters(n_topics):
        """
        토픽 수에 따른 경험적 추천
        """
        if n_topics <= 5:
            return 2
        elif n_topics <= 10:
            return 3
        elif n_topics <= 20:
            return int(np.sqrt(n_topics))  # √n
        elif n_topics <= 30:
            return int(n_topics / 4)
        else:
            return int(n_topics / 5)

    # 토픽 임베딩(좌표) 추출
    topic_embeddings = topic_model.topic_embeddings_

    # 토픽 정보 (outlier -1 제외)
    topic_info = topic_model.get_topic_info()
    valid_topics = topic_info[topic_info['Topic'] != -1]['Topic'].tolist()

    # outlier(-1)는 인덱스 0에 있으므로, 실제 토픽은 인덱스 1부터
    valid_embeddings = topic_embeddings[1:len(valid_topics) + 1]

    # 계층적 클러스터링
    clustering = AgglomerativeClustering(
        n_clusters=n_groups,
        metric='cosine',
        linkage='average'
    )
    cluster_labels = clustering.fit_predict(valid_embeddings)

    # 결과 정리
    topic_clusters = pd.DataFrame({
        'topic_num': valid_topics,
        'cluster': cluster_labels,
        'cnt': [topic_info[topic_info['Topic'] == t]['Count'].values[0] for t in valid_topics],
        'keyword': [', '.join([w for w, s in topic_model.get_topic(t)[:5]]) for t in valid_topics]
    })

    # 클러스터별 요약
    cluster_summary = topic_clusters.groupby('cluster').agg({
        'topic_num': lambda x: list(x),
        'cnt': 'sum'
    }).reset_index()

    cluster_summary.columns = ['cluster', 'topic_num', 'cnt']

    print(f"\n[클러스터 요약]")
    print(cluster_summary.to_string(index=False))

    return topic_clusters, cluster_summary


def create_topic_summary(topic_model, df_subset, label):
    """토픽 분석 결과 요약"""
    topic_info = topic_model.get_topic_info()
    results = []

    for topic_id in sorted(topic_info['Topic'].unique()):
        if topic_id != -1:
            keywords = topic_model.get_topic(topic_id)
            top_keywords = [word for word, score in keywords[:5]]
            topic_dramas = df_subset[df_subset['topic'] == topic_id]

            # hit_score 평균 계산
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
    """모델의 모든 토픽에서 키워드와 점수 추출"""
    all_keywords = {}
    topic_info = topic_model.get_topic_info()

    for topic_id in topic_info['Topic'].values:
        if topic_id != -1:
            keywords = topic_model.get_topic(topic_id)
            for word, score in keywords:
                if word in all_keywords:
                    all_keywords[word] = max(all_keywords[word], score)
                else:
                    all_keywords[word] = score

    return all_keywords

def save_topic_keywords(topic_model, output_path):
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

    pd.DataFrame(rows).to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"  ✓ 키워드 파일 저장: {output_path}")

def create_drama_umap_map(df_topics, df_embeddings, topic_clusters, label=""):
    """
    각 드라마별 UMAP 좌표 생성 (Streamlit용)
    """
    print(f"\n{label} 드라마별 UMAP 맵 생성 중...")

    embeddings_list = []
    valid_rows = []

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

    #UMAP 새로 학습 (작품 단위)
    n_neighbors = min(15, len(embeddings) - 1)
    umap_model = UMAP(
        n_neighbors=n_neighbors,
        n_components=2,
        min_dist=0.1,
        metric='cosine',
        random_state=42
    )

    umap_coords = umap_model.fit_transform(embeddings)

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
        'umap_x': umap_coords[:, 0],
        'umap_y': umap_coords[:, 1],
    })

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

