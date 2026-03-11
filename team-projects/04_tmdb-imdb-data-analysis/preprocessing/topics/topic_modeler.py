"""
BERTopic 기반 토픽 모델링 모듈

토픽 모델링이란?
  수백~수만 개의 문서(줄거리, 리뷰 등)에서 자동으로 주제(토픽)를 찾아내는 기술입니다.
  예: 1000개의 영화 줄거리를 분석하면 "범죄/수사", "로맨스", "SF/우주" 등의 토픽이 자동 추출됩니다.

BERTopic 특징:
  - 단순 단어 빈도가 아닌 의미 유사도 기반으로 토픽을 찾음
  - 이상치(-1 토픽)로 분류된 문서를 가장 가까운 토픽으로 재배정 가능

이상치(outlier) 병합이란?
  HDBSCAN이 어떤 클러스터에도 속하지 않는다고 판단한 문서를 토픽 -1로 분류합니다.
  reduce_outliers()로 이 문서들을 임베딩 유사도를 기준으로 가장 가까운 토픽에 배정합니다.
"""

from .config import *

class TopicModeler:
    """
    BERTopic을 사용해 영화/드라마/리뷰 데이터의 토픽을 추출하는 클래스.

    처리 흐름:
    1. 입력 데이터에서 임베딩 벡터와 텍스트 추출
    2. 콘텐츠 유형(drama/movie/review)에 맞는 불용어 설정
    3. BERTopic 모델 생성 (UMAP + HDBSCAN + c-TF-IDF)
    4. 모델 학습 및 토픽 추출
    5. 이상치 병합
    6. 결과 저장 (parquet + HTML 시각화)

    Args:
        data (pd.DataFrame): 'embedding', 'overview', 'imdb_id', 'title' 컬럼 포함
        type_name (str): 콘텐츠 유형 - 'drama', 'movie', 'review' 중 하나
    """

    def __init__(
            self,
            data,
            type_name
        ):

        # 분석할 데이터 저장
        self.data = data

        # 임베딩 벡터: 각 행의 embedding을 세로로 쌓아 2D 배열로 변환
        # np.vstack: [(768,), (768,), ...] → (N, 768) 행렬
        self.embeddings = np.vstack(self.data['embedding'].values)
        # c-TF-IDF 계산에 사용할 텍스트 (줄거리): BERTopic이 각 토픽 키워드를 이 텍스트에서 추출
        self.texts_for_ctfidf = self.data['overview'].tolist()

        base_stopwords = list(ENGLISH_STOP_WORDS)

        # 콘텐츠 유형별 불용어 설정
        # 불용어(stopword): 의미는 없지만 빈도가 높아 토픽 추출을 방해하는 단어
        # 유형별로 다른 상투어를 제거합니다 (드라마 vs 영화 vs 리뷰 특유의 반복 표현)
        # TF-IDF
        if type_name == 'drama':
            additional_drama_stopwords = [
            # ========== 드라마 포맷/메타 ==========
            'tv', 'television', 'show', 'series', 'episode', 'episodes',
            'season', 'seasons', 'installment',
            'pilot', 'finale',

            # ========== 제작/형식 정보 ==========
            'drama', 'dramas',
            'network', 'broadcast', 'air', 'airs',
            'production', 'produced',
            'creator', 'creators',
            'cast', 'crew',
            'actor', 'actors', 'actress', 'actresses',
            'director', 'directors',
            'writer', 'writers',

            # ========== 줄거리 서술 상투어 ==========
            'story', 'stories', 'plot',
            'follows', 'following',
            'centers', 'centred', 'revolves',
            'tells', 'depicts', 'chronicles',
            'focuses', 'explores',
            'takes', 'place',
            'begins', 'starts', 'ends',
            'finds', 'discovers', 'faces', 'way', 'actually', 'la',

            # ========== 일반적 시간 표현 ==========
            'time', 'times',
            'day', 'days',
            'year', 'years',
            'night', 'nights',
            'past', 'present', 'future',
            'later', 'earlier', 'soon',

            # ========== 순서/전개 표현 ==========
            'first', 'second', 'third',
            'last', 'next', 'previous',
            'early', 'late',

            # ========== 일반적 인물 지칭 ==========
            'man', 'woman', 'men', 'women',
            'person', 'people',
            'group', 'groups',
            'team', 'teams',
            'members', 'characters',
            # ========== 너무 일반적인 사건 형용사 ==========
            'high', 'characters', 'just', 'new',
            # ========== 너무 일반적인 사건 동사 ==========
            'life', 'lives',
            'work', 'works',
            'deal', 'deals', 'step', 'gets', 'decides',
            'struggle', 'struggles', 'make', 'sees', 'set',
            # ========== 고유명사 ==========
            'ryan', 'henry', 'james', 'xun', 'gu', 'ma ri', 'ri', 'ma', 'fernanda', 'rosendo', 'tyler',
            'carmina', 'mariela', 'lou'
            # 불용어에 추가 가능
                                  'öykü', 'demir', 'hanzawa', 'leonardo', 'damián', 'eva', 'elisa', 'esteban', 'tori',
            "eliseo", "sam", "ellen", "charlotte", "jarndyce", "alex",
        ]
            stopwords = list(set(base_stopwords + additional_drama_stopwords))

        elif type_name == 'movie':
            additional_movie_stopwords = [
                # ========== 영화 도메인 공통어 ==========
                'film', 'films', 'movie', 'movies', 'story', 'stories',
                'character', 'characters', 'scene', 'scenes', 'plot',
                'protagonist', 'audience', 'viewer', 'viewers',
                'series', 'sequel', 'part', 'chapter',
                'director', 'actor', 'actress', 'cast', 'crew',
                'documentary', 'footage', 'screen',

                # ========== 줄거리 서술 상투어 ==========
                'based', 'true', 'real', 'events', 'set',
                'follows', 'following', 'centers', 'revolves', 'tells',
                'takes', 'place', 'turns', 'finds', 'discovers', 'house',
                'begins', 'starts', 'ends', 'leads', 'brings', 'named', 'live', 'lives', 'meet', 'meets',

                # ========== 일반적 시간/수량 표현 ==========
                'time', 'times', 'year', 'years', 'day', 'days', 'night', 'nights',
                'moment', 'moments', 'later', 'ago', 'soon',
                'one', 'two', 'three', 'first', 'second', 'third', 'last',

                # ========== 일반적 인물 지칭 ==========
                'man', 'woman', 'men', 'women', 'people', 'person', 'guy', 'guys', 'self',
                'group', 'team', 'crew', 'members', 'girl', 'girls', 'boy', 'boys', 'dog', 'dogs',

                # ========== 기타 인물 이름 ==========
                'lena', 'jack', 'john', 'mary', 'sarah', 'mike', 'david', 'james', 'robert',
            ]
            stopwords = list(set(base_stopwords + additional_movie_stopwords))

        elif type_name == 'review':
            additional_review_stopwords = [
                "like","just","good","really","time","way","watch","watched","watching","people",
                "dont","didnt","doesnt","isnt","wasnt","werent","cant","couldnt","wouldnt",
                "im","ive","youre","theyre","thats","theres","hes","shes","weve","id",
                # 형식/대상 단어(드라마 쪽 강화)
                "film","films","movie","movies","show","shows","series","season","seasons","episode","episodes",
                "drama","dramas","tv","television",
                "story","plot","character","characters"
            ]
            stopwords = list(set(base_stopwords + additional_review_stopwords))

        else:
            stopwords = base_stopwords

        # c-TF-IDF용 CountVectorizer: 토픽 키워드 추출에 사용
        # ngram_range=(1,2): 단일 단어와 2단어 조합 모두 분석
        # min_df=1: 최소 1개 문서에만 있어도 포함
        # max_df=0.95: 95% 이상 문서에 등장하는 단어는 제외 (너무 흔한 단어)
        self.vectorizer_model = CountVectorizer(
            stop_words=stopwords,
            ngram_range=(1, 2),
            min_df=1,
            max_df=0.95
        )

        # UMAP 파라미터: 데이터 크기에 따라 자동 조정
        # n_neighbors: 지역 구조 보존 정도 (작을수록 세밀한 구조, 최대 데이터 수-1)
        self.n_neighbors = min(10, len(self.data) - 1)
        # HDBSCAN 파라미터: 데이터 크기에 비례하여 최소 클러스터 크기 설정
        # 너무 작으면 작은 노이즈도 토픽이 되고, 너무 크면 토픽이 적게 나옴
        self.min_cluster = max(15, len(self.data) // 100)
        self.embedding_model = embedding_model

        # 학습될 BERTopic 모델 (fit_transform() 호출 전까지는 None)
        self.bertopic_model = None

        # 토픽 분석 결과 DataFrame
        self.result_data = pd.DataFrame()

    def create_bertopic_model(self, min_samples=10):
        """
        BERTopic 모델을 생성합니다 (학습은 fit_transform()에서 진행).

        구성 요소:
        - UMAP: 고차원 임베딩(768차원)을 10차원으로 축소
          - n_neighbors: 지역 구조 보존 범위 (작을수록 세밀)
          - min_dist=0.05: 점들이 뭉칠 수 있는 정도 (작을수록 조밀)
          - metric='cosine': 벡터 간 각도 기반 거리 (BERT 임베딩에 적합)

        - HDBSCAN: 밀도 기반 클러스터링
          - min_cluster_size: 최소 클러스터 크기 (이보다 작으면 이상치로 처리)
          - min_samples: 핵심 점으로 인정받기 위한 최소 이웃 수
          - cluster_selection_method='leaf': 세분화된 클러스터 선택 (더 많은 토픽)
          - prediction_data=True: 새 문서의 토픽 예측 가능하게 설정

        Args:
            min_samples (int): HDBSCAN 최소 샘플 수 (기본값: 10)
        """
        self.bertopic_model = BERTopic(
            embedding_model=embedding_model,

            # UMAP: 차원 축소 (768차원 → 10차원)
            umap_model=UMAP(
                n_neighbors=self.n_neighbors,
                n_components=10,    # 출력 차원 수
                min_dist=0.05,
                metric='cosine',
                random_state=42     # 재현성을 위한 랜덤 시드 고정
            ),

            # HDBSCAN: 밀도 기반 클러스터링 (비슷한 문서 그룹 = 토픽)
            hdbscan_model=HDBSCAN(
                min_cluster_size=self.min_cluster,
                min_samples=min_samples,
                metric='euclidean',
                cluster_selection_method='leaf',
                prediction_data=True
            ),

            vectorizer_model=self.vectorizer_model,
            verbose=True  # 학습 진행 상황 출력
        )

    def fit_transform(self):
        """
        BERTopic 모델을 학습하고 각 문서에 토픽을 할당합니다.

        처리 흐름:
        1. BERTopic 모델 생성 (create_bertopic_model)
        2. 텍스트와 사전 계산된 임베딩으로 모델 학습
           - 텍스트: c-TF-IDF 키워드 추출용 (줄거리)
           - 임베딩: 클러스터링용 (장르+줄거리 합친 임베딩 벡터)
        3. 이상치(토픽 -1) 병합: 임베딩 유사도 기준으로 가장 가까운 토픽에 배정
        4. 토픽 업데이트: 새로운 토픽 할당으로 키워드 재계산
        5. 결과를 self.result_data에 저장
        """
        self.create_bertopic_model()

        # BERTopic 학습: 텍스트로 키워드를 추출하고, 임베딩으로 클러스터링 수행
        # 사전 계산된 임베딩을 전달하면 재계산하지 않아 속도가 빠름
        topics, probs = self.bertopic_model.fit_transform(
            self.texts_for_ctfidf,      # c-TF-IDF용 텍스트 (줄거리만)
            embeddings=self.embeddings  # 기존 임베딩 재사용 (장르+줄거리 합친 벡터)
        )

        # 이상치 병합: 토픽 -1로 분류된 문서를 유사도 기반으로 기존 토픽에 배정
        # strategy="embeddings": 임베딩 벡터 유사도로 가장 가까운 토픽 선택
        # threshold=0.6: 유사도 0.6 이상인 토픽에만 배정 (너무 다른 건 -1 유지)
        new_topics = self.bertopic_model.reduce_outliers(
            documents=self.texts_for_ctfidf,  # 반드시 텍스트 리스트
            topics=topics,
            strategy="embeddings",
            embeddings=self.embeddings,        # 임베딩 직접 전달 (재계산 방지)
            threshold=0.6
        )
        # 새로운 토픽 할당으로 c-TF-IDF 키워드 재계산
        self.bertopic_model.update_topics(
            self.texts_for_ctfidf,
            topics=new_topics,
            vectorizer_model=self.vectorizer_model
        )

        # 결과 저장: 각 작품에 토픽 번호 추가
        df_result = self.data[['imdb_id', 'title', 'combined_text']].copy()
        df_result['topic'] = new_topics
        self.result_data = df_result

    def save_results(self, save_point):
        """
        학습 결과를 파일로 저장합니다.

        저장 내용:
        1. {save_point}_topics.parquet: 각 작품의 토픽 번호 (imdb_id, title, topic 등)
        2. {save_point}_topic_info.parquet: 각 토픽의 문서 수, 대표 키워드 등 메타정보
        3. {save_point}_bertopic_model/: 학습된 BERTopic 모델 파일 (safetensors 형식)
        4. topics_*.html: 토픽 시각화 파일들 (인터랙티브 HTML)

        저장 경로: BERT_OUTPUT_DIR/{save_point}/

        Args:
            save_point (str): 저장 폴더명 및 파일명 접두어 (예: 'movie', 'drama')
        """
        # 데이터 경로 지정
        load_dotenv()
        output_dir = f"{os.getenv('BERT_OUTPUT_DIR')}"

        os.makedirs(output_dir, exist_ok=True)
        save_dir = f"{output_dir}/{save_point}"
        os.makedirs(save_dir, exist_ok=True)

        # 1. 토픽 할당 결과 저장
        self.result_data.to_parquet(f"{save_dir}/{save_point}_topics.parquet", index=False)
        print(f"  ✓ {save_dir}/{save_point}_topics.parquet")

        # 2. 토픽 정보(메타데이터) 저장: 각 토픽의 크기, 키워드 등
        topic_info = self.bertopic_model.get_topic_info()
        topic_info.to_parquet(f"{save_dir}/{save_point}_topic_info.parquet", index=False)
        print(f"  ✓ {save_dir}/{save_point}_topic_info.csv")

        # 3. 모델 파일 저장 (safetensors: 안전하고 빠른 텐서 저장 형식)
        try:
            self.bertopic_model.save(
                f"{save_dir}/{save_point}_bertopic_model",
                serialization="safetensors",
                save_ctfidf=True,
                save_embedding_model=False  # 임베딩 모델은 용량이 크므로 제외
            )
            print(f"  ✓ {save_dir}/bertopic_model/")

        except Exception as e:
            print(f"  ✗ 모델 저장 실패: {e}")

        # 4. HTML 시각화 저장
        self._save_visualizations(save_dir)

    def _save_visualizations(self, output_dir):
        """
        BERTopic의 다양한 시각화 결과를 인터랙티브 HTML 파일로 저장합니다.

        생성되는 시각화:
        - topics_barchart.html: 각 토픽의 상위 키워드 막대 그래프
        - topics_intertopic.html: 2D 공간에서 토픽 간 거리를 나타낸 산점도
        - topics_hierarchy.html: 토픽들의 계층적 유사성 트리 (덴드로그램)
        - topics_heatmap.html: 토픽 간 유사도를 색상으로 나타낸 히트맵
        - topics_documents.html: 2D UMAP 공간에서 문서 분포 (색상 = 토픽)

        Args:
            output_dir (str): HTML 파일을 저장할 디렉토리
        """
        visualizations = {
            'barchart': {
                'func': lambda: self.bertopic_model.visualize_barchart(top_n_topics=10),
                'name': '토픽별 키워드 막대그래프'
            },
            'intertopic': {
                'func': lambda: self.bertopic_model.visualize_topics(),
                'name': '토픽 간 거리맵'
            },
            'hierarchy': {
                'func': lambda: self.bertopic_model.visualize_hierarchy(
                    hierarchical_topics=self.bertopic_model.hierarchical_topics(self.texts_for_ctfidf)
                ),
                'name': '계층적 토픽 구조'
            },
            'heatmap': {
                'func': lambda: self.bertopic_model.visualize_heatmap(),
                'name': '토픽 유사도 히트맵'
            },
            'documents': {
                'func': lambda: self.bertopic_model.visualize_documents(
                    self.texts_for_ctfidf,
                    embeddings=self.embeddings,
                    hide_annotations=True
                ),
                'name': '문서 분포 시각화'
            }
        }

        for viz_type, viz_info in visualizations.items():
            try:
                fig = viz_info['func']()
                filepath = f"{output_dir}/topics_{viz_type}.html"
                fig.write_html(filepath)
                print(f"  ✓ {filepath}")
            except Exception as e:
                print(f"  ✗ {viz_info['name']} 저장 실패: {e}")