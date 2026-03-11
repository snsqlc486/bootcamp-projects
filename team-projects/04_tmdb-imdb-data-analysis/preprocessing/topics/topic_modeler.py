from .config import *

class TopicModeler:
    def __init__(
            self,
            data,
            type_name
        ):

        # 데이터
        self.data = data

        # 중요한 정보
        self.embeddings = np.vstack(self.data['embedding'].values)
        self.texts_for_ctfidf = self.data['overview'].tolist()

        base_stopwords = list(ENGLISH_STOP_WORDS)

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

        self.vectorizer_model = CountVectorizer(
            stop_words=stopwords,
            ngram_range=(1, 2),
            min_df=1,
            max_df=0.95
        )

        # 모델 파라미터
        self.n_neighbors = min(10, len(self.data) - 1)
        self.min_cluster = max(15, len(self.data) // 100)
        self.embedding_model = embedding_model

        # 모델
        self.bertopic_model = None

        # 분석 결과
        self.result_data = pd.DataFrame()

    def create_bertopic_model(self, min_samples=10):

        self.bertopic_model = BERTopic(
            embedding_model=embedding_model,

            # UMAP: 차원 축소
            umap_model=UMAP(
                n_neighbors=self.n_neighbors,
                n_components=10,
                min_dist=0.05,
                metric='cosine',
                random_state=42
            ),

            # HDBSCAN: 밀도 기반 클러스터링
            hdbscan_model=HDBSCAN(
                min_cluster_size=self.min_cluster,
                min_samples=min_samples,
                metric='euclidean',
                cluster_selection_method='leaf',
                prediction_data=True
            ),

            vectorizer_model=self.vectorizer_model,
            verbose=True
        )

    def fit_transform(self):
        self.create_bertopic_model()

        topics, probs = self.bertopic_model.fit_transform(
            self.texts_for_ctfidf,  # ← c-TF-IDF용 텍스트 (줄거리만)
            embeddings=self.embeddings  # ← 임베딩은 기존 것 사용 (장르+줄거리)
        )

        # 이상치 병합
        ## 실행
        new_topics = self.bertopic_model.reduce_outliers(
            documents= self.texts_for_ctfidf,  # 첫 번째 인자: 반드시 텍스트 리스트
            topics=topics,  # 두 번째 인자: 기존 토픽 결과
            strategy="embeddings",  # 전략 선택
            embeddings= self.embeddings,  # 임베딩 벡터 직접 전달 (속도 향상)
            threshold=0.6  # 유사도 문턱값
        )
        ## 결과 반영
        self.bertopic_model.update_topics(
            self.texts_for_ctfidf,
            topics=new_topics,
            vectorizer_model=self.vectorizer_model
        )

        # 결과 저장
        df_result = self.data[['imdb_id', 'title', 'combined_text']].copy()
        df_result['topic'] = new_topics
        self.result_data = df_result

    def save_results(self, save_point):
        # 결과 파일 저장

        # 데이터 경로 지정
        load_dotenv()
        output_dir = f"{os.getenv("BERT_OUTPUT_DIR")}"

        os.makedirs(output_dir, exist_ok=True)
        save_dir = f"{output_dir}/{save_point}"
        os.makedirs(save_dir, exist_ok=True)

        self.result_data.to_parquet(f"{save_dir}/{save_point}_topics.parquet", index=False)
        print(f"  ✓ {save_dir}/{save_point}_topics.parquet")

        # 토픽 정보 파일 저장
        topic_info = self.bertopic_model.get_topic_info()
        topic_info.to_parquet(f"{save_dir}/{save_point}_topic_info.parquet", index=False)
        print(f"  ✓ {save_dir}/{save_point}_topic_info.csv")

        # 모델 파일 저장
        try:
            self.bertopic_model.save(
                f"{save_dir}/{save_point}_bertopic_model",
                serialization="safetensors",
                save_ctfidf=True,
                save_embedding_model=False  # 용량 절약
            )
            print(f"  ✓ {save_dir}/bertopic_model/")

        except Exception as e:
            print(f"  ✗ 모델 저장 실패: {e}")

        self._save_visualizations(save_dir)

    def _save_visualizations(self, output_dir):
        """모든 시각화를 HTML로 저장"""

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