"""
BERT 기반 텍스트 임베딩 모듈

BERT 임베딩이란?
  단어나 문장의 의미를 수백~수천 차원의 숫자 벡터로 표현하는 기술입니다.
  TF-IDF와 달리 단어의 문맥적 의미를 이해합니다.
  예: "action-packed thriller"와 "exciting adventure"는 단어는 다르지만
      BERT는 두 표현의 의미가 비슷함을 수치로 표현할 수 있습니다.

사용 모델: Qwen/Qwen3-Embedding-0.6B
  Qwen3 기반의 텍스트 임베딩 전용 모델. 다국어를 지원하며 의미 검색에 최적화됩니다.

청크(chunk) 처리란?
  데이터가 너무 많을 때 일정 크기(chunk_size)로 나누어 처리하고,
  각 청크 결과를 파일로 저장한 뒤 마지막에 합치는 방식입니다.
  메모리 부족 문제를 방지하고, 중간에 오류가 나도 처음부터 다시 시작하지 않아도 됩니다.
"""

from .config import *

class BERTVectorizer():
    """
    BERT(SentenceTransformer) 모델로 텍스트를 임베딩 벡터로 변환하는 클래스.

    주요 기능:
    - GPU(CUDA/MPS) 자동 감지 및 사용
    - 전체 데이터를 한 번에 또는 청크 단위로 처리
    - 청크별 결과를 parquet 파일로 저장 (중간 저장)
    - 저장된 청크들을 하나로 병합

    Args:
        type (str): 처리 유형 식별자 (예: 'movie', 'drama', 'review')
                    청크 파일 저장 경로에 사용됩니다.
    """

    def __init__(
            self,
            type
        ):

        self.type = type

        # GPU 우선 사용: CUDA(NVIDIA) > MPS(Apple Silicon) > CPU 순서로 감지
        if torch.cuda.is_available():
            self.device = 'cuda'
        elif torch.backends.mps.is_available():
            self.device = 'mps'
        else:
            self.device = 'cpu'

        # Qwen3 임베딩 모델 로드 (HuggingFace Hub에서 자동 다운로드)
        self.model = SentenceTransformer(
            'Qwen/Qwen3-Embedding-0.6B',
            device=self.device
        )

    def fit_transform(
            self,
            data,
            columns,
            use_chunk = True
        ):
        """
        텍스트 데이터를 임베딩 벡터로 변환합니다.

        use_chunk=True (권장):
            - 데이터를 10,000개씩 나누어 처리하고 각 청크를 parquet으로 저장
            - 처리 중 오류가 나도 저장된 청크부터 이어서 처리 가능
            - 메모리 부족 방지

        use_chunk=False:
            - 전체 데이터를 한 번에 처리 (소규모 데이터에 적합)

        Args:
            data (pd.DataFrame): 임베딩할 데이터
            columns (list[str]): 임베딩에 사용할 컬럼 목록
            use_chunk (bool): 청크 단위 처리 여부 (기본값: True)

        Returns:
            pd.DataFrame: imdb_id + embedding 컬럼이 있는 결과 DataFrame
        """
        if use_chunk:
            self.fit_with_chunks(data, columns)
            return self.merge_chunks()
        else:
            return self.fit(data, columns)

    def fit(
            self,
            data,
            columns
        ):
        """
        전체 데이터를 한 번에 임베딩합니다.

        컬럼이 1개인 경우: 해당 컬럼 텍스트를 그대로 사용
        컬럼이 2개 이상인 경우: 여러 컬럼을 하나의 문장으로 합쳐서 사용
          예: overview + genres → "Overview: A hero fights... . Genres: Action, Drama."

        Args:
            data (pd.DataFrame): 임베딩할 데이터
            columns (list[str]): 사용할 컬럼 목록

        Returns:
            pd.DataFrame: {'imdb_id': ..., 'embedding': [...]} 형태의 결과
        """
        df = data.copy()
        id_col = df['imdb_id']

        missing_cols = set(columns) - set(df.columns)
        if missing_cols:
            raise KeyError(f"컬럼이 없습니다: {missing_cols}")

        texts = []

        if len(columns) < 2:
            texts = df[columns[0]].fillna('Unknown').tolist()
        else:
            # 여러 컬럼을 하나의 텍스트로 합치기: "Column Name: value. Column Name: value."
            texts = self._combine_columns(df, columns).tolist()

        # BERT 임베딩 생성
        # batch_size=32: 32개씩 묶어서 GPU에 한 번에 처리 (속도 향상)
        # normalize_embeddings=True: 벡터 길이를 1로 정규화 (코사인 유사도 계산 용이)
        embeddings = self.model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        return pd.DataFrame({
            'imdb_id': id_col,
            'embedding': [emb.tolist() for emb in embeddings]  # numpy 배열 → Python 리스트로 변환
        })

    def fit_with_chunks(
            self,
            data,
            columns,
            output_dir = 'files/embeddings/chunks/',
            chunk_size = 10000,
        ):
        """
        대용량 데이터를 청크 단위로 나누어 임베딩하고 각 청크를 parquet으로 저장합니다.

        처리 흐름:
        1. 데이터를 chunk_size(기본 10,000)개씩 분할
        2. 각 청크를 fit()으로 임베딩
        3. 청크 결과를 'chunk_00000.parquet' 형식으로 저장
        4. GPU 메모리 정리 (다음 청크를 위해)

        저장 경로: output_dir/{type}/chunk_00000.parquet, chunk_00001.parquet, ...

        Args:
            data (pd.DataFrame): 임베딩할 전체 데이터
            columns (list[str]): 사용할 컬럼 목록
            output_dir (str): 청크 파일 저장 기본 경로
            chunk_size (int): 청크 크기 (기본값: 10,000)
        """
        import os

        output_dir_f = f'{output_dir}/{self.type}'
        os.makedirs(output_dir_f, exist_ok=True)

        n_total = len(data)
        # 올림 나눗셈: (n_total + chunk_size - 1) // chunk_size
        n_chunks = (n_total + chunk_size - 1) // chunk_size

        for chunk_idx in range(n_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, n_total)

            df_chunk = data.iloc[start_idx:end_idx].copy()
            df_chunk = self.fit(df_chunk, columns)
            output_path = f"{output_dir_f}/chunk_{chunk_idx:05d}.parquet"
            df_chunk.to_parquet(output_path, index=False)

            # GPU 메모리 정리: 처리 완료된 텐서를 GPU 메모리에서 해제
            if self.device == "cuda":
                torch.cuda.empty_cache()
            elif self.device == "mps":
                torch.mps.empty_cache()

    def merge_chunks(
            self,
            output_dir = 'files/embeddings/chunks/'
        ):
        """
        fit_with_chunks()로 저장된 청크 파일들을 하나의 DataFrame으로 병합합니다.

        파일명 패턴 chunk_*.parquet을 찾아 알파벳순으로 정렬한 뒤
        순서대로 읽어서 이어 붙입니다.

        Args:
            output_dir (str): 청크 파일들이 저장된 기본 경로

        Returns:
            pd.DataFrame: 모든 청크를 합친 전체 임베딩 결과
        """
        import os
        import glob

        output_dir_f = f'{output_dir}/{self.type}'
        pattern = os.path.join(output_dir_f, "chunk_*.parquet")
        chunk_files = sorted(glob.glob(pattern))  # 파일명 순서대로 정렬

        if not chunk_files:
            print("No chunk files found")
            return pd.DataFrame()

        print(f"▶ Found {len(chunk_files)} chunk files")

        dfs = []
        for path in tqdm(chunk_files, desc="Loading chunks"):
            dfs.append(pd.read_parquet(path))

        # ignore_index=True: 각 청크의 인덱스를 무시하고 0부터 재부여
        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def _combine_columns(df, columns):
        """
        여러 컬럼의 텍스트를 하나의 문장으로 합칩니다.

        형식: "Column Name: value. Column Name: value."
        예: ["overview", "genres"] → "Overview: A detective investigates... . Genres: Action, Drama."

        - 컬럼명의 언더스코어(_)를 공백으로 바꾸고 첫 글자 대문자로 변환
        - 빈 값은 'Unknown'으로 채움

        Args:
            df (pd.DataFrame): 데이터
            columns (list[str]): 합칠 컬럼 목록

        Returns:
            pd.Series: 합쳐진 텍스트 시리즈
        """
        formatted_series = [
            (col.replace('_', ' ').title() + ': ' +
             df[col].fillna('Unknown').replace('', 'Unknown').astype(str))
            for col in columns
        ]

        # str.cat()으로 벡터화 결합: 반복문 없이 한 번에 전체 행 처리 (속도 향상)
        return formatted_series[0].str.cat(formatted_series[1:], sep='. ')