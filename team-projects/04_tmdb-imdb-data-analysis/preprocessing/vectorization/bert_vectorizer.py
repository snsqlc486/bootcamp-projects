from .config import *

class BERTVectorizer():
    def __init__(
            self,
            type
        ):

        self.type = type

        if torch.cuda.is_available():
            self.device = 'cuda'
        elif torch.backends.mps.is_available():
            self.device = 'mps'
        else:
            self.device = 'cpu'

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
        df = data.copy()
        id_col = df['imdb_id']

        missing_cols = set(columns) - set(df.columns)
        if missing_cols:
            raise KeyError(f"컬럼이 없습니다: {missing_cols}")

        texts = []

        if len(columns) < 2:
            texts = df[columns[0]].fillna('Unknown').tolist()
        else:
            texts = self._combine_columns(df, columns).tolist()

        embeddings =  self.model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        return pd.DataFrame({
            'imdb_id': id_col,
            'embedding': [emb.tolist() for emb in embeddings]
        })

    def fit_with_chunks(
            self,
            data,
            columns,
            output_dir = 'files/embeddings/chunks/',
            chunk_size = 10000,
        ):
        import os

        output_dir_f = f'{output_dir}/{self.type}'

        os.makedirs(output_dir_f, exist_ok=True)

        n_total = len(data)
        n_chunks = (n_total + chunk_size - 1) // chunk_size

        for chunk_idx in range(n_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, n_total)

            df_chunk = data.iloc[start_idx:end_idx].copy()
            df_chunk = self.fit(df_chunk, columns)
            output_path = f"{output_dir_f}/chunk_{chunk_idx:05d}.parquet"
            df_chunk.to_parquet(output_path, index=False)

            # GPU 메모리 정리
            if self.device == "cuda":
                torch.cuda.empty_cache()
            elif self.device == "mps":
                torch.mps.empty_cache()

    def merge_chunks(
            self,
            output_dir = 'files/embeddings/chunks/'
        ):
        import os
        import glob

        output_dir_f = f'{output_dir}/{self.type}'
        pattern = os.path.join(output_dir_f, "chunk_*.parquet")
        chunk_files = sorted(glob.glob(pattern))

        if not chunk_files:
            print("No chunk files found")
            return pd.DataFrame()

        print(f"▶ Found {len(chunk_files)} chunk files")

        dfs = []
        for path in tqdm(chunk_files, desc="Loading chunks"):
            dfs.append(pd.read_parquet(path))

        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def _combine_columns(df, columns):

        formatted_series = [
            (col.replace('_', ' ').title() + ': ' +
             df[col].fillna('Unknown').replace('', 'Unknown').astype(str))
            for col in columns
        ]

        # str.cat()으로 벡터화 결합 (매우 빠름!)
        return formatted_series[0].str.cat(formatted_series[1:], sep='. ')