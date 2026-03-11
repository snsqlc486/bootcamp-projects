"""
BERT 기반 감성분석 모듈

주요 기능:
1. 대용량 데이터 청크 단위 처리
2. Resume 기능 (중단 시 이어하기)
3. GPU/CPU 자동 감지
4. 배치 처리로 성능 최적화
5. 로그 자동 기록
"""

import os
import re
import glob
import time
import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class SentimentAnalyzer:
    """
    BERT 기반 감성분석 클래스

    - 청크 단위 대용량 처리
    - 이미 처리된 청크 자동 스킵 (Resume)
    - CUDA 에러 발생 시 안전 종료
    """

    def __init__(
        self
    ):
        self.chunk_size = 100_000
        self.batch_size = 48
        self.max_length = 512

        if torch.cuda.is_available():
            self.device = 'cuda'
        elif torch.backends.mps.is_available():
            self.device = 'mps'
        else:
            self.device = 'cpu'

        # 모델 & 토크나이저
        self.tokenizer = None
        self.model = None
        self.softmax = torch.nn.Softmax(dim=1)

        # 로그
        self.log_path = None

    def load_model(self, model_name= "distilbert-base-uncased-finetuned-sst-2-english"):

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()

    def _log(self, msg: str):
        """로그 기록 (콘솔 + 파일)"""
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line)

        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def _get_existing_chunks(self, output_dir: str) -> set:
        """이미 처리된 청크 번호 수집"""
        existing = set()
        pattern = os.path.join(output_dir, "review_chunk_*.parquet")

        for path in glob.glob(pattern):
            match = re.search(r"chunk_(\d+)\.parquet$", path)
            if match:
                existing.add(int(match.group(1)))

        return existing

    def _predict_batch(self, texts: list) -> tuple:
        """
        배치 단위 감성 예측

        Returns:
        --------
        tuple : (labels, scores)
            labels: list of str ('positive' or 'negative')
            scores: list of float (positive 확률)
        """
        with torch.no_grad():
            # 토큰화
            enc = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt"
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}

            # 추론
            out = self.model(**enc)
            probs = self.softmax(out.logits)  # [batch, 2]
            pos_prob = probs[:, 1].detach().cpu().numpy()

            # 라벨 생성
            labels = ["positive" if p >= 0.5 else "negative" for p in pos_prob]
            scores = pos_prob.tolist()

            return labels, scores

    def _process_chunk(self, df_chunk: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """
        단일 청크 처리

        Parameters:
        -----------
        df_chunk : pd.DataFrame
            처리할 청크 데이터
        text_column : str
            텍스트가 담긴 컬럼명

        Returns:
        --------
        pd.DataFrame
            감성분석 결과 (review_id, imdb_id, content_type, sentiment_label, sentiment_score)
        """
        texts = df_chunk[text_column].astype(str).tolist()
        n = len(texts)

        all_labels = []
        all_scores = []

        # 배치 단위 처리
        for start in tqdm(range(0, n, self.batch_size), desc="Processing batches", leave=False):
            end = min(start + self.batch_size, n)
            batch_texts = texts[start:end]

            labels, scores = self._predict_batch(batch_texts)
            all_labels.extend(labels)
            all_scores.extend(scores)

        # 결과 DataFrame 생성
        result_df = pd.DataFrame({
            "review_id": df_chunk["review_id"].values,
            "imdb_id": df_chunk["imdb_id"].values,
            "sentiment_label": all_labels,
            "sentiment_score": pd.Series(all_scores, dtype="float32"),
        })

        return result_df

    def analyze(
        self,
        data: pd.DataFrame,
        output_dir: str,
        text_column: str = "review_text_clean"
    ):
        """
        감성분석 실행 (청크 단위 처리 + Resume)

        Parameters:
        -----------
        input_path : str
            입력 parquet 파일 경로
        output_dir : str
            출력 디렉토리 (청크별 parquet 저장)
        text_column : str
            분석할 텍스트 컬럼명
        """
        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        self.log_path = os.path.join(output_dir, "_step3_log.txt")

        # 모델 로드
        if self.model is None:
            self.load_model()

        # 데이터 로드
        n_total = len(data)
        n_chunks = (n_total + self.chunk_size - 1) // self.chunk_size

        print(f"▶ Total rows: {n_total:,}")
        print(f"▶ Total chunks: {n_chunks}")

        # Resume: 이미 처리된 청크 확인
        existing_chunks = self._get_existing_chunks(output_dir)
        print(f"▶ Existing chunks: {len(existing_chunks)} (will skip)")

        self._log("=== SENTIMENT ANALYSIS START ===")

        try:
            for chunk_idx in range(n_chunks):
                # 이미 처리된 청크는 스킵
                if chunk_idx in existing_chunks:
                    self._log(f"CHUNK {chunk_idx:05d} - SKIPPED (already exists)")
                    continue

                # 청크 범위 계산
                start_idx = chunk_idx * self.chunk_size
                end_idx = min(start_idx + self.chunk_size, n_total)

                # 청크 추출
                df_chunk = data.iloc[start_idx:end_idx].copy()
                n_rows = len(df_chunk)

                # 출력 파일 경로
                output_path = os.path.join(output_dir, f"review_chunk_{chunk_idx:05d}.parquet")

                self._log(f"CHUNK {chunk_idx}/{n_chunks-1} - rows={n_rows:,}, range=[{start_idx:,}:{end_idx:,}]")

                # 처리 시작
                t0 = time.time()
                result_df = self._process_chunk(df_chunk, text_column)
                elapsed = time.time() - t0

                # 결과 저장
                result_df.to_parquet(output_path, index=False)

                # 성능 로그
                rps = n_rows / elapsed if elapsed > 0 else 0
                self._log(
                    f"CHUNK {chunk_idx:05d} - DONE"
                    f"({elapsed/60:.2f} min, {rps:.1f} rows/sec) -> {output_path}"
                )

                # GPU 메모리 정리
                if self.device == "cuda":
                    torch.cuda.empty_cache()

            self._log("=== SENTIMENT ANALYSIS COMPLETED ===")
            print("\n✅ All chunks processed successfully!")

        except RuntimeError as e:
            # CUDA 에러 등 처리
            self._log(f"ERROR (RuntimeError): {repr(e)}")
            self._log("Stopped. Restart to resume from next missing chunk.")
            raise

        except Exception as e:
            self._log(f"ERROR (Unexpected): {repr(e)}")
            raise

    def merge_chunks(self, output_dir: str, merged_filename: str = "review_score.parquet"):
        """
        청크 파일들을 하나로 병합

        Parameters:
        -----------
        output_dir : str
            청크 파일들이 저장된 디렉토리
        merged_filename : str
            병합 파일명
        """
        print("▶ Merging chunks...")

        pattern = os.path.join(output_dir, "review_chunk_*.parquet")
        chunk_files = sorted(glob.glob(pattern))

        if not chunk_files:
            print("No chunk files found")
            return

        print(f"▶ Found {len(chunk_files)} chunk files")

        dfs = []
        for path in tqdm(chunk_files, desc="Loading chunks"):
            dfs.append(pd.read_parquet(path))

        merged_df = pd.concat(dfs, ignore_index=True)

        output_path = os.path.join(output_dir, merged_filename)
        merged_df.to_parquet(output_path, index=False)

        print(f"Merged {len(merged_df):,} rows -> {output_path}")

        return merged_df