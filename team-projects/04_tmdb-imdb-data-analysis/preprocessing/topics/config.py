import pandas as pd
import numpy as np
import os
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from sklearn.cluster import AgglomerativeClustering

import warnings
warnings.filterwarnings('ignore')
os.environ['MallocStackLogging'] = '0'

load_dotenv()
OUTPUT_DIR = os.getenv("BERT_OUTPUT_DIR")
DRAMA_FILE_PATH = os.getenv("DRAMA_FILE_PATH")
MOVIE_FILE_PATH = os.getenv("MOVIE_FILE_PATH")
HIT_FILE_PATH = os.getenv("HIT_FILE_PATH")
os.makedirs(OUTPUT_DIR, exist_ok=True)

embedding_model = SentenceTransformer('Qwen/Qwen3-Embedding-0.6B')