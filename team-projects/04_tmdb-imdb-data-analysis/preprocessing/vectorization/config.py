import re
import spacy
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
import torch
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

HIT_FILE_PATH = 'files/final_files/00_hit_score.parquet'
MAIN_FILE_PATH = 'files/final_files/movie/00_movie_main.parquet'