"""
통계 분석 모듈 공통 설정

statistical_analysis 패키지의 모든 모듈이 공유하는 라이브러리 임포트입니다.
assumptions.py, chi_square.py, kruskal.py가 이 파일을 import합니다.
"""

import pandas as pd
from itertools import combinations  # 그룹 쌍 조합 생성 (사후 검정에서 두 그룹씩 비교할 때 사용)
import scipy.stats as stats         # 통계 검정 함수 (shapiro, levene, kruskal, chi2_contingency 등)
import matplotlib.pyplot as plt     # 그래프 그리기
import seaborn as sns               # 통계 시각화 (히트맵 등)
