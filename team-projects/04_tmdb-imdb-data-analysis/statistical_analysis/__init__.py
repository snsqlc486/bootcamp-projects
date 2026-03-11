"""
statistical_analysis 패키지

흥행 점수와 범주형 변수 간의 통계적 관계를 검정하는 패키지입니다.

모듈 구성:
- assumptions: 통계 검정 전 사전 검증 (정규성 검정, 등분산성 검정)
- kruskal:     크루스칼-왈리스 검정 + 사후 검정 (비모수 ANOVA)
              "장르별로 흥행 점수에 차이가 있는가?"
- chi_square:  카이제곱 독립성 검정 + Cramér's V (범주형 변수 간 관계)
              "OTT 플랫폼과 흥행 여부 사이에 관계가 있는가?"

검정 선택 기준:
  1. pre_test()로 정규성/등분산성 확인
  2. 정규성 불만족 → 크루스칼-왈리스 검정 사용 (모수 검정 대신 비모수 검정)
  3. 범주형 × 범주형 관계 → 카이제곱 검정 사용

사용 예시:
    from statistical_analysis import pre_test, kruskal_test, chi_square_test

    pre_test(df, group_col='genre', value_col='hit_score')
    kruskal_test(df, group_col='genre', value_col='hit_score')
    chi_square_test(df, x_column='provider', y_column='is_hit')
"""

__version__ = "1.0.0"

from .assumptions import pre_test
from .kruskal import *
from .chi_square import *
