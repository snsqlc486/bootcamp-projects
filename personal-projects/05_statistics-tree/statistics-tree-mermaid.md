```mermaid
flowchart TB
    Start(["통계 분석 기법 선택"]) --> DataType{"데이터 유형?"}
    DataType -- Y: 연속형<br>X: 범주형 --> Comparison["집단 비교"]
    DataType -- Y: 연속형 <br>X: 연속형 --> Relationship["관계 분석"]
    Relationship --> RelType{"분석 목적?"}
    DataType -- Y: 이항형<br>X: 연속/범주형 --> Classification["분류 분석"]
    Classification --> Logistic[["Logistic Regression"]]
    DataType -- Y: 범주형<br>X: 범주형 --> Categorical["범주형 분석"]
    DataType -- 단일 표본 --> OneSample["단일 표본 검정"]
    Comparison --> CompGroups{"집단 수?"}
    CompGroups -- 2개 --> TwoType{"독립/대응?"}
    TwoType -- 독립 --> TwoIndepNorm{"정규성?"}
    TwoIndepNorm -- YES --> TwoIndepVar{"등분산성?"}
    TwoIndepVar -- YES --> IndepT[["Independent t-test"]]
    TwoIndepVar -- NO --> WelchT@{ label: "Welch's t-test" }
    TwoIndepNorm -- NO --> MannW[["Mann-Whitney U"]]
    TwoType -- 대응 --> TwoPairedNorm{"차이값<br>정규성?"}
    TwoPairedNorm -- YES --> PairedT[["Paired t-test"]]
    TwoPairedNorm -- NO --> Wilcox[["Wilcoxon<br>Signed-Rank"]]
    CompGroups -- 3개 이상 --> MultiType{"독립/대응?"}
    MultiType -- 독립 --> MultiIndepNorm{"정규성?"}
    MultiIndepNorm -- YES --> MultiIndepVar{"등분산성?"}
    MultiIndepVar -- YES --> OneWayANOVA[["One-way ANOVA"]]
    MultiIndepVar -- NO --> WelchANOVA@{ label: "Welch's ANOVA" }
    MultiIndepNorm -- NO --> KruskalW[["Kruskal-Wallis"]]
    MultiType -- 대응 --> MultiPairedNorm{"정규성?"}
    MultiPairedNorm -- YES --> Sphericity{"구형성?"}
    Sphericity -- YES --> RMANOVA[["RM ANOVA"]]
    Sphericity -- NO --> RMANOVA_GG[["RM ANOVA<br>+ G-G 보정"]]
    MultiPairedNorm -- NO --> Friedman[["Friedman test"]]
    RelType -- 상관관계 --> CorrNorm{"정규성 &amp;<br>선형성?"}
    CorrNorm -- YES --> Pearson[["Pearson r"]]
    CorrNorm -- NO --> CorrNonParam{"데이터 특성?"}
    CorrNonParam -- 순서형 --> Spearman[["Spearman ρ"]]
    CorrNonParam -- 동점 많음 --> Kendall@{ label: "Kendall's τ" }
    RelType -- 예측/인과</br>(X: 연속/범주형 가능) --> RegType{"관계 유형?"}
    RegType -- 선형 --> RegMulticollin{"다중공선성<br>VIF &lt; 10?"}
    RegMulticollin -- YES --> LinearReg[["Linear/Multiple<br>Regression (OLS)"]]
    RegMulticollin -- NO --> RegMulticollinAction[["변수 제거 또는<br>Ridge/Lasso"]]
    RegType -- 비선형 --> PolyReg[["Polynomial<br>Regression"]]
    LinearReg -.-> ResidCheck1{{"잔차 검정:<br>정규성, 등분산성, 독립성"}}
    PolyReg -.-> ResidCheck2{{"잔차 검정:<br>정규성, 등분산성, 독립성"}}
    Categorical --> CatType{"독립/대응?"}
    CatType -- 독립 --> CatIndepFreq{"기대빈도 조건?"}
    CatIndepFreq -- 모든 셀 ≥ 5 --> ChiSq[["(독립성 검정) Chi-square test<br>(비율 비교) Two-sample proportion z-test"]]
    CatIndepFreq -- 20% 이상 셀 &lt; 5<br>또는 어느 셀 &lt; 1 --> Fisher@{ label: "Fisher's Exact" }
    CatType -- 대응 --> CatPairedSize{"교차표<br>크기?"}
    CatPairedSize -- 2×2 --> McNemar[["McNemar test"]]
    CatPairedSize -- k×k --> McNemarBowker[["McNemar-Bowker"]]
    CatPairedSize -- 이분형<br>3회 이상 반복측정 --> Cochran@{ label: "Cochran's Q" }
    OneSample --> OS_Type{"데이터 유형?"}
    OS_Type -- 연속형 --> OS_ContNorm{"정규성?"}
    OS_ContNorm -- YES --> OneT[["(평균 비교) One-sample t-test<br>(분산 비교) Chi-square Test for One Variance"]]
    OS_ContNorm -- NO --> OneWilcox[["One-sample <br>Wilcoxon Signed-Rank"]]
    OS_Type -- 이항형 --> OneProp[["(비율 비교) One-sample <br>proportion z-test"]]
    OS_Type -- 범주형 --> GOF[["(적합도 검정) <br>Chi-square test (GOF)"]]
    OneWayANOVA -. 유의한 차이 .-> PostHoc1{{"Tukey HSD"}}
    WelchANOVA -. 유의한 차이 .-> PostHoc2{{"Games-Howell"}}
    KruskalW -. 유의한 차이 .-> PostHoc3{{"Dunn test"}}
    RMANOVA -. 유의한 차이 .-> PostHoc4{{"Bonferroni"}}
    RMANOVA_GG -. 유의한 차이 .-> PostHoc5{{"Bonferroni"}}
    Friedman -. 유의한 차이 .-> PostHoc6{{"Wilcoxon signed-rank<br>+ Bonferroni"}}
    ChiSq -. 유의한 차이 .-> ResidualAnal{{"잔차 분석"}}
    McNemarBowker -. 유의한 차이 .-> PostHoc7{{"쌍별 McNemar"}}
    Cochran -. 유의한 차이 .-> PostHoc8{{"쌍별 McNemar"}}

    WelchT@{ shape: subroutine}
    WelchANOVA@{ shape: subroutine}
    Kendall@{ shape: subroutine}
    Fisher@{ shape: subroutine}
    Cochran@{ shape: subroutine}
     Start:::startStyle
     DataType:::decisionStyle
     Comparison:::categoryStyle
     Relationship:::categoryStyle
     RelType:::decisionStyle
     Classification:::categoryStyle
     Logistic:::paramTest
     Categorical:::categoryStyle
     OneSample:::categoryStyle
     CompGroups:::decisionStyle
     TwoType:::decisionStyle
     TwoIndepNorm:::decisionStyle
     TwoIndepVar:::decisionStyle
     IndepT:::paramTest
     WelchT:::paramTest
     MannW:::nonParamTest
     TwoPairedNorm:::decisionStyle
     PairedT:::paramTest
     Wilcox:::nonParamTest
     MultiType:::decisionStyle
     MultiIndepNorm:::decisionStyle
     MultiIndepVar:::decisionStyle
     OneWayANOVA:::paramTest
     WelchANOVA:::paramTest
     KruskalW:::nonParamTest
     MultiPairedNorm:::decisionStyle
     Sphericity:::decisionStyle
     RMANOVA:::paramTest
     RMANOVA_GG:::paramTest
     Friedman:::nonParamTest
     CorrNorm:::decisionStyle
     Pearson:::paramTest
     CorrNonParam:::decisionStyle
     Spearman:::nonParamTest
     Kendall:::nonParamTest
     RegType:::decisionStyle
     RegMulticollin:::decisionStyle
     LinearReg:::paramTest
     RegMulticollinAction:::warningStyle
     PolyReg:::paramTest
     ResidCheck1:::postHocStyle
     ResidCheck2:::postHocStyle
     CatType:::decisionStyle
     CatIndepFreq:::decisionStyle
     ChiSq:::paramTest
     Fisher:::nonParamTest
     CatPairedSize:::decisionStyle
     McNemar:::nonParamTest
     McNemarBowker:::nonParamTest
     Cochran:::nonParamTest
     OS_Type:::decisionStyle
     OneT:::paramTest
     OneWilcox:::nonParamTest
     OneProp:::paramTest
     GOF:::nonParamTest
     PostHoc1:::postHocStyle
     PostHoc2:::postHocStyle
     PostHoc3:::postHocStyle
     PostHoc4:::postHocStyle
     PostHoc5:::postHocStyle
     PostHoc6:::postHocStyle
     ResidualAnal:::postHocStyle
     PostHoc7:::postHocStyle
     PostHoc8:::postHocStyle
    classDef startStyle fill:#e74c3c,stroke:#c0392b,stroke-width:3px,color:#fff,font-weight:bold
    classDef categoryStyle fill:#3498db,stroke:#2980b9,stroke-width:3px,color:#fff,font-weight:bold
    classDef decisionStyle fill:#f39c12,stroke:#e67e22,stroke-width:2px,color:#000,font-weight:bold
    classDef paramTest fill:#27ae60,stroke:#229954,stroke-width:2px,color:#fff,font-weight:bold
    classDef nonParamTest fill:#00b8d4,stroke:#0097a7,stroke-width:2px,color:#fff,font-weight:bold
    classDef postHocStyle fill:#9b59b6,stroke:#8e44ad,stroke-width:1px,color:#fff,font-style:italic
    classDef warningStyle fill:#e67e22,stroke:#d35400,stroke-width:2px,color:#fff,font-weight:bold
    style OS_ContNorm fill:#f39c12,stroke:#e67e22,stroke-width:2px,color:#000
    