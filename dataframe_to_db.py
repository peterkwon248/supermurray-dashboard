"""
DataFrame을 직접 DB에 저장하는 유틸리티
엑셀 변환 단계를 건너뛰어 속도와 정확도 향상
"""

import pandas as pd
from datetime import datetime

def save_dataframe_to_db(df: pd.DataFrame, channel: str, year_month: str, save_monthly_data_func):
    """
    DataFrame을 직접 DB에 저장
    
    Args:
        df: 저장할 DataFrame (구글 시트에서 로드한 데이터)
        channel: 채널명 ("이베이", "11번가", "B2B")
        year_month: 저장할 연월 (예: "2024-11")
        save_monthly_data_func: database.py의 save_monthly_data 함수
    
    Returns:
        bool: 성공 여부
    """
    try:
        # 1. 요약 데이터 생성
        summary = calculate_summary(df)
        
        # 2. daily_df 준비 (날짜별 데이터)
        daily_df = prepare_daily_data(df)
        
        # 3. DB에 저장
        result = save_monthly_data_func(year_month, channel, summary, daily_df)
        
        return result
        
    except Exception as e:
        print(f"DB 저장 오류: {e}")
        return False


def calculate_summary(df: pd.DataFrame) -> dict:
    """
    DataFrame에서 요약 통계 계산
    
    Returns:
        dict: {
            'total_revenue': 총 매출,
            'total_profit': 총 순이익,
            'avg_profit_rate': 평균 순이익률,
            'roas': ROAS,
            'total_ad_cost': 총 광고비
        }
    """
    summary = {}
    
    # 정산매출 찾기 (정확한 매칭 우선)
    revenue_col = None
    for col in df.columns:
        if col == "정산매출":  # 정확히 일치
            revenue_col = col
            break
    
    # 정확히 일치하는 게 없으면 부분 매칭
    if not revenue_col:
        revenue_col = find_column(df, ["정산매출", "총매출", "매출"], exclude=["전환", "광고", "센터"])
    
    if revenue_col:
        summary['total_revenue'] = float(pd.to_numeric(df[revenue_col], errors='coerce').sum())
    else:
        summary['total_revenue'] = 0.0
    
    # 순이익 찾기
    profit_col = find_column(df, ["순이익", "순손익"], exclude=["률", "율"])
    if profit_col:
        summary['total_profit'] = float(pd.to_numeric(df[profit_col], errors='coerce').sum())
    else:
        summary['total_profit'] = 0.0
    
    # 순이익률 찾기
    profit_rate_col = find_column(df, ["순이익률", "순손익률"])
    if profit_rate_col:
        summary['avg_profit_rate'] = float(pd.to_numeric(df[profit_rate_col], errors='coerce').mean())
    else:
        if summary['total_revenue'] > 0:
            summary['avg_profit_rate'] = float((summary['total_profit'] / summary['total_revenue']) * 100)
        else:
            summary['avg_profit_rate'] = 0.0
    
    # ROAS 찾기
    roas_col = find_column(df, ["ROAS", "로아스"])
    if roas_col:
        summary['roas'] = float(pd.to_numeric(df[roas_col], errors='coerce').mean())
    else:
        summary['roas'] = 0.0
    
    # 광고비 찾기
    ad_cost_col = find_column(df, ["광고비", "마케팅비", "광고 비용"])
    if ad_cost_col:
        summary['total_ad_cost'] = float(pd.to_numeric(df[ad_cost_col], errors='coerce').sum())
    else:
        summary['total_ad_cost'] = 0.0
    
    return summary


def prepare_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    날짜별 데이터 준비 (DB 저장용)
    
    Returns:
        DataFrame: 날짜별 데이터
    """
    # 날짜 컬럼이 있는지 확인
    if '날짜' not in df.columns:
        # 날짜 컬럼이 없으면 빈 DataFrame 반환
        return pd.DataFrame()
    
    # 날짜별로 그룹화하여 합계 계산
    daily_df = df.copy()
    
    # 필요한 컬럼만 선택 (존재하는 컬럼만)
    columns_to_keep = ['날짜']
    for col in df.columns:
        if col != '날짜' and pd.api.types.is_numeric_dtype(df[col]):
            columns_to_keep.append(col)
    
    daily_df = daily_df[columns_to_keep]
    
    return daily_df


def find_column(df: pd.DataFrame, keywords: list, exclude: list = None) -> str:
    """
    DataFrame에서 키워드가 포함된 컬럼 찾기
    
    Args:
        df: DataFrame
        keywords: 찾을 키워드 리스트
        exclude: 제외할 키워드 리스트
    
    Returns:
        str: 찾은 컬럼명 (없으면 None)
    """
    if exclude is None:
        exclude = []
    
    for col in df.columns:
        col_str = str(col)
        
        # 키워드가 포함되어 있는지 확인
        for keyword in keywords:
            if keyword in col_str:
                # 제외 키워드 확인
                if exclude and any(ex in col_str for ex in exclude):
                    continue
                return col
    
    return None