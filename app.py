import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px

from database import init_database, save_monthly_data, get_available_months, get_monthly_summary, delete_month_data, save_archive_metadata, get_daily_details
import io
import json
import os

import numpy as np
from io import BytesIO

# ============================
# 0. 기본 설정
# ============================

SHEET_ID = "1lIiU5_agxG4PLsvMEIcGAJ6eVqHxLBBlzwxjiKX1mHE"
JSON_PATH = "supermurray-dashboard-1ee87560d47f.json"

# [신규] 아카이브 폴더 ID (Google Drive의 "supermurray 아카이브" 폴더)
# 사용법: Google Drive에서 폴더 열기 → URL에서 folders/ 뒤의 ID 복사
ARCHIVE_FOLDER_ID = "1buSvKM-TxFO6cwcHFmVuzMfAsyTp5veD"

# [기존] 일별 매출 분석 시트
SHEETS = {
    "메인 A": "메인 A",
    "메인 B": "메인 B",
    "이베이": "이베이",
    "11번가": "11번가",
    "B2B": "B2B",
}

# [신규] 상품 분석 시트
PRODUCT_SHEETS = {
    "통합_상품분석": "통합_상품분석",
    "이베이_상품분석": "이베이_상품분석",
    "11_상품분석": "11_상품분석",
    "B2B_상품분석": "B2B_상품분석",
}

# [신규] 상품 분석 시트별 안내 문구 (Disclaimer)
PRODUCT_DISCLAIMERS = {
    "통합_상품분석": """
    <p>각 사이트별 할인, 쿠폰, 옵션 수수료가 제대로 차감되지 않은 데이터입니다.<br>
    판매추이만 참고해주시기 바랍니다.</p>
    """,
    "이베이_상품분석": """
    <p>이베이의 경우 자체 할인 프로모션이 많습니다.<br>
    따라서 표기된 금액보다 덜 정산되는 경우가 많습니다.<br>
    완벽한 데이터라기보단 제품 판매 추이 확인용으로 참고 부탁드립니다.</p>
    """,
    "11_상품분석": """
    <p>11번가의 경우 일반 상품 안에 1+1 옵션이 많아서 데이터 오염이 심합니다.<br>
    따라서 판매수량이 1이어도 실제로 출고된 수량은 2 또는 3일 수 있습니다.<br>
    제조원가가 실제보다 많이 낮게 책정될 수 있으니 이 점 유의 부탁드립니다.</p>
    """,
    "B2B_상품분석": """
    <p>B2B의 경우 합배송 건이 제대로 소팅되지 않을 수 있습니다.<br>
    해당 데이터로 대략적인 판매 추이만 살펴주시기 바랍니다.</p>
    """
}

# ============================
# 1. 스타일 (토스 스타일 프리미엄 UI)
# ============================

def inject_css():
    st.markdown(
        """
        <style>
        /* 전체 배경 - 토스 스타일 그라데이션 */
        .stApp {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
            background-attachment: fixed;
            color: #f8fafc;
        }
        
        /* 스크롤바 스타일링 */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: rgba(15, 23, 42, 0.5);
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb {
            background: rgba(148, 163, 184, 0.4);
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(148, 163, 184, 0.6);
        }
        
        /* 메인 헤더 - 토스 스타일 */
        .main-title {
            font-size: 2.8rem;
            font-weight: 900;
            display: flex;
            align-items: center;
            gap: 0.8rem;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 0.5rem;
        }
        .main-title span.icon {
            font-size: 2.6rem;
            filter: drop-shadow(0 2px 8px rgba(59, 130, 246, 0.3));
        }
        .main-subtitle {
            font-size: 1rem;
            color: #94a3b8;
            margin-top: 0.5rem;
            font-weight: 400;
            letter-spacing: 0.01em;
        }
        
        /* 탭 스타일 - 토스 스타일 */
        button[role="tab"] {
            font-weight: 600 !important;
            font-size: 0.95rem !important;
            padding: 0.75rem 1.25rem !important;
            border-radius: 12px !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
            margin-right: 0.5rem !important;
        }
        button[role="tab"]:hover {
            background: rgba(59, 130, 246, 0.1) !important;
            transform: translateY(-1px);
        }
        button[role="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(99, 102, 241, 0.15) 100%) !important;
            border: 1px solid rgba(59, 130, 246, 0.3) !important;
        }
        
        /* 안내 문구 박스 - 토스 스타일 */
        .info-box {
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, rgba(99, 102, 241, 0.05) 100%);
            border-left: 3px solid #3b82f6;
            padding: 1.25rem 1.5rem;
            border-radius: 16px;
            margin-bottom: 2rem;
            font-size: 0.95rem;
            color: #cbd5e1;
            line-height: 1.7;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15), 0 0 0 1px rgba(59, 130, 246, 0.1);
            backdrop-filter: blur(10px);
        }
        .info-box p {
            margin: 0;
        }
        
        /* KPI 카드 영역 - 토스 스타일 */
        .metric-row {
            display: flex;
            gap: 1.25rem;
            flex-wrap: wrap;
            margin-bottom: 1.5rem;
        }
        .metric-card {
            flex: 1;
            min-width: 220px;
            padding: 1.5rem 1.75rem;
            border-radius: 20px;
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3), 
                        0 0 0 1px rgba(148, 163, 184, 0.1),
                        inset 0 1px 0 rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(148, 163, 184, 0.15);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            backdrop-filter: blur(20px);
            position: relative;
            overflow: hidden;
        }
        .metric-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, transparent, rgba(59, 130, 246, 0.5), transparent);
            opacity: 0;
            transition: opacity 0.4s;
        }
        .metric-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 12px 48px rgba(0, 0, 0, 0.4), 
                        0 0 0 1px rgba(59, 130, 246, 0.2),
                        inset 0 1px 0 rgba(255, 255, 255, 0.08);
            border-color: rgba(59, 130, 246, 0.3);
        }
        .metric-card:hover::before {
            opacity: 1;
        }
        .metric-label {
            font-size: 0.875rem;
            color: #94a3b8;
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.75rem;
            font-weight: 500;
            letter-spacing: 0.01em;
        }
        .metric-main {
            display: flex;
            align-items: baseline;
            gap: 0.5rem;
            margin-top: 0.5rem;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            line-height: 1.2;
        }
        .metric-unit {
            font-size: 0.875rem;
            color: #64748b;
            font-weight: 500;
        }
        .metric-delta {
            font-size: 0.875rem;
            margin-top: 0.75rem;
            color: #34d399;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }
        .metric-delta::before {
            content: '↑';
            font-size: 0.75rem;
        }
        .metric-delta.neg {
            color: #f87171;
        }
        .metric-delta.neg::before {
            content: '↓';
        }
        .metric-chip {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.7rem;
            font-weight: 600;
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(99, 102, 241, 0.15) 100%);
            color: #93c5fd;
            margin-left: 0.5rem;
            border: 1px solid rgba(59, 130, 246, 0.2);
            letter-spacing: 0.02em;
        }
        
        /* 섹션 타이틀 - 토스 스타일 */
        .section-title {
            font-size: 1.5rem;
            font-weight: 800;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 1rem;
            margin-top: 1.5rem;
            letter-spacing: -0.01em;
            color: #f1f5f9;
        }
        .section-caption {
            font-size: 0.9rem;
            color: #94a3b8;
            margin-bottom: 1.5rem;
            line-height: 1.6;
            font-weight: 400;
        }
        
        /* 작은 뱃지 - 토스 스타일 */
        .pill {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            border: 1px solid rgba(148, 163, 184, 0.3);
            color: #e2e8f0;
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
        }
        
        /* 구분선 스타일 */
        hr {
            border: none;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(148, 163, 184, 0.2), transparent);
            margin: 2rem 0;
        }
        
        /* 데이터프레임 스타일 개선 */
        .dataframe {
            border-radius: 12px;
            overflow: hidden;
        }
        
        /* 버튼 스타일 개선 */
        .stButton > button {
            border-radius: 12px;
            font-weight: 600;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.2);
        }
        
        /* Expander 스타일 */
        .streamlit-expanderHeader {
            border-radius: 12px;
            font-weight: 600;
        }
        
        /* Selectbox, Radio 등 입력 요소 스타일 */
        .stSelectbox > div > div, .stRadio > div {
            border-radius: 12px;
        }

        
        /* 채널 카드 스타일 - Overview 전용 */
        .channel-card {
            padding: 1.75rem 2rem;
            border-radius: 24px;
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.9) 0%, rgba(15, 23, 42, 0.95) 100%);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3), 
                        0 0 0 1px rgba(148, 163, 184, 0.15),
                        inset 0 1px 0 rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(148, 163, 184, 0.2);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            backdrop-filter: blur(20px);
            position: relative;
            overflow: hidden;
            margin-bottom: 1.5rem;
        }
        .channel-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, transparent, currentColor, transparent);
            opacity: 0.6;
        }
        .channel-card:hover {
            transform: translateY(-6px);
            box-shadow: 0 16px 48px rgba(0, 0, 0, 0.4), 
                        0 0 0 1px currentColor,
                        inset 0 1px 0 rgba(255, 255, 255, 0.1);
        }
        .channel-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1.25rem;
        }
        .channel-name {
            font-size: 1.5rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        .channel-badges {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }
        .channel-badge {
            padding: 0.35rem 0.85rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            border: 1px solid;
        }
        .channel-badge.gold {
            background: linear-gradient(135deg, rgba(251, 191, 36, 0.2) 0%, rgba(245, 158, 11, 0.15) 100%);
            color: #fbbf24;
            border-color: rgba(251, 191, 36, 0.3);
        }
        .channel-badge.silver {
            background: linear-gradient(135deg, rgba(148, 163, 184, 0.2) 0%, rgba(100, 116, 139, 0.15) 100%);
            color: #94a3b8;
            border-color: rgba(148, 163, 184, 0.3);
        }
        .channel-badge.bronze {
            background: linear-gradient(135deg, rgba(180, 83, 9, 0.2) 0%, rgba(154, 52, 18, 0.15) 100%);
            color: #d97706;
            border-color: rgba(180, 83, 9, 0.3);
        }
        .channel-badge.excellent {
            background: linear-gradient(135deg, rgba(34, 197, 94, 0.2) 0%, rgba(22, 163, 74, 0.15) 100%);
            color: #22c55e;
            border-color: rgba(34, 197, 94, 0.3);
        }
        .channel-metrics {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.25rem;
            margin-top: 1rem;
        }
        .channel-metric {
            display: flex;
            flex-direction: column;
        }
        .channel-metric-label {
            font-size: 0.8rem;
            color: #94a3b8;
            margin-bottom: 0.5rem;
            font-weight: 500;
        }
        .channel-metric-value {
            font-size: 1.5rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .channel-metric-unit {
            font-size: 0.75rem;
            color: #64748b;
            margin-left: 0.25rem;
        }
        /* 바둑돌 스타일 작은 원형 카드 */
        .stone-card {
            width: 140px;
            height: 140px;
            border-radius: 50%;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.9) 0%, rgba(15, 23, 42, 0.95) 100%);
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4), 
                        inset 0 2px 4px rgba(255, 255, 255, 0.1),
                        inset 0 -2px 4px rgba(0, 0, 0, 0.3);
            border: 2px solid rgba(148, 163, 184, 0.2);
            transition: all 0.3s ease;
            position: relative;
            margin: 0.5rem;
        }
        .stone-card:hover {
            transform: scale(1.05);
            box-shadow: 0 6px 30px rgba(59, 130, 246, 0.3), 
                        inset 0 2px 4px rgba(255, 255, 255, 0.15);
            border-color: rgba(59, 130, 246, 0.4);
        }
        .stone-value {
            font-size: 1.8rem;
            font-weight: 700;
            color: #f1f5f9;
            line-height: 1.2;
            margin-bottom: 0.25rem;
            text-align: center;
        }
        .stone-label {
            font-size: 0.7rem;
            color: #94a3b8;
            text-align: center;
            font-weight: 500;
            line-height: 1.2;
            padding: 0 0.5rem;
        }
        .stone-unit {
            font-size: 0.65rem;
            color: #64748b;
            margin-top: 0.1rem;
        }
        .stone-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            justify-content: flex-start;
            margin: 1rem 0;
        }
        .stone-group {
            margin-bottom: 1.5rem;
        }
        .stone-group-title {
            font-size: 1rem;
            font-weight: 600;
            color: #e2e8f0;
            margin-bottom: 0.75rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid rgba(148, 163, 184, 0.2);
        }
        .stone-badge {
            position: absolute;
            top: -8px;
            right: -8px;
            font-size: 0.65rem;
            font-weight: 700;
            padding: 0.25rem 0.5rem;
            border-radius: 12px;
            border: 2px solid;
            backdrop-filter: blur(10px);
            z-index: 10;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
        }
        .stone-badge.excellent {
            background: linear-gradient(135deg, rgba(34, 197, 94, 0.3) 0%, rgba(22, 163, 74, 0.2) 100%);
            color: #22c55e;
            border-color: #22c55e;
        }
        .stone-badge.good {
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.3) 0%, rgba(37, 99, 235, 0.2) 100%);
            color: #60a5fa;
            border-color: #60a5fa;
        }
        .stone-badge.normal {
            background: linear-gradient(135deg, rgba(251, 191, 36, 0.3) 0%, rgba(245, 158, 11, 0.2) 100%);
            color: #fbbf24;
            border-color: #fbbf24;
        }
        .stone-badge.poor {
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.3) 0%, rgba(220, 38, 38, 0.2) 100%);
            color: #f87171;
            border-color: #f87171;
        }
        
        /* 인사이트 박스 */
        .insight-box {
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(99, 102, 241, 0.08) 100%);
            border-left: 4px solid #3b82f6;
            padding: 1.5rem 2rem;
            border-radius: 20px;
            margin-top: 2rem;
            font-size: 1rem;
            color: #e2e8f0;
            line-height: 1.8;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2), 0 0 0 1px rgba(59, 130, 246, 0.15);
            backdrop-filter: blur(10px);
        }
        .insight-box h3 {
            font-size: 1.1rem;
            font-weight: 700;
            margin-bottom: 1rem;
            color: #f1f5f9;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .insight-item {
            margin-bottom: 0.75rem;
            padding-left: 1.5rem;
            position: relative;
        }
        .insight-item::before {
            content: '▸';
            position: absolute;
            left: 0;
            color: #3b82f6;
            font-weight: bold;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ============================
# 2. Google Sheets 로딩 (캐싱 적용 & 배포 호환)
# ============================

@st.cache_resource
def get_gc():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    
    # [수정] 1. Railway 등 서버 환경 변수에서 JSON 키 확인
    if "GOOGLE_SHEETS_JSON" in os.environ:
        try:
            creds_dict = json.loads(os.environ["GOOGLE_SHEETS_JSON"])
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        except json.JSONDecodeError:
            st.error("환경 변수 GOOGLE_SHEETS_JSON 로딩 실패: JSON 형식이 올바르지 않습니다.")
            return None
    
    # [수정] 2. 로컬 파일 확인
    else:
        creds = Credentials.from_service_account_file(JSON_PATH, scopes=scopes)
        
    return gspread.authorize(creds)

# [신규] 아카이브 폴더에서 스프레드시트 목록 가져오기
@st.cache_data(ttl=600)  # 10분 캐싱
def get_archive_files():
    """Google Drive 아카이브 폴더에서 스프레드시트 목록을 가져옵니다."""
    try:
        from googleapiclient.discovery import build
        
        # credentials 직접 생성
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        
        if "GOOGLE_SHEETS_JSON" in os.environ:
            creds_dict = json.loads(os.environ["GOOGLE_SHEETS_JSON"])
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds = Credentials.from_service_account_file(JSON_PATH, scopes=scopes)
        
        # Drive API 서비스 생성
        drive_service = build('drive', 'v3', credentials=creds)
        
        # 폴더 내 스프레드시트 검색
        query = f"'{ARCHIVE_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, modifiedTime)",
            orderBy="name desc"
        ).execute()
        
        files = results.get('files', [])
        return files
    except Exception as e:
        st.error(f"아카이브 폴더 로딩 오류: {e}")
        return []

# [신규] 특정 스프레드시트의 시트 목록 가져오기
@st.cache_data(ttl=600)
def get_spreadsheet_sheets(spreadsheet_id: str):
    """특정 스프레드시트의 시트 목록을 가져옵니다."""
    try:
        gc = get_gc()
        if gc is None:
            return []
        
        spreadsheet = gc.open_by_key(spreadsheet_id)
        return [ws.title for ws in spreadsheet.worksheets()]
    except Exception as e:
        return []

@st.cache_data(ttl=300)  # 5분 캐싱
def load_sheet(sheet_name: str, spreadsheet_id: str = None) -> pd.DataFrame:
    """시트 데이터를 로드합니다. spreadsheet_id가 None이면 기본 SHEET_ID 사용."""
    try:
        gc = get_gc()
        if gc is None: return pd.DataFrame()
        
        # spreadsheet_id가 지정되지 않으면 기본값 사용
        target_id = spreadsheet_id if spreadsheet_id else SHEET_ID
        
        ws = gc.open_by_key(target_id).worksheet(sheet_name)
        values = ws.get_all_values()
    except Exception as e:
        # st.error(f"Google Sheet 로딩 오류: {e}") # 디버깅용
        return pd.DataFrame()

    if len(values) < 2:
        return pd.DataFrame()

    # 헤더 찾기
    header_row_idx = 1 # 기본값
    if "Model" in values[0] or "모델" in values[0]:
        header_row_idx = 0
    
    header = values[header_row_idx]
    rows = values[header_row_idx+1:]

    # 중복된 컬럼명 처리
    seen = {}
    new_header = []
    for col in header:
        if col in seen:
            seen[col] += 1
            new_header.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_header.append(col)

    df = pd.DataFrame(rows, columns=new_header)

    # "합계" 행 제거
    if not df.empty:
        if "날짜" in df.columns and str(df["날짜"].iloc[0]) == "합계":
            df = df.iloc[1:].reset_index(drop=True)
        elif str(df.iloc[0, 0]) == "합계":
             df = df.iloc[1:].reset_index(drop=True)

    # 날짜 컬럼 파싱
    if "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")

    # 숫자형 컬럼 정리
    for c in df.columns:
        if c == "날짜" or c == "Model" or c == "카테고리" or "모델" in c:
            continue
        df[c] = (
            df[c]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        df[c] = pd.to_numeric(df[c], errors="ignore")

    return df

# ============================
# 3. KPI & 분석 함수
# ============================

def calc_kpis(df: pd.DataFrame):
    total_revenue_col = None
    total_profit_col = None
    avg_profit_rate_col = None
    roas_col = None
    total_ad_cost_col = None

    # 1차 패스
    for col in df.columns:
        name = str(col)
        if total_revenue_col is None and "정산매출" in name and "합계" in name:
            if "ROAS" not in name: total_revenue_col = col
        if total_profit_col is None and "순이익" in name and "합계" in name:
            if "률" not in name and "율" not in name: total_profit_col = col
        if total_ad_cost_col is None and "총광고비" in name:
            total_ad_cost_col = col

    # 2차 패스
    for col in df.columns:
        name = str(col)
        if total_revenue_col is None and "정산매출" in name:
            if "ROAS" not in name: total_revenue_col = col
        if total_profit_col is None and "순이익" in name:
            if "률" not in name and "율" not in name: total_profit_col = col
        if avg_profit_rate_col is None and "순이익률" in name:
            if "손익" not in name and "광고비" not in name: avg_profit_rate_col = col
        if roas_col is None and ("ROAS" in name or "로아스" in name):
            if "광고센터" not in name: roas_col = col
        if total_ad_cost_col is None and "광고비" in name:
            if "총" in name or "합계" in name: total_ad_cost_col = col

    total_revenue = pd.to_numeric(df[total_revenue_col], errors="coerce").sum() if total_revenue_col else 0
    total_profit = pd.to_numeric(df[total_profit_col], errors="coerce").sum() if total_profit_col else 0

    if total_revenue > 0 and total_profit != 0:
        avg_profit_rate = (total_profit / total_revenue) * 100
    elif avg_profit_rate_col:
        avg_profit_rate = pd.to_numeric(df[avg_profit_rate_col], errors="coerce").mean()
        if avg_profit_rate < 1: avg_profit_rate = avg_profit_rate * 100
    else:
        avg_profit_rate = 0

    ad_cost_col = None
    for col in df.columns:
        name = str(col)
        if "광고비" in name and ("총" in name or "합계" in name):
            ad_cost_col = col
            break
        elif "총광고비" in name:
            ad_cost_col = col
            break
    
    if ad_cost_col and total_revenue > 0:
        total_ad_cost = pd.to_numeric(df[ad_cost_col], errors="coerce").sum()
        if total_ad_cost > 0:
            roas = total_revenue / total_ad_cost
        else:
            roas = 0
    else:
        roas = 0

    return {
        "total_revenue": total_revenue,
        "total_profit": total_profit,
        "avg_profit_rate": avg_profit_rate,
        "roas": roas,
        "total_revenue_col": total_revenue_col,
        "total_profit_col": total_profit_col,
    }

def format_delta_text(current, previous):
    if previous is None or previous == 0: return None
    rate = (current - previous) / previous * 100
    sign = "+" if rate >= 0 else ""
    return f"{sign}{rate:.1f}%"

def make_combo_chart(df, x_col, revenue_col, profit_col):
    fig = go.Figure()
    if revenue_col:
        fig.add_trace(go.Bar(x=df[x_col], y=df[revenue_col], name=str(revenue_col), opacity=0.7))
    if profit_col:
        fig.add_trace(go.Scatter(x=df[x_col], y=df[profit_col], mode="lines+markers", name=str(profit_col), line=dict(width=2.2)))
    
    # Y축 포맷 (콤마)
    fig.update_yaxes(tickformat=",")
    
    # X축 포맷 (날짜 한글화: 11월 02일 형식)
    fig.update_xaxes(
        tickformat="%m월 %d일", 
        hoverformat="%Y년 %m월 %d일"
    )
    
    fig.update_traces(hovertemplate="%{y:,.0f} 원<extra></extra>")
    fig.update_layout(template="plotly_dark", height=400, margin=dict(l=10, r=10, t=40, b=40), legend=dict(orientation="h", y=1.02, x=1, xanchor="right"))
    return fig

def make_weekday_chart(df, date_col, revenue_col):
    if date_col not in df.columns or revenue_col is None: return None
    tmp = df.dropna(subset=[date_col])
    if tmp.empty: return None
    if hasattr(tmp[date_col].dt, "day_name"):
        tmp["weekday_en"] = tmp[date_col].dt.day_name()
    else: return None
    mapper = {"Monday": "월", "Tuesday": "화", "Wednesday": "수", "Thursday": "목", "Friday": "금", "Saturday": "토", "Sunday": "일"}
    tmp["요일"] = tmp["weekday_en"].map(mapper)
    summary = tmp.groupby("요일")[revenue_col].sum().reindex(["월", "화", "수", "목", "금", "토", "일"])
    
    # 텍스트 포맷 수정 (k, m 단위 제거)
    fig = px.bar(summary, title="요일별 정산매출 합계", labels={"value": "정산매출", "요일": "요일"}, height=350)
    fig.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
    fig.update_layout(template="plotly_dark", margin=dict(l=10, r=10, t=40, b=30), xaxis_title=None, yaxis_title=None)
    fig.update_yaxes(tickformat=",")
    return fig

def get_top_bottom_days(df, date_col, revenue_col, top_n=5):
    if date_col not in df.columns or revenue_col is None: return pd.DataFrame(), pd.DataFrame()
    tmp = df[[date_col, revenue_col]].dropna()
    if tmp.empty: return pd.DataFrame(), pd.DataFrame()
    top_days = tmp.nlargest(top_n, revenue_col).copy()
    top_days['순위'] = range(1, len(top_days) + 1)
    bottom_days = tmp.nsmallest(top_n, revenue_col).copy()
    bottom_days['순위'] = range(1, len(bottom_days) + 1)
    return top_days, bottom_days

def generate_performance_analysis(df, date_col, kpi, channel_name):
    analyses = []
    if kpi['roas'] > 0:
        roas_pct = kpi['roas'] * 100
        roas_msg = "매우 우수" if roas_pct > 1000 else "우수" if roas_pct > 500 else "보통"
        analyses.append(f"📊 {channel_name} ROAS {roas_pct:,.0f}% - {roas_msg}")
    if kpi['avg_profit_rate'] > 0:
        prof_msg = "수익성 최상" if kpi['avg_profit_rate'] > 30 else "수익성 우수" if kpi['avg_profit_rate'] > 20 else "보통"
        analyses.append(f"💰 {channel_name} 순이익률 {kpi['avg_profit_rate']:.1f}% - {prof_msg}")
    return analyses


# ============================
# Overview 탭 전용 헬퍼 함수들
# ============================

def prepare_daily_trend_data(channel_data):
    """일별 트렌드 데이터 준비"""
    trend_data = {}
    for ch, data in channel_data.items():
        df = data["df"]
        if "날짜" not in df.columns or df.empty:
            continue
        
        date_col = "날짜"
        revenue_col = data["kpi"]["total_revenue_col"]
        profit_col = data["kpi"]["total_profit_col"]
        
        if revenue_col is None:
            continue
        
        # 날짜별 데이터 정리
        daily_df = df[[date_col, revenue_col]].copy()
        if profit_col:
            daily_df[profit_col] = df[profit_col]
        
        daily_df = daily_df.dropna(subset=[date_col])
        daily_df = daily_df.sort_values(date_col)
        daily_df[date_col] = pd.to_datetime(daily_df[date_col])
        
        trend_data[ch] = {
            "df": daily_df,
            "revenue_col": revenue_col,
            "profit_col": profit_col
        }
    
    return trend_data

def calculate_growth_rates(channel_data, previous_month_data=None):
    """성장률 계산 (전주, 전월, MoM, WoW)"""
    growth_data = {}
    today = datetime.now()
    week_ago = today - timedelta(days=7)
    
    for ch, data in channel_data.items():
        df = data["df"]
        if "날짜" not in df.columns or df.empty:
            continue
        
        date_col = "날짜"
        revenue_col = data["kpi"]["total_revenue_col"]
        if revenue_col is None:
            continue
        
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        
        # 전주 대비 (최근 7일 vs 그 전 7일)
        recent_7d = df[df[date_col] >= week_ago][revenue_col].sum()
        prev_7d = df[(df[date_col] >= week_ago - timedelta(days=7)) & (df[date_col] < week_ago)][revenue_col].sum()
        wow_growth = ((recent_7d - prev_7d) / prev_7d * 100) if prev_7d > 0 else 0
        
        # 전일 대비
        if len(df) > 0:
            latest_date = df[date_col].max()
            latest_revenue = df[df[date_col] == latest_date][revenue_col].sum()
            prev_date = latest_date - timedelta(days=1)
            prev_revenue = df[df[date_col] == prev_date][revenue_col].sum() if len(df[df[date_col] == prev_date]) > 0 else 0
            day_over_day = ((latest_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0
        else:
            day_over_day = 0
        
        # 전월 대비 (아카이빙 데이터 활용)
        mom_growth = None
        if previous_month_data and ch in previous_month_data:
            prev_month_revenue = previous_month_data[ch].get("revenue", 0)
            current_revenue = data["revenue"]
            mom_growth = ((current_revenue - prev_month_revenue) / prev_month_revenue * 100) if prev_month_revenue > 0 else 0
        
        growth_data[ch] = {
            "wow": wow_growth,
            "day_over_day": day_over_day,
            "mom": mom_growth,
            "recent_7d": recent_7d,
            "prev_7d": prev_7d
        }
    
    return growth_data

def calculate_efficiency_metrics(channel_data):
    """효율성 지표 계산 (ROI, 광고비 효율성, 단위 광고비당 매출)"""
    efficiency_data = {}
    
    for ch, data in channel_data.items():
        revenue = data["revenue"]
        profit = data["profit"]
        roas = data["roas"]
        
        # 광고비 계산
        ad_cost = (revenue / roas) if roas > 0 else 0
        
        # ROI (투자 대비 수익률)
        roi = ((profit - ad_cost) / ad_cost * 100) if ad_cost > 0 else 0
        
        # 단위 광고비당 매출
        revenue_per_ad_cost = (revenue / ad_cost) if ad_cost > 0 else 0
        
        efficiency_data[ch] = {
            "roi": roi,
            "ad_cost": ad_cost,
            "revenue_per_ad_cost": revenue_per_ad_cost,
            "efficiency_score": roi * 0.5 + roas * 0.3 + (revenue_per_ad_cost / 10) * 0.2  # 종합 효율성 점수
        }
    
    return efficiency_data

def calculate_volatility_metrics(channel_data):
    """변동성 지표 계산 (표준편차, 변동계수)"""
    volatility_data = {}
    
    for ch, data in channel_data.items():
        df = data["df"]
        if "날짜" not in df.columns or df.empty:
            continue
        
        date_col = "날짜"
        revenue_col = data["kpi"]["total_revenue_col"]
        if revenue_col is None:
            continue
        
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col, revenue_col])
        
        if len(df) == 0:
            continue
        
        revenues = df[revenue_col].values
        mean_revenue = np.mean(revenues)
        std_revenue = np.std(revenues)
        cv = (std_revenue / mean_revenue * 100) if mean_revenue > 0 else 0  # 변동계수
        
        # 최고일 vs 평균
        max_revenue = np.max(revenues)
        avg_revenue = mean_revenue
        max_vs_avg = ((max_revenue - avg_revenue) / avg_revenue * 100) if avg_revenue > 0 else 0
        
        volatility_data[ch] = {
            "std": std_revenue,
            "cv": cv,
            "max_revenue": max_revenue,
            "avg_revenue": avg_revenue,
            "max_vs_avg": max_vs_avg
        }
    
    return volatility_data

def apply_date_filter(df, date_col, period_filter):
    """날짜 필터 적용 헬퍼 함수"""
    if date_col not in df.columns:
        return df
    
    # 날짜 컬럼이 datetime이 아니면 변환
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    
    # 날짜가 없는 행 제거
    df = df.dropna(subset=[date_col])
    
    if len(df) == 0:
        return df
    
    if isinstance(period_filter, dict):
        filter_type = period_filter.get("type", "전체")
        
        if filter_type == "전체":
            return df
        
        # 오늘 날짜 (시간 제거, 날짜만)
        today = datetime.now().date()
        
        if filter_type == "최근 7일":
            cutoff_date = today - timedelta(days=7)
            # 날짜만 비교 (시간 무시)
            filtered = df[df[date_col].dt.date >= cutoff_date]
            return filtered
        elif filter_type == "최근 30일":
            cutoff_date = today - timedelta(days=30)
            filtered = df[df[date_col].dt.date >= cutoff_date]
            return filtered
        elif filter_type == "custom":
            start_date = period_filter.get("start")
            end_date = period_filter.get("end")
            if start_date and end_date:
                # Timestamp를 date로 변환하여 비교
                if isinstance(start_date, pd.Timestamp):
                    start_date = start_date.date()
                elif hasattr(start_date, 'date'):
                    start_date = start_date.date()
                if isinstance(end_date, pd.Timestamp):
                    end_date = end_date.date()
                elif hasattr(end_date, 'date'):
                    end_date = end_date.date()
                filtered = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)]
                return filtered
    else:
        # 이전 형식 호환성
        today = datetime.now().date()
        if period_filter == "최근 7일":
            cutoff_date = today - timedelta(days=7)
            return df[df[date_col].dt.date >= cutoff_date]
        elif period_filter == "최근 30일":
            cutoff_date = today - timedelta(days=30)
            return df[df[date_col].dt.date >= cutoff_date]
    
    return df

def create_enhanced_trend_chart(trend_data, selected_channels, period_filter="전체"):
    """향상된 시계열 트렌드 차트 생성"""
    fig = go.Figure()
    
    channel_colors = {
        "이베이": "#3b82f6",
        "11번가": "#10b981",
        "B2B": "#8b5cf6"
    }
    
    for ch in selected_channels:
        if ch not in trend_data:
            continue
        
        data = trend_data[ch]
        df = data["df"].copy()
        
        # 기간 필터 적용
        df = apply_date_filter(df, "날짜", period_filter)
        
        if len(df) == 0:
            continue
        
        color = channel_colors.get(ch, "#64748b")
        
        # 매출 라인
        fig.add_trace(go.Scatter(
            x=df["날짜"],
            y=df[data["revenue_col"]],
            mode="lines+markers",
            name=f"{ch} 매출",
            line=dict(color=color, width=3),
            marker=dict(size=6, color=color),
            hovertemplate=f"<b>{ch}</b><br>날짜: %{{x|%Y-%m-%d}}<br>매출: %{{y:,.0f}}원<extra></extra>",
            legendgroup=ch
        ))
        
        # 순이익 라인 (있는 경우)
        if data["profit_col"] and data["profit_col"] in df.columns:
            fig.add_trace(go.Scatter(
                x=df["날짜"],
                y=df[data["profit_col"]],
                mode="lines+markers",
                name=f"{ch} 순이익",
                line=dict(color=color, width=2, dash="dash"),
                marker=dict(size=5, color=color, symbol="diamond"),
                hovertemplate=f"<b>{ch}</b><br>날짜: %{{x|%Y-%m-%d}}<br>순이익: %{{y:,.0f}}원<extra></extra>",
                legendgroup=ch,
                yaxis="y2"
            ))
        
        # 이동평균선 추가 (7일)
        if len(df) >= 7:
            df_sorted = df.sort_values("날짜")
            df_sorted["ma7"] = df_sorted[data["revenue_col"]].rolling(window=7, min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=df_sorted["날짜"],
                y=df_sorted["ma7"],
                mode="lines",
                name=f"{ch} 7일 평균",
                line=dict(color=color, width=1.5, dash="dot"),
                opacity=0.6,
                hovertemplate=f"<b>{ch} 7일 평균</b><br>날짜: %{{x|%Y-%m-%d}}<br>평균: %{{y:,.0f}}원<extra></extra>",
                legendgroup=ch,
                showlegend=False
            ))
    
    # 전일 대비 증감률 계산 및 annotation으로 표시
    if len(fig.data) > 0:
        # 첫 번째 채널의 데이터로 날짜와 증감률 계산
        first_ch = selected_channels[0] if selected_channels else None
        if first_ch and first_ch in trend_data:
            first_data = trend_data[first_ch]
            first_df = first_data["df"].copy()
            first_df = apply_date_filter(first_df, "날짜", period_filter)
            if len(first_df) > 1:
                first_df = first_df.sort_values("날짜")
                revenue_col = first_data["revenue_col"]
                
                # 전일 대비 증감률 계산
                first_df["prev_revenue"] = first_df[revenue_col].shift(1)
                first_df["change_pct"] = ((first_df[revenue_col] - first_df["prev_revenue"]) / first_df["prev_revenue"] * 100).fillna(0)
                
                # 최소값을 기준으로 annotation 위치 계산
                min_revenue = first_df[revenue_col].min()
                annotation_y = min_revenue * 0.95  # 차트 하단 근처
                
                # 각 날짜에 증감률 annotation 추가
                for _, row in first_df.iterrows():
                    change_pct = row["change_pct"]
                    if pd.notna(change_pct) and change_pct != 0:
                        color = "#22c55e" if change_pct > 0 else "#ef4444"
                        fig.add_annotation(
                            x=row["날짜"],
                            y=annotation_y,
                            text=f"{change_pct:+.1f}%",
                            showarrow=False,
                            font=dict(size=10, color=color),
                            bgcolor="rgba(15, 23, 42, 0.8)",
                            bordercolor=color,
                            borderwidth=1,
                            borderpad=2,
                            yshift=-25
                        )
    
    fig.update_layout(
        template="plotly_dark",
        height=500,
        margin=dict(l=10, r=10, t=50, b=60),
        xaxis_title="날짜",
        yaxis_title="매출 (원)",
        yaxis2=dict(title="순이익 (원)", overlaying="y", side="right"),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            tickformat="%m월 %d일"
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            tickformat=","
        )
    )
    
    return fig

# ============================
# 4. Streamlit 레이아웃
# ============================

st.set_page_config(page_title="머레이 통합 대시보드", page_icon="📊", layout="wide")
inject_css()

current_month = datetime.now().strftime("%Y년 %m월")

# active_sheet_id는 사이드바에서 설정되므로 일단 기본값 설정
if 'selected_data_source' not in st.session_state:
    st.session_state.selected_data_source = "🔴 실시간 (현재 월)"

# 헤더 - 토스 스타일
st.markdown(
    f"""
    <div style="margin-bottom: 2.5rem;">
        <div class="main-title">
            <span class="icon">📊</span>
            <span>머레이 통합 대시보드</span>
        </div>
        <div class="main-subtitle">
            Google Sheets 연동 · 현재 월: {current_month}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# ============================
# 4-1. 데이터 소스 선택 (사이드바)
# ============================
with st.sidebar:
    st.markdown("### 📂 데이터 소스 선택")
    
    # 아카이브 파일 목록 가져오기
    archive_files = get_archive_files()
    
    # 데이터 소스 옵션 생성
    data_source_options = ["🔴 실시간 (현재 월)"]
    archive_file_map = {}  # 이름 -> ID 매핑
    
    for file in archive_files:
        display_name = f"📁 {file['name']}"
        data_source_options.append(display_name)
        archive_file_map[display_name] = file['id']
    
    # 선택된 데이터 소스
    if 'selected_data_source' not in st.session_state:
        st.session_state.selected_data_source = data_source_options[0]
    
    selected_source = st.selectbox(
        "조회할 데이터 선택",
        data_source_options,
        index=data_source_options.index(st.session_state.selected_data_source) if st.session_state.selected_data_source in data_source_options else 0,
        key="data_source_select"
    )
    st.session_state.selected_data_source = selected_source
    
    # 선택된 소스 정보 표시
    if selected_source == "🔴 실시간 (현재 월)":
        active_sheet_id = SHEET_ID
        st.success("✅ 실시간 데이터 연동 중")
        data_source_label = f"실시간 · {current_month}"
    else:
        active_sheet_id = archive_file_map.get(selected_source, SHEET_ID)
        # 파일 정보 표시
        for file in archive_files:
            if file['id'] == active_sheet_id:
                modified_time = file.get('modifiedTime', '')
                if modified_time:
                    from datetime import datetime as dt
                    try:
                        mod_dt = dt.fromisoformat(modified_time.replace('Z', '+00:00'))
                        st.info(f"📅 마지막 수정: {mod_dt.strftime('%Y-%m-%d %H:%M')}")
                    except:
                        pass
                break
        data_source_label = selected_source.replace("📁 ", "")
    
    # 캐시 새로고침 버튼
    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    # 안내 문구
    with st.expander("ℹ️ 아카이브 사용 안내"):
        st.markdown("""
        **아카이브 파일이란?**
        - Google Drive의 `supermurray 아카이브` 폴더에 저장된 과거 데이터입니다.
        - `2025-11`: 2025년 11월 데이터
        - `통합 데이터`: 전체 기간 통합 데이터
        
        **사용 방법**
        1. 드롭다운에서 원하는 데이터 선택
        2. 대시보드가 자동으로 해당 데이터로 갱신됨
        3. 실시간으로 돌아가려면 "🔴 실시간 (현재 월)" 선택
        """)
    
    st.markdown("---")

# 시트 로딩 & KPI 계산 (선택된 데이터 소스 사용)
sheet_dfs = {}
sheet_kpis = {}
for label, sheet_name in SHEETS.items():
    try:
        df = load_sheet(sheet_name, active_sheet_id)
        sheet_dfs[label] = df
        sheet_kpis[label] = calc_kpis(df) if not df.empty else None
    except Exception as e:
        sheet_dfs[label] = pd.DataFrame()
        sheet_kpis[label] = None

# 탭 구성
tab_labels = ["Overview"] + list(SHEETS.keys()) + list(PRODUCT_SHEETS.keys()) + ["📊 월별 비교", "📊 월별 비교 (상세)"]
tabs = st.tabs(tab_labels)

# ============================
# 5. Overview 탭 (대폭 개선)
# ============================
with tabs[0]:
    st.markdown("""<div class="section-title"><span>📊 채널 전체 Overview</span></div>""", unsafe_allow_html=True)
    channels_for_overview = ["이베이", "11번가", "B2B"]
    
    # 채널별 데이터 수집
    channel_data = {}
    for ch in channels_for_overview:
        df = sheet_dfs.get(ch)
        kpi = sheet_kpis.get(ch)
        if df is not None and kpi is not None:
            channel_data[ch] = {
                "df": df,
                "kpi": kpi,
                "revenue": kpi["total_revenue"],
                "profit": kpi["total_profit"],
                "profit_rate": kpi["avg_profit_rate"],
                "roas": kpi["roas"],
            }
    
    if channel_data:
        # ========== 필터 및 비교 모드 (사이드바) - 먼저 렌더링 ==========
        # session_state 초기화
        if 'period_filter_applied' not in st.session_state:
            st.session_state.period_filter_applied = {"type": "전체"}
        if 'compare_mode_applied' not in st.session_state:
            st.session_state.compare_mode_applied = False
        if 'compare_channel1_applied' not in st.session_state:
            st.session_state.compare_channel1_applied = None
        if 'compare_channel2_applied' not in st.session_state:
            st.session_state.compare_channel2_applied = None
        
        with st.sidebar:
            available_channels = list(channel_data.keys())
            selected_channels = available_channels  # 모든 채널 표시 (필터 제거)
            
            # 기간 필터 (Expander로 접기 가능)
            with st.expander("📅 기간 필터", expanded=True):
                period_filter_type = st.radio(
                    "필터 유형",
                    ["전체", "최근 7일", "최근 30일", "수동 선택"],
                    index=["전체", "최근 7일", "최근 30일", "수동 선택"].index(st.session_state.period_filter_applied.get("type", "전체")) if st.session_state.period_filter_applied.get("type", "전체") in ["전체", "최근 7일", "최근 30일", "수동 선택"] else 0,
                    label_visibility="visible",
                    key="period_filter_radio"
                )
                
                # 수동 날짜 선택
                start_date = None
                end_date = None
                if period_filter_type == "수동 선택":
                    # 이전에 설정된 날짜가 있으면 사용
                    prev_start = st.session_state.period_filter_applied.get("start")
                    prev_end = st.session_state.period_filter_applied.get("end")
                    
                    col_date1, col_date2 = st.columns(2)
                    with col_date1:
                        start_date = st.date_input(
                            "시작일",
                            value=prev_start.date() if prev_start and hasattr(prev_start, 'date') else (datetime.now().date() - timedelta(days=30)),
                            key="start_date_input"
                        )
                    with col_date2:
                        end_date = st.date_input(
                            "종료일",
                            value=prev_end.date() if prev_end and hasattr(prev_end, 'date') else datetime.now().date(),
                            key="end_date_input"
                        )
                    
                    if start_date > end_date:
                        st.error("⚠️ 시작일이 종료일보다 늦을 수 없습니다.")
                        start_date = None
                        end_date = None
                
                # 적용 버튼
                if st.button("✅ 필터 적용", type="primary", use_container_width=True, key="apply_filter_btn"):
                    if period_filter_type == "수동 선택" and start_date and end_date:
                        st.session_state.period_filter_applied = {
                            "type": "custom",
                            "start": pd.Timestamp(start_date),
                            "end": pd.Timestamp(end_date)
                        }
                    else:
                        st.session_state.period_filter_applied = {"type": period_filter_type}
                    st.success("✅ 필터가 적용되었습니다!")
                    st.rerun()
            
            # 현재 적용된 필터 표시
            if st.session_state.period_filter_applied.get("type") != "전체":
                filter_type = st.session_state.period_filter_applied.get("type", "전체")
                if filter_type == "custom":
                    start = st.session_state.period_filter_applied.get("start")
                    end = st.session_state.period_filter_applied.get("end")
                    if start and end:
                        st.info(f"📅 적용 중: {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")
                elif filter_type == "최근 7일":
                    today = datetime.now().date()
                    start_date = today - timedelta(days=7)
                    st.info(f"📅 적용 중: {start_date.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')} (최근 7일)")
                elif filter_type == "최근 30일":
                    today = datetime.now().date()
                    start_date = today - timedelta(days=30)
                    st.info(f"📅 적용 중: {start_date.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')} (최근 30일)")
                else:
                    st.info(f"📅 적용 중: {filter_type}")
            
            
            # 데이터 내보내기
            with st.expander("💾 데이터 내보내기", expanded=False):
                # Excel 다운로드
                try:
                    overview_df_for_export = pd.DataFrame([
                        {
            "채널": ch,
                            "총 정산매출": data["revenue"],
                            "총 순이익": data["profit"],
                            "평균 순이익률(%)": data["profit_rate"],
                            "ROAS": data["roas"] * 100,
                        }
                        for ch, data in channel_data.items()
                    ])
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        overview_df_for_export.to_excel(writer, index=False, sheet_name='Overview')
                    excel_buffer.seek(0)
                    st.download_button(
                        label="📥 Excel 다운로드",
                        data=excel_buffer,
                        file_name=f"overview_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"Excel 생성 오류: {e}")
                
                # CSV 다운로드
                try:
                    csv_buffer = overview_df_for_export.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 CSV 다운로드",
                        data=csv_buffer,
                        file_name=f"overview_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"CSV 생성 오류: {e}")
        
        # 적용된 필터 가져오기 (사이드바 렌더링 후)
        period_filter = st.session_state.period_filter_applied
        
        # 필터된 기간의 데이터로 KPI 재계산
        def calculate_filtered_kpis(channel_data, period_filter):
            """필터된 기간의 KPI 계산"""
            filtered_revenue = 0
            filtered_profit = 0
            filtered_ad_cost = 0
            
            for ch, data in channel_data.items():
                df = data["df"].copy()
                if "날짜" not in df.columns or df.empty:
                    # 날짜가 없으면 전체 데이터 사용
                    filtered_revenue += data["revenue"]
                    filtered_profit += data["profit"]
                    filtered_ad_cost += (data["revenue"] / data["roas"] if data["roas"] > 0 else 0)
                    continue
                
                # 날짜 필터 적용
                if "날짜" in df.columns:
                    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
                    df = df.dropna(subset=["날짜"])
                    df_filtered = apply_date_filter(df, "날짜", period_filter)
                else:
                    # 날짜 컬럼이 없으면 전체 데이터 사용
                    df_filtered = df
                
                if len(df_filtered) == 0:
                    continue
                
                # 필터된 기간의 매출/순이익 계산
                revenue_col = data["kpi"]["total_revenue_col"]
                profit_col = data["kpi"]["total_profit_col"]
                
                if revenue_col and revenue_col in df_filtered.columns:
                    revenue_sum = df_filtered[revenue_col].sum()
                    if pd.notna(revenue_sum):
                        filtered_revenue += revenue_sum
                
                if profit_col and profit_col in df_filtered.columns:
                    profit_sum = df_filtered[profit_col].sum()
                    if pd.notna(profit_sum):
                        filtered_profit += profit_sum
                
                # 광고비 계산 (필터된 기간 기준)
                ad_cost_col = None
                for col in df_filtered.columns:
                    col_name = str(col)
                    if "광고비" in col_name and ("총" in col_name or "합계" in col_name):
                        ad_cost_col = col
                        break
                
                if ad_cost_col and ad_cost_col in df_filtered.columns:
                    filtered_ad_cost += df_filtered[ad_cost_col].sum()
                else:
                    # 광고비 컬럼이 없으면 ROAS로 역산
                    if data["roas"] > 0:
                        ch_revenue = df_filtered[revenue_col].sum() if revenue_col and revenue_col in df_filtered.columns else 0
                        filtered_ad_cost += (ch_revenue / data["roas"] if data["roas"] > 0 else 0)
            
            filtered_roas = (filtered_revenue / filtered_ad_cost * 100) if filtered_ad_cost > 0 else 0
            filtered_profit_rate = (filtered_profit / filtered_revenue * 100) if filtered_revenue > 0 else 0
            
            return {
                "revenue": filtered_revenue,
                "profit": filtered_profit,
                "ad_cost": filtered_ad_cost,
                "roas": filtered_roas,
                "profit_rate": filtered_profit_rate
            }
        
        # 필터 적용 여부 확인
        filter_type = period_filter.get("type", "전체") if isinstance(period_filter, dict) else "전체"
        
        if filter_type == "전체":
            # 필터가 "전체"면 전체 데이터 사용
            total_revenue = sum(d["revenue"] for d in channel_data.values())
            total_profit = sum(d["profit"] for d in channel_data.values())
            total_ad_cost = sum(d["revenue"] / d["roas"] if d["roas"] > 0 else 0 for d in channel_data.values())
            overall_roas = (total_revenue / total_ad_cost * 100) if total_ad_cost > 0 else 0
            weighted_profit_rate = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        else:
            # 필터가 적용된 경우 필터된 데이터로 계산
            filtered_kpis = calculate_filtered_kpis(channel_data, period_filter)
            total_revenue = filtered_kpis["revenue"]
            total_profit = filtered_kpis["profit"]
            total_ad_cost = filtered_kpis["ad_cost"]
            overall_roas = filtered_kpis["roas"]
            weighted_profit_rate = filtered_kpis["profit_rate"]
            
            # 필터된 기간에 데이터가 없으면 전체 데이터로 폴백 (경고 표시)
            if total_revenue == 0 and total_profit == 0:
                total_revenue = sum(d["revenue"] for d in channel_data.values())
                total_profit = sum(d["profit"] for d in channel_data.values())
                total_ad_cost = sum(d["revenue"] / d["roas"] if d["roas"] > 0 else 0 for d in channel_data.values())
                overall_roas = (total_revenue / total_ad_cost * 100) if total_ad_cost > 0 else 0
                weighted_profit_rate = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        
        # ========== 1. 전체 KPI 카드 섹션 ==========
        
        st.markdown("""<div class="section-title" style="margin-top: 0;"><span>📈 전체 성과 요약</span></div>""", unsafe_allow_html=True)
        st.markdown('<div class="metric-row">', unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">
                <span>총 정산매출</span>
                <span class="metric-chip">ALL CHANNELS</span>
            </div>
            <div class="metric-main">
                <span class="metric-value">{total_revenue:,.0f}</span>
                <span class="metric-unit">원</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">
                <span>총 순이익</span>
                <span class="metric-chip">ALL CHANNELS</span>
            </div>
            <div class="metric-main">
                <span class="metric-value">{total_profit:,.0f}</span>
                <span class="metric-unit">원</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">
                <span>가중 평균 순이익률</span>
                <span class="metric-chip">WEIGHTED AVG</span>
            </div>
            <div class="metric-main">
                <span class="metric-value">{weighted_profit_rate:.2f}</span>
                <span class="metric-unit">%</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">
                <span>전체 ROAS</span>
                <span class="metric-chip">OVERALL</span>
            </div>
            <div class="metric-main">
                <span class="metric-value">{overall_roas:,.0f}</span>
                <span class="metric-unit">%</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("---")
        
        # 필터된 채널별 데이터 계산 함수 정의
        def calculate_filtered_channel_data(channel_data, period_filter):
            """필터된 기간의 채널별 데이터 계산"""
            filtered_channel_data = {}
            filter_type = period_filter.get("type", "전체") if isinstance(period_filter, dict) else "전체"
            
            for ch, data in channel_data.items():
                if filter_type == "전체":
                    # 필터가 "전체"면 전체 데이터 사용
                    filtered_channel_data[ch] = {
                        "revenue": data["revenue"],
                        "profit": data["profit"],
                        "profit_rate": data["profit_rate"],
                        "roas": data["roas"]
                    }
                else:
                    # 필터 적용
                    df = data["df"].copy()
                    if "날짜" not in df.columns or df.empty:
                        # 날짜가 없으면 전체 데이터 사용
                        filtered_channel_data[ch] = {
                            "revenue": data["revenue"],
                            "profit": data["profit"],
                            "profit_rate": data["profit_rate"],
                            "roas": data["roas"]
                        }
                        continue
                    
                    # 날짜 필터 적용
                    if "날짜" in df.columns:
                        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
                        df = df.dropna(subset=["날짜"])
                        df_filtered = apply_date_filter(df, "날짜", period_filter)
                    else:
                        df_filtered = df
                    
                    if len(df_filtered) == 0:
                        # 필터된 기간에 데이터가 없으면 0으로 설정
                        filtered_channel_data[ch] = {
                            "revenue": 0,
                            "profit": 0,
                            "profit_rate": 0,
                            "roas": 0
                        }
                        continue
                    
                    # 필터된 기간의 매출/순이익 계산
                    revenue_col = data["kpi"]["total_revenue_col"]
                    profit_col = data["kpi"]["total_profit_col"]
                    
                    filtered_revenue = 0
                    filtered_profit = 0
                    
                    if revenue_col and revenue_col in df_filtered.columns:
                        revenue_sum = df_filtered[revenue_col].sum()
                        if pd.notna(revenue_sum):
                            filtered_revenue = revenue_sum
                    
                    if profit_col and profit_col in df_filtered.columns:
                        profit_sum = df_filtered[profit_col].sum()
                        if pd.notna(profit_sum):
                            filtered_profit = profit_sum
                    
                    # 순이익률 계산
                    filtered_profit_rate = (filtered_profit / filtered_revenue * 100) if filtered_revenue > 0 else 0
                    
                    # ROAS 계산
                    ad_cost_col = None
                    for col in df_filtered.columns:
                        col_name = str(col)
                        if "광고비" in col_name and ("총" in col_name or "합계" in col_name):
                            ad_cost_col = col
                            break
                    
                    if ad_cost_col and ad_cost_col in df_filtered.columns:
                        filtered_ad_cost = df_filtered[ad_cost_col].sum()
                        filtered_roas = (filtered_revenue / filtered_ad_cost * 100) if filtered_ad_cost > 0 else 0
                    else:
                        # 광고비 컬럼이 없으면 기존 ROAS 비율 사용
                        if data["roas"] > 0 and data["revenue"] > 0:
                            filtered_roas = (filtered_revenue / data["revenue"]) * data["roas"] * 100
                        else:
                            filtered_roas = 0
                    
                    filtered_channel_data[ch] = {
                        "revenue": filtered_revenue,
                        "profit": filtered_profit,
                        "profit_rate": filtered_profit_rate,
                        "roas": filtered_roas / 100 if filtered_roas > 0 else 0
                    }
            
            return filtered_channel_data
        
        # 필터된 채널별 데이터 계산
        filtered_channel_data_for_display = calculate_filtered_channel_data(channel_data, period_filter)
        
        # ========== 2. 채널별 개별 카드 ==========
        st.markdown("""<div class="section-title"><span>🏢 채널별 상세 성과</span></div>""", unsafe_allow_html=True)
        
        # 순위 계산 (필터된 데이터 사용)
        revenue_ranking = sorted(filtered_channel_data_for_display.items(), key=lambda x: x[1]["revenue"], reverse=True)
        profit_ranking = sorted(filtered_channel_data_for_display.items(), key=lambda x: x[1]["profit"], reverse=True)
        profit_rate_ranking = sorted(filtered_channel_data_for_display.items(), key=lambda x: x[1]["profit_rate"], reverse=True)
        roas_ranking = sorted(filtered_channel_data_for_display.items(), key=lambda x: x[1]["roas"], reverse=True)
        
        # 채널별 색상 및 아이콘
        channel_styles = {
            "이베이": {"icon": "🛒", "color": "#3b82f6", "gradient": "linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(99, 102, 241, 0.1) 100%)"},
            "11번가": {"icon": "🛍️", "color": "#10b981", "gradient": "linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.1) 100%)"},
            "B2B": {"icon": "🏢", "color": "#8b5cf6", "gradient": "linear-gradient(135deg, rgba(139, 92, 246, 0.15) 0%, rgba(124, 58, 237, 0.1) 100%)"},
        }
        
        # 채널별 카드 렌더링 (필터된 데이터 사용)
        for ch, data in filtered_channel_data_for_display.items():
            style = channel_styles.get(ch, {"icon": "📊", "color": "#64748b", "gradient": ""})
            
            # 배지 생성
            badges = []
            rev_rank = next(i for i, (name, _) in enumerate(revenue_ranking) if name == ch) + 1
            prof_rank = next(i for i, (name, _) in enumerate(profit_ranking) if name == ch) + 1
            rate_rank = next(i for i, (name, _) in enumerate(profit_rate_ranking) if name == ch) + 1
            roas_rank = next(i for i, (name, _) in enumerate(roas_ranking) if name == ch) + 1
            
            if rev_rank == 1:
                badges.append('<span class="channel-badge gold">🥇 최고 매출</span>')
            elif rev_rank == 2:
                badges.append('<span class="channel-badge silver">🥈 매출 2위</span>')
            elif rev_rank == 3:
                badges.append('<span class="channel-badge bronze">🥉 매출 3위</span>')
            
            if prof_rank == 1:
                badges.append('<span class="channel-badge gold">💰 최고 순이익</span>')
            
            if rate_rank == 1:
                badges.append('<span class="channel-badge excellent">📈 최고 수익성</span>')
            
            if data["roas"] > 1000:
                badges.append('<span class="channel-badge excellent">⭐ ROAS 우수</span>')
            elif data["roas"] > 500:
                badges.append('<span class="channel-badge">📊 ROAS 양호</span>')
            
            badges_html = "".join(badges)
            
            st.markdown(f"""
            <div class="channel-card" style="border-color: {style['color']}40; background: {style['gradient']};">
                <div class="channel-header">
                    <div class="channel-name" style="color: {style['color']};">
                        <span>{style['icon']}</span>
                        <span>{ch}</span>
                    </div>
                    <div class="channel-badges">
                        {badges_html}
                    </div>
                </div>
                <div class="channel-metrics">
                    <div class="channel-metric">
                        <div class="channel-metric-label">총 정산매출</div>
                        <div class="channel-metric-value">{data['revenue']:,.0f}<span class="channel-metric-unit">원</span></div>
                    </div>
                    <div class="channel-metric">
                        <div class="channel-metric-label">총 순이익</div>
                        <div class="channel-metric-value">{data['profit']:,.0f}<span class="channel-metric-unit">원</span></div>
                    </div>
                    <div class="channel-metric">
                        <div class="channel-metric-label">평균 순이익률</div>
                        <div class="channel-metric-value">{data['profit_rate']:.2f}<span class="channel-metric-unit">%</span></div>
                    </div>
                    <div class="channel-metric">
                        <div class="channel-metric-label">ROAS</div>
                        <div class="channel-metric-value">{data['roas']*100:,.0f}<span class="channel-metric-unit">%</span></div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # overview_df 미리 생성 (필터 적용)
        overview_df = pd.DataFrame([
            {
                "채널": ch,
                "총 정산매출": data["revenue"],
                "총 순이익": data["profit"],
                "평균 순이익률(%)": data["profit_rate"],
                "ROAS": data["roas"] * 100,
            }
            for ch, data in filtered_channel_data_for_display.items()
        ])
        
        # ========== 필터 적용 (모든 채널 사용) ==========
        filtered_channel_data = channel_data
        
        # 적용된 필터 가져오기
        period_filter = st.session_state.period_filter_applied
        compare_mode = st.session_state.compare_mode_applied
        compare_channel1 = st.session_state.compare_channel1_applied
        compare_channel2 = st.session_state.compare_channel2_applied
        
        # ========== 채널 간 비교 모드 (상단으로 이동) ==========
        if compare_mode and compare_channel1 and compare_channel2:
            st.markdown("""<div class="section-title" style="margin-top: 0;"><span>🔄 채널 간 비교 분석</span></div>""", unsafe_allow_html=True)
            
            ch1_data = channel_data.get(compare_channel1)
            ch2_data = channel_data.get(compare_channel2)
            
            if ch1_data and ch2_data:
                comparison_metrics = pd.DataFrame({
                    "지표": ["총 정산매출", "총 순이익", "평균 순이익률(%)", "ROAS(%)"],
                    compare_channel1: [
                        ch1_data["revenue"],
                        ch1_data["profit"],
                        ch1_data["profit_rate"],
                        ch1_data["roas"] * 100
                    ],
                    compare_channel2: [
                        ch2_data["revenue"],
                        ch2_data["profit"],
                        ch2_data["profit_rate"],
                        ch2_data["roas"] * 100
                    ]
                })
                
                col_comp1, col_comp2 = st.columns(2)
                
                with col_comp1:
                    fig_comp = go.Figure()
                    fig_comp.add_trace(go.Bar(
                        name=compare_channel1,
                        x=comparison_metrics["지표"],
                        y=comparison_metrics[compare_channel1],
                        marker_color="#3b82f6"
                    ))
                    fig_comp.add_trace(go.Bar(
                        name=compare_channel2,
                        x=comparison_metrics["지표"],
                        y=comparison_metrics[compare_channel2],
                        marker_color="#10b981"
                    ))
                    fig_comp.update_layout(
                        title="채널 간 지표 비교",
                        template="plotly_dark",
                        barmode="group",
                        height=400,
                        yaxis=dict(tickformat=","),
                        xaxis_title="지표",
                        yaxis_title="값"
                    )
                    st.plotly_chart(fig_comp, use_container_width=True, key="channel_comparison")
                
                with col_comp2:
                    st.markdown("### 📊 비교 지표 상세")
                    st.dataframe(
                        comparison_metrics.style.format({
                            compare_channel1: "{:,.2f}",
                            compare_channel2: "{:,.2f}"
                        }),
                        use_container_width=True,
                        height=400
                    )
                
                # 차이 계산 및 표시
                st.markdown("### 📈 차이 분석")
                diff_data = []
                for idx, row in comparison_metrics.iterrows():
                    val1 = row[compare_channel1]
                    val2 = row[compare_channel2]
                    diff = val1 - val2
                    diff_pct = (diff / val2 * 100) if val2 != 0 else 0
                    diff_data.append({
                        "지표": row["지표"],
                        "차이": diff,
                        "차이율 (%)": diff_pct,
                        "우위": compare_channel1 if diff > 0 else compare_channel2
                    })
                
                diff_df = pd.DataFrame(diff_data)
                
                # matplotlib이 있으면 background_gradient 사용, 없으면 텍스트 색상으로 표시
                try:
                    import matplotlib  # type: ignore
                    styled_df = diff_df.style.format({
                        "차이": "{:,.2f}",
                        "차이율 (%)": "{:+.2f}%"
                    }).background_gradient(subset=["차이율 (%)"], cmap="RdYlGn", vmin=-50, vmax=50)
                except (ImportError, AttributeError):
                    # matplotlib이 없으면 텍스트 색상으로 표시
                    def color_diff(val):
                        if isinstance(val, (int, float)):
                            if val > 0:
                                return 'color: #22c55e'  # 초록색
                            elif val < 0:
                                return 'color: #ef4444'  # 빨간색
                        return 'color: #94a3b8'  # 회색
                    
                    styled_df = diff_df.style.format({
                        "차이": "{:,.2f}",
                        "차이율 (%)": "{:+.2f}%"
                    }).applymap(color_diff, subset=["차이율 (%)"])
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=200
                )
            
            st.markdown("---")
        
        # ========== 3. 향상된 차트 섹션 (인터랙티브) ==========
        st.markdown("""<div class="section-title"><span>📊 시각화 분석 (인터랙티브)</span></div>""", unsafe_allow_html=True)
        
        col_chart1, col_chart2 = st.columns([1.5, 1])
        
        with col_chart1:
            # 향상된 바 차트 (애니메이션, 상세 정보)
            fig_bar = px.bar(
                overview_df, 
                x="채널", 
                y=["총 정산매출", "총 순이익"], 
                barmode="group", 
                title="채널별 매출 / 순이익 비교",
                color_discrete_map={"총 정산매출": "#3b82f6", "총 순이익": "#10b981"},
                height=450,
                animation_frame=None
            )
            fig_bar.update_yaxes(tickformat=",")

            fig_bar.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>채널: %{x}<br>금액: %{y:,.0f}원<extra></extra>",
                texttemplate="%{y:,.0f}",
                textposition="outside",
                marker_line_width=2,
                marker_line_color="rgba(255,255,255,0.2)"
            )
            fig_bar.update_layout(
                template="plotly_dark", 
                margin=dict(l=10, r=10, t=60, b=40), 
                xaxis_title=None, 
                yaxis_title=None, 
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                hovermode='x unified',
                # 인터랙티브 기능 활성화
                dragmode='zoom',
                modebar_add=['zoom', 'pan', 'select', 'lasso', 'zoomin', 'zoomout', 'autoscale', 'reset']
            )
            # 클릭 이벤트를 위한 커스텀 설정
            fig_bar.update_xaxes(showspikes=True, spikecolor="rgba(59, 130, 246, 0.5)", spikesnap="cursor", spikemode="across")
            fig_bar.update_yaxes(showspikes=True, spikecolor="rgba(59, 130, 246, 0.5)", spikesnap="cursor", spikemode="across")
            st.plotly_chart(fig_bar, use_container_width=True, key="overview_bar", config={
                'displayModeBar': True,
                'displaylogo': False,
                'modeBarButtonsToAdd': ['drawline', 'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape']
            })
        
        with col_chart2:
            # 향상된 도넛 차트
            fig_pie = px.pie(
                overview_df, 
                names="채널", 
                values="총 정산매출", 
                title="채널별 매출 비중",
                hole=0.50, 
                height=450,
                color_discrete_map={
                    "이베이": "#3b82f6",
                    "11번가": "#10b981",
                    "B2B": "#8b5cf6"
                }
            )
            fig_pie.update_traces(
                textinfo="percent+label+value", 
                textfont_size=13, 
                textposition="inside",
                hovertemplate="<b>%{label}</b><br>매출: %{value:,.0f}원<br>비중: %{percent}<br>전체 대비: %{percent}<extra></extra>",
                marker=dict(line=dict(color='rgba(255,255,255,0.3)', width=2)),
                pull=[0.05, 0.05, 0.05]  # 약간의 분리 효과
            )
            fig_pie.update_layout(
                template="plotly_dark", 
                margin=dict(l=20, r=20, t=60, b=20), 
                showlegend=True, 
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                hovermode='closest'
            )
            st.plotly_chart(fig_pie, use_container_width=True, key="overview_pie", config={
                'displayModeBar': True,
                'displaylogo': False
            })
        
        # 추가 차트: 효율성 비교
        st.markdown("---")
        col_chart3, col_chart4 = st.columns(2)
        
        with col_chart3:
            # ROAS 비교 차트
            fig_roas = px.bar(
                overview_df,
                x="채널",
                y="ROAS",
                title="채널별 ROAS 비교",
                color="ROAS",
                color_continuous_scale="Viridis",
                height=420,
                text="ROAS"
            )
            fig_roas.update_traces(
                texttemplate='%{y:,.0f}%',
                textposition='outside',
                hovertemplate="<b>%{x}</b><br>ROAS: %{y:,.0f}%<extra></extra>"
            )
            # Y축 범위 조정하여 상단 여유 공간 확보
            max_roas = overview_df["ROAS"].max() if not overview_df.empty else 1000
            yaxis_range = [0, max_roas * 1.15]  # 상단에 15% 여유 공간
            
            fig_roas.update_layout(
                template="plotly_dark",
                margin=dict(l=10, r=10, t=80, b=50),
                yaxis_title="ROAS (%)",
                yaxis=dict(range=yaxis_range),
                showlegend=False,
                height=420
            )
            st.plotly_chart(fig_roas, use_container_width=True, key="roas_chart")
        
        with col_chart4:
            # 순이익률 비교 차트
            fig_profit_rate = px.bar(
                overview_df,
                x="채널",
                y="평균 순이익률(%)",
                title="채널별 순이익률 비교",
                color="평균 순이익률(%)",
                color_continuous_scale="Blues",
                height=420,
                text="평균 순이익률(%)"
            )
            fig_profit_rate.update_traces(
                texttemplate='%{y:.2f}%',
                textposition='outside',
                hovertemplate="<b>%{x}</b><br>순이익률: %{y:.2f}%<extra></extra>"
            )
            # Y축 범위 조정하여 상단 여유 공간 확보
            max_profit_rate = overview_df["평균 순이익률(%)"].max() if not overview_df.empty else 50
            yaxis_range = [0, max_profit_rate * 1.15]  # 상단에 15% 여유 공간
            
            fig_profit_rate.update_layout(
                template="plotly_dark",
                margin=dict(l=10, r=10, t=80, b=50),
                yaxis_title="순이익률 (%)",
                yaxis=dict(range=yaxis_range),
                showlegend=False,
                height=420
            )
            st.plotly_chart(fig_profit_rate, use_container_width=True, key="profit_rate_chart")
        
        # ========== 시계열 트렌드 분석 ==========
        st.markdown("""<div class="section-title"><span>📈 시계열 트렌드 분석</span></div>""", unsafe_allow_html=True)
        
        trend_data = prepare_daily_trend_data(filtered_channel_data)
        
        if trend_data:
            # 일별 매출/순이익 추이와 기간별 평균 비교
            col_trend1, col_trend2 = st.columns([2, 1])
            
            with col_trend1:
                st.markdown("### 📊 일별 매출/순이익 추이")
                # 차트 생성 및 표시
                fig_trend = create_enhanced_trend_chart(trend_data, selected_channels, period_filter)
                if fig_trend and len(fig_trend.data) > 0:
                    st.plotly_chart(fig_trend, use_container_width=True, key="trend_chart")
                else:
                    filter_type = period_filter.get("type", "전체") if isinstance(period_filter, dict) else "전체"
                    if filter_type == "전체":
                        st.info("📊 전체 기간 데이터를 표시합니다. 필터를 선택하면 해당 기간만 표시됩니다.")
                    else:
                        st.warning(f"⚠️ 선택한 기간({filter_type})에 데이터가 없습니다.")
            
            # 최근 7일/30일 평균 vs 현재 비교
            with col_trend2:
                st.markdown("### 📊 기간별 평균 비교")
                avg_comparison_data = []
                for ch in selected_channels:
                    if ch not in trend_data:
                        continue
                    df = trend_data[ch]["df"].copy()
                    revenue_col = trend_data[ch]["revenue_col"]
                    
                    if len(df) == 0:
                        continue
                    
                    # 기간 필터 적용된 데이터
                    filtered_df = apply_date_filter(df, "날짜", period_filter)
                    filtered_avg = filtered_df[revenue_col].mean() if len(filtered_df) > 0 else 0
                    
                    # 비교용: 최근 7일, 30일, 전체
                    today = datetime.now()
                    recent_7d = df[df["날짜"] >= today - timedelta(days=7)][revenue_col].mean() if len(df[df["날짜"] >= today - timedelta(days=7)]) > 0 else 0
                    recent_30d = df[df["날짜"] >= today - timedelta(days=30)][revenue_col].mean() if len(df[df["날짜"] >= today - timedelta(days=30)]) > 0 else 0
                    overall_avg = df[revenue_col].mean()
                    
                    # 필터 적용된 평균을 "선택 기간 평균"으로 표시
                    period_label = "선택 기간 평균"
                    if isinstance(period_filter, dict):
                        filter_type = period_filter.get("type", "전체")
                        if filter_type == "최근 7일":
                            period_label = "최근 7일 평균"
                        elif filter_type == "최근 30일":
                            period_label = "최근 30일 평균"
                        elif filter_type == "custom":
                            period_label = "선택 기간 평균"
                        else:
                            period_label = "전체 평균"
                    
                    avg_comparison_data.append({
                        "채널": ch,
                        period_label: filtered_avg,
                        "최근 7일 평균": recent_7d,
                        "최근 30일 평균": recent_30d,
                        "전체 평균": overall_avg
                    })
                
                if avg_comparison_data:
                    avg_df = pd.DataFrame(avg_comparison_data)
                    # 동적으로 컬럼 선택 (period_label이 포함된 컬럼들)
                    y_columns = [col for col in avg_df.columns if col != "채널"]
                    color_map = {
                        "선택 기간 평균": "#f59e0b",
                        "최근 7일 평균": "#3b82f6",
                        "최근 30일 평균": "#10b981",
                        "전체 평균": "#8b5cf6"
                    }
                    fig_avg = px.bar(
                        avg_df,
                        x="채널",
                        y=y_columns,
                        barmode="group",
                        title="기간별 평균 매출 비교",
                        color_discrete_map={k: color_map.get(k, "#64748b") for k in y_columns},
                        height=300
                    )
                    fig_avg.update_yaxes(tickformat=",")
                    fig_avg.update_layout(template="plotly_dark", margin=dict(l=10, r=10, t=40, b=30))
                    st.plotly_chart(fig_avg, use_container_width=True, key="avg_comparison")
        
        st.markdown("---")
        
        # ========== 성장률 분석 ==========
        st.markdown("""<div class="section-title"><span>📊 성장률 분석</span></div>""", unsafe_allow_html=True)
        
        # 전월 데이터 가져오기 (아카이빙)
        previous_month = (datetime.now() - timedelta(days=30)).strftime("%Y-%m")
        prev_month_data = {}
        try:
            prev_summary = get_monthly_summary(previous_month)
            if not prev_summary.empty:
                for _, row in prev_summary.iterrows():
                    prev_month_data[row['channel']] = {
                        "revenue": float(row.get('total_revenue', 0) or 0),
                        "profit": float(row.get('total_profit', 0) or 0)
                    }
        except:
            pass
        
        growth_data = calculate_growth_rates(filtered_channel_data, prev_month_data if prev_month_data else None)
        
        if growth_data:
            growth_rows = []
            for ch, data in growth_data.items():
                growth_rows.append({
                    "채널": ch,
                    "전일 대비": f"{data['day_over_day']:+.1f}%",
                    "전주 대비 (WoW)": f"{data['wow']:+.1f}%",
                    "전월 대비 (MoM)": f"{data['mom']:+.1f}%" if data['mom'] is not None else "N/A",
                    "최근 7일 매출": f"{data['recent_7d']:,.0f}원"
                })
            
            growth_df = pd.DataFrame(growth_rows)
            col_growth1, col_growth2 = st.columns([1, 1])
            
            with col_growth1:
                st.dataframe(growth_df, use_container_width=True, height=200)
            
            with col_growth2:
                # 성장률 시각화
                fig_growth = go.Figure()
                channels = list(growth_data.keys())
                wow_values = [growth_data[ch]['wow'] for ch in channels]
                mom_values = [growth_data[ch]['mom'] if growth_data[ch]['mom'] is not None else 0 for ch in channels]
                
                fig_growth.add_trace(go.Bar(name="WoW", x=channels, y=wow_values, marker_color="#3b82f6"))
                fig_growth.add_trace(go.Bar(name="MoM", x=channels, y=mom_values, marker_color="#10b981"))
                fig_growth.update_layout(
                    title="채널별 성장률",
                    template="plotly_dark",
                    barmode="group",
                    height=300,
                    yaxis=dict(title="성장률 (%)")
                )
                st.plotly_chart(fig_growth, use_container_width=True, key="growth_chart")
        
        st.markdown("---")
        
        # ========== 효율성 지표 ==========
        st.markdown("""<div class="section-title"><span>⚡ 효율성 지표</span></div>""", unsafe_allow_html=True)
        
        # 효율성 지표 해설
        with st.expander("📖 효율성 지표 해설", expanded=False):
            st.markdown("""
            <div style="background: rgba(30, 41, 59, 0.6); padding: 1.5rem; border-radius: 12px; margin-bottom: 1rem;">
                <h4 style="color: #3b82f6; margin-bottom: 1rem;">📊 지표 설명</h4>
                <ul style="line-height: 1.8; color: #cbd5e1;">
                    <li><strong style="color: #60a5fa;">ROI (%)</strong>: 투자 대비 수익률. 광고비 대비 순이익의 비율입니다. 높을수록 광고 투자 효율이 좋습니다.</li>
                    <li><strong style="color: #60a5fa;">광고비</strong>: 해당 기간 동안 지출한 총 광고비입니다.</li>
                    <li><strong style="color: #60a5fa;">단위 광고비당 매출</strong>: 1원의 광고비로 얼마나 매출을 올렸는지 나타냅니다. 예를 들어 9.35는 1원 투자 시 9.35원의 매출을 의미합니다.</li>
                    <li><strong style="color: #60a5fa;">효율성 점수</strong>: ROI와 단위 광고비당 매출을 종합한 점수입니다. 높을수록 광고 효율이 우수합니다.</li>
                </ul>
                <div style="margin-top: 1rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid #3b82f6; border-radius: 4px;">
                    <strong style="color: #60a5fa;">💡 해석 가이드</strong>
                    <p style="color: #94a3b8; margin-top: 0.5rem; margin-bottom: 0;">
                        • <strong>ROI 50% 이상</strong>: 매우 우수한 효율<br>
                        • <strong>ROI 20-50%</strong>: 양호한 효율<br>
                        • <strong>ROI 20% 미만</strong>: 개선 필요<br>
                        • <strong>단위 광고비당 매출 5 이상</strong>: 효율적인 광고 운영
                    </p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        efficiency_data = calculate_efficiency_metrics(filtered_channel_data)
        
        if efficiency_data:
            # 광고비가 0이거나 매우 작은 채널 제외 (B2B 등)
            filtered_efficiency_data = {
                ch: data for ch, data in efficiency_data.items() 
                if data.get('ad_cost', 0) > 1000  # 광고비가 1000원 이상인 경우만 표시
            }
            
            if not filtered_efficiency_data:
                st.info("📊 효율성 지표를 계산할 수 있는 채널이 없습니다. (광고비가 있는 채널만 표시됩니다)")
            else:
                efficiency_rows = []
                for ch, data in filtered_efficiency_data.items():
                    efficiency_rows.append({
                        "채널": ch,
                        "ROI (%)": data['roi'],
                        "광고비": f"{data['ad_cost']:,.0f}원",
                        "단위 광고비당 매출": f"{data['revenue_per_ad_cost']:.2f}",
                        "효율성 점수": data['efficiency_score']  # 숫자로 유지
                    })
                
                efficiency_df = pd.DataFrame(efficiency_rows)
                efficiency_df = efficiency_df.sort_values("효율성 점수", ascending=False)
                
                col_eff1, col_eff2 = st.columns([1, 1])
                
                with col_eff1:
                    # 바둑돌 스타일 카드로 표시
                    for ch, data in filtered_efficiency_data.items():
                        # 효율성 점수에 따른 뱃지 결정
                        score = data['efficiency_score']
                        if score >= 30:
                            badge_class = "excellent"
                            badge_text = "🏆 우수"
                        elif score >= 20:
                            badge_class = "good"
                            badge_text = "⭐ 양호"
                        elif score >= 10:
                            badge_class = "normal"
                            badge_text = "📊 보통"
                        else:
                            badge_class = "poor"
                            badge_text = "⚠️ 개선"
                        
                        st.markdown(f"""
                        <div class="stone-group">
                            <div class="stone-group-title">{ch}</div>
                            <div class="stone-grid">
                                <div class="stone-card">
                                    <div class="stone-value">{data['roi']:.1f}<span class="stone-unit">%</span></div>
                                    <div class="stone-label">ROI</div>
                                </div>
                                <div class="stone-card">
                                    <div class="stone-value">{data['ad_cost']/10000:.0f}<span class="stone-unit">만원</span></div>
                                    <div class="stone-label">광고비</div>
                                </div>
                                <div class="stone-card">
                                    <div class="stone-value">{data['revenue_per_ad_cost']:.2f}</div>
                                    <div class="stone-label">단위 광고비당<br>매출</div>
                                </div>
                                <div class="stone-card" style="background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(99, 102, 241, 0.15) 100%); border-color: rgba(59, 130, 246, 0.4); position: relative;">
                                    <div class="stone-badge {badge_class}">{badge_text}</div>
                                    <div class="stone-value" style="color: #60a5fa;">{data['efficiency_score']:.1f}</div>
                                    <div class="stone-label" style="color: #93c5fd;">효율성 점수</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col_eff2:
                    # 효율성 순위 차트
                    fig_eff = px.bar(
                        efficiency_df,
                        x="채널",
                        y="효율성 점수",
                        title="채널별 효율성 점수",
                        color="효율성 점수",
                        color_continuous_scale="Viridis",
                        height=450,
                        text="효율성 점수"
                    )
                    fig_eff.update_traces(
                        texttemplate='%{y:.1f}',
                        textposition='outside',
                        hovertemplate="<b>%{x}</b><br>효율성 점수: %{y:.1f}<extra></extra>"
                    )
                    # Y축 범위 조정하여 상단 여유 공간 확보
                    max_score = efficiency_df["효율성 점수"].max() if not efficiency_df.empty else 50
                    yaxis_range = [0, max_score * 1.15]  # 상단에 15% 여유 공간
                    
                    fig_eff.update_layout(
                        template="plotly_dark", 
                        margin=dict(l=10, r=10, t=80, b=50), 
                        yaxis=dict(range=yaxis_range),
                        showlegend=False
                    )
                    st.plotly_chart(fig_eff, use_container_width=True, key="efficiency_chart")
        
        st.markdown("---")
        
        # ========== 상세 분석 섹션 ==========
        st.markdown("""<div class="section-title"><span>🔍 상세 분석</span></div>""", unsafe_allow_html=True)
        
        volatility_data = calculate_volatility_metrics(filtered_channel_data)
        
        col_detail1, col_detail2 = st.columns(2)
        
        with col_detail1:
            st.markdown("### 🏆 TOP/BOTTOM 일자")
            for ch in selected_channels:
                if ch not in channel_data:
                    continue
                
                df = channel_data[ch]["df"]
                if "날짜" not in df.columns:
                    continue
                
                date_col = "날짜"
                revenue_col = channel_data[ch]["kpi"]["total_revenue_col"]
                if revenue_col is None:
                    continue
                
                top_days, bottom_days = get_top_bottom_days(df, date_col, revenue_col, top_n=3)
                
                st.markdown(f"**{ch}**")
                if not top_days.empty:
                    st.markdown("**TOP 3:**")
                    for _, row in top_days.iterrows():
                        st.markdown(f"- {row[date_col].strftime('%Y-%m-%d')}: {row[revenue_col]:,.0f}원")
                if not bottom_days.empty:
                    st.markdown("**BOTTOM 3:**")
                    for _, row in bottom_days.iterrows():
                        st.markdown(f"- {row[date_col].strftime('%Y-%m-%d')}: {row[revenue_col]:,.0f}원")
                st.markdown("---")
        
        with col_detail2:
            st.markdown("### 📊 변동성 지표")
            
            # 변동성 지표 해설
            with st.expander("📖 변동성 지표 해설", expanded=False):
                st.markdown("""
                <div style="background: rgba(30, 41, 59, 0.6); padding: 1.5rem; border-radius: 12px; margin-bottom: 1rem;">
                    <h4 style="color: #8b5cf6; margin-bottom: 1rem;">📊 지표 설명</h4>
                    <ul style="line-height: 1.8; color: #cbd5e1;">
                        <li><strong style="color: #a78bfa;">표준편차</strong>: 일별 매출의 변동 폭을 나타냅니다. 값이 클수록 매출 변동이 크다는 의미입니다.</li>
                        <li><strong style="color: #a78bfa;">변동계수 (%)</strong>: 표준편차를 평균으로 나눈 값입니다. 평균 대비 변동성을 상대적으로 비교할 수 있습니다.</li>
                        <li><strong style="color: #a78bfa;">최고일 vs 평균</strong>: 최고 매출일이 평균 매출보다 얼마나 높은지 비율로 나타냅니다.</li>
                    </ul>
                    <div style="margin-top: 1rem; padding: 1rem; background: rgba(139, 92, 246, 0.1); border-left: 4px solid #8b5cf6; border-radius: 4px;">
                        <strong style="color: #a78bfa;">💡 해석 가이드</strong>
                        <p style="color: #94a3b8; margin-top: 0.5rem; margin-bottom: 0;">
                            • <strong>변동계수 50% 미만</strong>: 안정적인 매출 패턴<br>
                            • <strong>변동계수 50-100%</strong>: 보통 수준의 변동성<br>
                            • <strong>변동계수 100% 이상</strong>: 높은 변동성 (불안정)<br>
                            • <strong>최고일 vs 평균 150% 이상</strong>: 특정일에 집중된 매출 패턴
                        </p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            if volatility_data:
                # 바둑돌 스타일 카드로 표시
                for ch, data in volatility_data.items():
                    st.markdown(f"""
                    <div class="stone-group">
                        <div class="stone-group-title">{ch}</div>
                        <div class="stone-grid">
                            <div class="stone-card">
                                <div class="stone-value">{data['std']/1000:.0f}<span class="stone-unit">K</span></div>
                                <div class="stone-label">표준편차</div>
                            </div>
                            <div class="stone-card">
                                <div class="stone-value">{data['cv']:.1f}<span class="stone-unit">%</span></div>
                                <div class="stone-label">변동계수</div>
                            </div>
                            <div class="stone-card" style="background: linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(124, 58, 237, 0.15) 100%); border-color: rgba(139, 92, 246, 0.4);">
                                <div class="stone-value" style="color: #a78bfa;">{data['max_vs_avg']:+.1f}<span class="stone-unit">%</span></div>
                                <div class="stone-label" style="color: #c4b5fd;">최고일 vs<br>평균</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # ========== 4. 인사이트 섹션 ==========
        insights = []
        
        # 매출 1위 채널
        top_revenue_ch = revenue_ranking[0][0]
        top_revenue_val = revenue_ranking[0][1]["revenue"]
        revenue_pct = (top_revenue_val / total_revenue * 100) if total_revenue > 0 else 0
        insights.append(f"<strong>{top_revenue_ch}</strong>이(가) 전체 매출의 <strong>{revenue_pct:.1f}%</strong>를 차지하며 최대 채널입니다. (매출: {top_revenue_val:,.0f}원)")
        
        # 순이익 1위 채널
        top_profit_ch = profit_ranking[0][0]
        top_profit_val = profit_ranking[0][1]["profit"]
        profit_pct = (top_profit_val / total_profit * 100) if total_profit > 0 else 0
        insights.append(f"<strong>{top_profit_ch}</strong>이(가) 전체 순이익의 <strong>{profit_pct:.1f}%</strong>를 기여하며 가장 수익성이 높습니다. (순이익: {top_profit_val:,.0f}원)")
        
        # 수익률 1위 채널
        top_rate_ch = profit_rate_ranking[0][0]
        top_rate_val = profit_rate_ranking[0][1]["profit_rate"]
        insights.append(f"<strong>{top_rate_ch}</strong>의 순이익률이 <strong>{top_rate_val:.2f}%</strong>로 가장 우수합니다.")
        
        # ROAS 분석
        top_roas_ch = roas_ranking[0][0]
        top_roas_val = roas_ranking[0][1]["roas"] * 100
        if top_roas_val > 1000:
            insights.append(f"<strong>{top_roas_ch}</strong>의 ROAS가 <strong>{top_roas_val:,.0f}%</strong>로 매우 우수한 수준입니다.")
        elif top_roas_val > 500:
            insights.append(f"<strong>{top_roas_ch}</strong>의 ROAS가 <strong>{top_roas_val:,.0f}%</strong>로 양호한 수준입니다.")
        
        # 채널별 기여도 분석
        if len(channel_data) == 3:
            insights.append(f"전체 {len(channel_data)}개 채널 중 <strong>{top_revenue_ch}</strong>이(가) 매출 1위, <strong>{top_profit_ch}</strong>이(가) 순이익 1위를 기록했습니다.")
        
        insights_html = "".join([f'<div class="insight-item">{insight}</div>' for insight in insights])
        
        st.markdown(f"""
        <div class="insight-box">
            <h3>💡 주요 인사이트</h3>
            {insights_html}
        </div>
        """, unsafe_allow_html=True)
        
    else:
        st.warning("⚠️ 채널별 데이터를 로딩하지 못했습니다. Google Sheets 연결을 확인해주세요.")

# ============================
# 6. 일별 시트 탭
# ============================
previous_kpis = {}

for idx, label in enumerate(SHEETS.keys(), start=1):
    with tabs[idx]:
        df = sheet_dfs[label]
        kpi = sheet_kpis[label]

        st.markdown(f"""<div class="section-title"><span>📌 {label} 요약</span></div>""", unsafe_allow_html=True)

        if df.empty or kpi is None:
            st.warning("데이터가 비어 있거나 시트를 읽는 데 실패했습니다.")
            continue

        prev = previous_kpis.get(label, None)
        st.markdown('<div class="metric-row">', unsafe_allow_html=True)

        delta_sales = format_delta_text(kpi["total_revenue"], prev["total_revenue"]) if prev else None
        delta_cls = "" if (delta_sales is None or (delta_sales and delta_sales.startswith("+"))) else "neg"
        st.markdown(f"""<div class="metric-card"><div class="metric-label"><span>총 정산매출</span><span class="metric-chip">MONTHLY</span></div><div class="metric-main"><span class="metric-value">{kpi['total_revenue']:,.0f}</span><span class="metric-unit">원</span></div>{f'<div class="metric-delta {delta_cls}">전월 대비 {delta_sales}</div>' if delta_sales else ''}</div>""", unsafe_allow_html=True)

        delta_profit = format_delta_text(kpi["total_profit"], prev["total_profit"]) if prev else None
        delta_cls = "" if (delta_profit is None or (delta_profit and delta_profit.startswith("+"))) else "neg"
        st.markdown(f"""<div class="metric-card"><div class="metric-label"><span>총 순이익</span><span class="metric-chip">MONTHLY</span></div><div class="metric-main"><span class="metric-value">{kpi['total_profit']:,.0f}</span><span class="metric-unit">원</span></div>{f'<div class="metric-delta {delta_cls}">전월 대비 {delta_profit}</div>' if delta_profit else ''}</div>""", unsafe_allow_html=True)

        st.markdown(f"""<div class="metric-card"><div class="metric-label"><span>평균 순이익률</span><span class="metric-chip">AVG</span></div><div class="metric-main"><span class="metric-value">{kpi['avg_profit_rate']:.2f}</span><span class="metric-unit">%</span></div></div>""", unsafe_allow_html=True)
        st.markdown(f"""<div class="metric-card"><div class="metric-label"><span>정산매출 ROAS</span><span class="metric-chip">AVG</span></div><div class="metric-main"><span class="metric-value">{kpi['roas']*100:,.0f}</span><span class="metric-unit">%</span></div></div>""", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("---")

        date_col = "날짜" if "날짜" in df.columns else df.columns[0]
        fig_combo = make_combo_chart(df, date_col, kpi["total_revenue_col"], kpi["total_profit_col"])
        if fig_combo: st.plotly_chart(fig_combo, use_container_width=True, key=f"combo_{label}")
        
        st.markdown("---")
        fig_weekday = make_weekday_chart(df, date_col, kpi["total_revenue_col"])
        if fig_weekday: st.plotly_chart(fig_weekday, use_container_width=True, key=f"weekday_{label}")
        
        st.markdown("---")
        performance_analyses = generate_performance_analysis(df, date_col, kpi, label)
        if performance_analyses:
            for analysis in performance_analyses:
                st.markdown(f"""<div style="background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%); padding: 1rem 1.5rem; border-radius: 16px; border-left: 3px solid #3b82f6; margin-bottom: 0.75rem; box-shadow: 0 4px 20px rgba(0,0,0,0.2), 0 0 0 1px rgba(59, 130, 246, 0.1); font-size: 0.95rem; color: #e2e8f0; backdrop-filter: blur(10px); transition: all 0.3s;">{analysis}</div>""", unsafe_allow_html=True)

        col_top, col_bottom = st.columns(2)
        top_days, bottom_days = get_top_bottom_days(df, date_col, kpi["total_revenue_col"], top_n=5)
        with col_top:
            st.markdown("""<div class="section-title"><span>🏆 TOP 5 매출 최고일</span></div>""", unsafe_allow_html=True)
            if not top_days.empty:
                display_top = top_days[[date_col, kpi["total_revenue_col"], "순위"]].copy()
                display_top.rename(columns={date_col: "날짜", kpi["total_revenue_col"]: "매출"}, inplace=True)
                st.dataframe(display_top.style.format({"매출": "{:,.0f}원"}), use_container_width=True, height=220)
        with col_bottom:
            st.markdown("""<div class="section-title"><span>📉 BOTTOM 5 매출 최저일</span></div>""", unsafe_allow_html=True)
            if not bottom_days.empty:
                display_bottom = bottom_days[[date_col, kpi["total_revenue_col"], "순위"]].copy()
                display_bottom.rename(columns={date_col: "날짜", kpi["total_revenue_col"]: "매출"}, inplace=True)
                st.dataframe(display_bottom.style.format({"매출": "{:,.0f}원"}), use_container_width=True, height=220)

        with st.expander("🔍 원본 데이터 표 보기"):
            st.dataframe(df, use_container_width=True)

# ============================
# 6-4. 상품 분석 탭 (안내 문구 추가됨)
# ============================
product_tab_start_idx = 1 + len(SHEETS)

for i, (label, sheet_name) in enumerate(PRODUCT_SHEETS.items()):
    with tabs[product_tab_start_idx + i]:
        st.markdown(f"""<div class="section-title">📦 {label} 대시보드</div><div class="section-caption">모델별 판매량, 매출, 순이익을 심층 분석합니다.</div>""", unsafe_allow_html=True)
        
        # [NEW] 상단 안내 문구 (Info Box) 렌더링
        if label in PRODUCT_DISCLAIMERS:
            st.markdown(f"""<div class="info-box">{PRODUCT_DISCLAIMERS[label]}</div>""", unsafe_allow_html=True)
        
        try:
            df = load_sheet(sheet_name, active_sheet_id)
        except Exception as e:
            st.error(f"시트 로딩 실패: {e}")
            continue
            
        if df.empty:
            st.warning("데이터가 없거나 로딩 실패. 시트 이름을 확인해주세요.")
            continue
            
        # 매출 컬럼 찾기
        revenue_col = next((c for c in df.columns if ("총" in str(c) or "합계" in str(c)) and ("매출" in str(c) or "정산" in str(c))), None)
        if not revenue_col:
            revenue_col = next((c for c in df.columns if "매출" in str(c) or "정산" in str(c)), None)

        # 판관비 차감 전 이익 컬럼 찾기 (총 판관비 우선) → 없으면 순이익 컬럼
        profit_col = next((c for c in df.columns if "총" in str(c) and "판관비" in str(c) and "차감" in str(c) and "이익" in str(c) and "개당" not in str(c)), None)
        if not profit_col:
            profit_col = next((c for c in df.columns if "판관비" in str(c) and "차감" in str(c) and "이익" in str(c) and "개당" not in str(c) and "이베이" not in str(c) and "11번가" not in str(c) and "B2B" not in str(c)), None)
        if not profit_col:
            profit_col = next((c for c in df.columns if "판관비" in str(c) and "차감" in str(c) and "이익" in str(c) and "개당" not in str(c)), None)
        if not profit_col:
            profit_col = next((c for c in df.columns if ("총" in str(c) or "합계" in str(c)) and "순이익" in str(c) and "률" not in str(c) and "개당" not in str(c)), None)
        if not profit_col:
            profit_col = next((c for c in df.columns if "순이익" in str(c) and "률" not in str(c) and "개당" not in str(c)), None)
        
        # 판매량 컬럼 찾기
        qty_col = next((c for c in df.columns if ("총" in str(c) or "합계" in str(c)) and ("판매" in str(c) or "수량" in str(c))), None)
        if not qty_col:
            qty_col = next((c for c in df.columns if "판매" in str(c) or "수량" in str(c)), None)

        col_map = {
            "model": next((c for c in df.columns if "Model" in str(c) or "모델" in str(c)), None),
            "category": next((c for c in df.columns if "카테고리" in str(c)), None),
            "sales_qty": qty_col,
            "revenue": revenue_col,
            "profit": profit_col
        }
        
        if not (col_map["model"] and col_map["revenue"]):
            st.error(f"필수 컬럼(Model, 매출)을 찾을 수 없습니다. 현재 컬럼: {list(df.columns)}")
            st.dataframe(df.head())
            continue
            
        # 데이터 전처리
        for c_key in ["revenue", "profit", "sales_qty"]:
            if col_map[c_key]:
                df[col_map[c_key]] = pd.to_numeric(df[col_map[c_key]], errors='coerce').fillna(0)

        # KPI 계산
        total_rev = df[col_map["revenue"]].sum()
        total_prof = df[col_map["profit"]].sum() if col_map["profit"] else 0
        total_qty = df[col_map["sales_qty"]].sum() if col_map["sales_qty"] else 0
        avg_margin = (total_prof / total_rev * 100) if total_rev > 0 else 0
        
        # 총 판매 Model 수 계산 (빈 셀 제외)
        total_models = df[col_map["model"]].replace('', pd.NA).dropna().nunique()
        
        # 상단 카드
        st.markdown('<div class="metric-row">', unsafe_allow_html=True)
        cols = st.columns(5)
        cols[0].metric("총 매출", f"{total_rev:,.0f}원")
        cols[1].metric("판관비 차감 전 이익", f"{total_prof:,.0f}원")
        cols[2].metric("총 판매 Model", f"{total_models:,}개")
        cols[3].metric("총 판매량", f"{total_qty:,.0f}개")
        cols[4].metric("평균 이익률", f"{avg_margin:.1f}%")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        c1, c2 = st.columns([1, 1])
        
        with c1:
            top10_rev = df.nlargest(10, col_map["revenue"]).sort_values(col_map["revenue"], ascending=True)
            fig_top = px.bar(top10_rev, x=col_map["revenue"], y=col_map["model"], orientation='h', title="🏆 매출 TOP 10 모델")
            fig_top.update_traces(texttemplate='%{x:,.0f} 원', textposition='outside')
            fig_top.update_xaxes(tickformat=",")
            fig_top.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig_top, use_container_width=True, key=f"top10_rev_{label}")
            
        with c2:
            if col_map["profit"]:
                top10_prof = df.nlargest(10, col_map["profit"]).sort_values(col_map["profit"], ascending=True)
                fig_prof = px.bar(top10_prof, x=col_map["profit"], y=col_map["model"], orientation='h', title="💰 순이익 TOP 10 모델", color_discrete_sequence=['#2ecc71'])
                fig_prof.update_traces(texttemplate='%{x:,.0f} 원', textposition='outside')
                fig_prof.update_xaxes(tickformat=",")
                fig_prof.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig_prof, use_container_width=True, key=f"top10_prof_{label}")

        # 카테고리 분석 그래프
        if col_map["category"]:
            st.markdown("---")
            c3, c4 = st.columns([1, 1])
            
            with c3:
                # 가장 많이 팔린 카테고리 (판매량 기준)
                if col_map["sales_qty"]:
                    cat_sales = df.groupby(col_map["category"])[col_map["sales_qty"]].sum().reset_index()
                    cat_sales = cat_sales[cat_sales[col_map["category"]] != ''].nlargest(10, col_map["sales_qty"]).sort_values(col_map["sales_qty"], ascending=True)
                    fig_cat_sales = px.bar(cat_sales, x=col_map["sales_qty"], y=col_map["category"], orientation='h', title="📦 판매량 TOP 카테고리", color_discrete_sequence=['#f59e0b'])
                    fig_cat_sales.update_traces(texttemplate='%{x:,.0f} 개', textposition='outside')
                    fig_cat_sales.update_xaxes(tickformat=",")
                    fig_cat_sales.update_layout(template="plotly_dark", height=400)
                    st.plotly_chart(fig_cat_sales, use_container_width=True, key=f"cat_sales_{label}")
            
            with c4:
                # 가장 상품 종류가 많은 카테고리 (Model 수 기준)
                cat_models = df.groupby(col_map["category"])[col_map["model"]].nunique().reset_index()
                cat_models.columns = [col_map["category"], 'model_count']
                cat_models = cat_models[cat_models[col_map["category"]] != ''].nlargest(10, 'model_count').sort_values('model_count', ascending=True)
                fig_cat_models = px.bar(cat_models, x='model_count', y=col_map["category"], orientation='h', title="🏷️ 상품 종류 TOP 카테고리", color_discrete_sequence=['#8b5cf6'])
                fig_cat_models.update_traces(texttemplate='%{x:,.0f} 종', textposition='outside')
                fig_cat_models.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig_cat_models, use_container_width=True, key=f"cat_models_{label}")

        if col_map["profit"]:
            st.markdown("### 🧩 상품 포트폴리오 분석 (매출 vs 이익)")
            fig_scat = px.scatter(
                df, x=col_map["revenue"], y=col_map["profit"], 
                hover_name=col_map["model"], size=col_map["sales_qty"] if col_map["sales_qty"] else None,
                color=col_map["category"] if col_map["category"] else None,
                title="모델별 매출 대비 순이익 분포 (원 크기: 판매량)"
            )
            fig_scat.update_xaxes(tickformat=",")
            fig_scat.update_yaxes(tickformat=",")
            fig_scat.update_traces(hovertemplate="<b>%{hovertext}</b><br>매출: %{x:,.0f}원<br>이익: %{y:,.0f}원<extra></extra>")
            fig_scat.update_layout(template="plotly_dark", height=500)
            st.plotly_chart(fig_scat, use_container_width=True, key=f"scatter_{label}")

        with st.expander("📋 전체 데이터 리스트"):
            st.dataframe(df, use_container_width=True)

# ============================
# ============================
# 7. 월별 비교 탭 (요약)
# ============================
with tabs[-2]:
    st.markdown("""<div class="section-title"><span>📊 월별 비교 분석</span></div>""", unsafe_allow_html=True)
    
    available_months = get_available_months()
    
    if len(available_months) < 1:
        st.warning("⚠️ 비교할 과거 데이터가 없습니다.")
        st.info("""
        **👉 해결 방법:**
        1. 사이드바에서 **아카이브 파일**을 선택하세요.
        2. Google Drive의 'supermurray 아카이브' 폴더에 과거 데이터 파일이 있어야 합니다.
        """)
    else:
        current_month_option = f"{current_month} (실시간)"
        all_options = [current_month_option] + available_months
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            compare_type = st.radio("비교 유형", ["단일 월 비교", "기간 비교"])
        
        if compare_type == "단일 월 비교":
            with col2: month1 = st.selectbox("기준 월 (최근)", all_options, index=0)
            with col3: 
                remaining = [m for m in all_options if m != month1]
                month2 = st.selectbox("비교 월 (과거)", remaining) if remaining else None
            
            if month2 and st.button("📊 비교 분석 실행", type="primary", use_container_width=True):
                st.markdown("---")
                
                def get_data_for_month(m_label):
                    if "실시간" in m_label:
                        return {k: v for k, v in sheet_kpis.items() if v is not None}, current_month.replace("년 ", "-").replace("월", "")
                    else:
                        df = get_monthly_summary(m_label)
                        data = {}
                        if not df.empty:
                            for _, row in df.iterrows():
                                ch = row['channel']
                                data[ch] = {
                                    'total_revenue': float(row['total_revenue'] or 0),
                                    'total_profit': float(row['total_profit'] or 0),
                                    'avg_profit_rate': float(row['avg_profit_rate'] or 0),
                                    'roas': float(row['roas'] or 0)
                                }
                        return data, m_label

                data1, label1 = get_data_for_month(month1)
                data2, label2 = get_data_for_month(month2)
                
                comp_rows = []
                target_channels = ["이베이", "11번가", "B2B"]
                
                for ch in target_channels:
                    d1 = data1.get(ch)
                    d2 = data2.get(ch)
                    
                    if d1 and d2:
                        rev_diff = (d1['total_revenue'] - d2['total_revenue'])
                        rev_pct = (rev_diff / d2['total_revenue'] * 100) if d2['total_revenue'] > 0 else 0
                        prof_diff = (d1['total_profit'] - d2['total_profit'])
                        prof_pct = (prof_diff / d2['total_profit'] * 100) if d2['total_profit'] > 0 else 0
                        
                        comp_rows.append({
                            "채널": ch,
                            f"{label1} 매출": d1['total_revenue'],
                            f"{label2} 매출": d2['total_revenue'],
                            "매출 차액": rev_diff,
                            "성장률(%)": rev_pct,
                            f"{label1} 순이익": d1['total_profit'],
                            f"{label2} 순이익": d2['total_profit'],
                            "이익 성장률(%)": prof_pct
                        })
                
                if comp_rows:
                    comp_df = pd.DataFrame(comp_rows)
                    st.markdown(f"### 🆚 {label2} 대비 {label1} 성과")
                    
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name=label1, x=comp_df['채널'], y=comp_df[f"{label1} 매출"], text=comp_df[f"{label1} 매출"], textposition='auto'))
                    fig.add_trace(go.Bar(name=label2, x=comp_df['채널'], y=comp_df[f"{label2} 매출"], text=comp_df[f"{label2} 매출"], textposition='auto'))
                    
                    fig.update_traces(texttemplate='%{y:,.0f}')
                    fig.update_layout(barmode='group', title="채널별 매출 비교", template="plotly_dark", height=400)
                    fig.update_yaxes(tickformat=",")
                    st.plotly_chart(fig, use_container_width=True, key="compare_bar")
                    

                    # matplotlib이 있으면 background_gradient 사용, 없으면 텍스트 색상으로 표시
                    try:
                        import matplotlib  # type: ignore
                        styled_comp_df = comp_df.style.format({
                            f"{label1} 매출": "{:,.0f}", f"{label2} 매출": "{:,.0f}", "매출 차액": "{:+,.0f}", "성장률(%)": "{:+.1f}%",
                            f"{label1} 순이익": "{:,.0f}", f"{label2} 순이익": "{:,.0f}", "이익 성장률(%)": "{:+.1f}%"
                        }).background_gradient(subset=["성장률(%)"], cmap="RdYlGn", vmin=-50, vmax=50)
                    except (ImportError, AttributeError):
                        # matplotlib이 없으면 텍스트 색상으로 표시
                        def color_growth(val):
                            if isinstance(val, (int, float)):
                                if val > 0:
                                    return 'color: #22c55e'  # 초록색
                                elif val < 0:
                                    return 'color: #ef4444'  # 빨간색
                            return 'color: #94a3b8'  # 회색
                        
                        styled_comp_df = comp_df.style.format({
                            f"{label1} 매출": "{:,.0f}", f"{label2} 매출": "{:,.0f}", "매출 차액": "{:+,.0f}", "성장률(%)": "{:+.1f}%",
                            f"{label1} 순이익": "{:,.0f}", f"{label2} 순이익": "{:,.0f}", "이익 성장률(%)": "{:+.1f}%"
                        }).applymap(color_growth, subset=["성장률(%)", "이익 성장률(%)"])
                    
                    st.dataframe(
                        styled_comp_df,
                        use_container_width=True
                    )
                else:
                    st.warning("비교할 수 있는 공통 채널 데이터가 없습니다.")

        else:
            with col2: selected_months = st.multiselect("분석할 월 선택 (2개 이상)", all_options, default=all_options[:min(3, len(all_options))])
            
            if len(selected_months) >= 2 and st.button("📈 트렌드 분석 실행", type="primary", use_container_width=True):
                trend_data = []
                for m_opt in selected_months:
                    if "실시간" in m_opt:
                        raw_d = sheet_kpis
                        m_label = current_month.replace("년 ", "-").replace("월", "")
                        for ch, val in raw_d.items():
                            if ch in ["이베이", "11번가", "B2B"] and val:
                                trend_data.append({"월": m_label, "채널": ch, "매출": val['total_revenue'], "순이익": val['total_profit']})
                    else:
                        df = get_monthly_summary(m_opt)
                        for _, row in df.iterrows():
                            trend_data.append({"월": m_opt, "채널": row['channel'], "매출": float(row['total_revenue']), "순이익": float(row['total_profit'])})
                
                if trend_data:
                    tdf = pd.DataFrame(trend_data).sort_values("월")
                    st.markdown("### 📈 월별 매출 추이")
                    fig_rev = px.line(tdf, x="월", y="매출", color="채널", markers=True, text="매출")
                    # X축 포맷 수정 (월별 트렌드)
                    fig_rev.update_xaxes(tickformat="%Y년 %m월")
                    fig_rev.update_traces(texttemplate="%{y:,.0f}", textposition="top center")
                    fig_rev.update_yaxes(tickformat=",")
                    fig_rev.update_layout(template="plotly_dark", height=400)
                    st.plotly_chart(fig_rev, use_container_width=True, key="trend_rev")
                    
                    st.markdown("### 💰 월별 순이익 추이")
                    fig_prof = px.line(tdf, x="월", y="순이익", color="채널", markers=True, text="순이익")
                    # X축 포맷 수정 (월별 트렌드)
                    fig_prof.update_xaxes(tickformat="%Y년 %m월")
                    fig_prof.update_traces(texttemplate="%{y:,.0f}", textposition="top center")
                    fig_prof.update_yaxes(tickformat=",")
                    fig_prof.update_layout(template="plotly_dark", height=400)
                    st.plotly_chart(fig_prof, use_container_width=True, key="trend_prof")
                else:
                    st.error("데이터를 불러오지 못했습니다.")

# ============================
# 8. 월별 비교 (상세) 탭 - Overview 스타일
# ============================
with tabs[-1]:
    st.markdown("""<div class="section-title"><span>📊 월별 비교 (상세)</span></div>""", unsafe_allow_html=True)
    
    # 아카이브 파일 목록 가져오기
    archive_files = get_archive_files()
    
    if not archive_files:
        st.warning("⚠️ 아카이브 파일이 없습니다.")
        st.info("Google Drive의 'supermurray 아카이브' 폴더에 과거 데이터 파일을 추가하세요.")
    else:
        # 현재 실시간 + 아카이브 파일 옵션
        file_options = [("🔴 실시간 (현재)", SHEET_ID, "실시간")]
        for f in archive_files:
            file_options.append((f"📁 {f['name']}", f['id'], f['name']))
        
        st.markdown("### 🆚 두 기간 선택")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📅 기간 A (기준)")
            period_a_idx = st.selectbox(
                "기간 A 선택",
                range(len(file_options)),
                format_func=lambda x: file_options[x][0],
                key="detail_period_a"
            )
            period_a_name = file_options[period_a_idx][0]
            period_a_id = file_options[period_a_idx][1]
        
        with col2:
            st.markdown("#### 📅 기간 B (비교)")
            # 기간 A와 다른 옵션만 표시
            remaining_options = [(i, opt) for i, opt in enumerate(file_options) if i != period_a_idx]
            if remaining_options:
                period_b_choice = st.selectbox(
                    "기간 B 선택",
                    range(len(remaining_options)),
                    format_func=lambda x: remaining_options[x][1][0],
                    key="detail_period_b"
                )
                period_b_idx = remaining_options[period_b_choice][0]
                period_b_name = file_options[period_b_idx][0]
                period_b_id = file_options[period_b_idx][1]
            else:
                st.warning("비교할 다른 기간이 없습니다.")
                period_b_id = None
        
        if period_b_id and st.button("📊 상세 비교 실행", type="primary", use_container_width=True, key="run_detail_compare"):
            st.markdown("---")
            
            with st.spinner("데이터 로딩 중..."):
                # 기간 A 데이터 로드
                data_a = {}
                for ch in ["이베이", "11번가", "B2B"]:
                    df = load_sheet(ch, period_a_id)
                    if not df.empty:
                        kpis = calc_kpis(df)
                        if kpis:
                            data_a[ch] = {
                                "df": df,
                                "kpis": kpis,
                                "revenue": kpis.get("total_revenue", 0),
                                "profit": kpis.get("total_profit", 0),
                                "profit_rate": kpis.get("avg_profit_rate", 0),
                                "roas": kpis.get("roas", 0),
                                "ad_cost": kpis.get("total_ad_cost", 0)
                            }
                
                # 기간 B 데이터 로드
                data_b = {}
                for ch in ["이베이", "11번가", "B2B"]:
                    df = load_sheet(ch, period_b_id)
                    if not df.empty:
                        kpis = calc_kpis(df)
                        if kpis:
                            data_b[ch] = {
                                "df": df,
                                "kpis": kpis,
                                "revenue": kpis.get("total_revenue", 0),
                                "profit": kpis.get("total_profit", 0),
                                "profit_rate": kpis.get("avg_profit_rate", 0),
                                "roas": kpis.get("roas", 0),
                                "ad_cost": kpis.get("total_ad_cost", 0)
                            }
            
            if not data_a and not data_b:
                st.error("두 기간 모두 데이터를 불러오지 못했습니다.")
            else:
                # 전체 요약 비교
                st.markdown(f"## 🏆 전체 성과 비교")
                st.markdown(f"**{period_a_name}** vs **{period_b_name}**")
                
                # 전체 합계 계산
                total_a = {"revenue": 0, "profit": 0, "ad_cost": 0}
                total_b = {"revenue": 0, "profit": 0, "ad_cost": 0}
                
                for ch_data in data_a.values():
                    total_a["revenue"] += ch_data["revenue"]
                    total_a["profit"] += ch_data["profit"]
                    total_a["ad_cost"] += ch_data["ad_cost"]
                
                for ch_data in data_b.values():
                    total_b["revenue"] += ch_data["revenue"]
                    total_b["profit"] += ch_data["profit"]
                    total_b["ad_cost"] += ch_data["ad_cost"]
                
                # 전체 KPI 카드
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    rev_diff = total_a["revenue"] - total_b["revenue"]
                    rev_pct = (rev_diff / total_b["revenue"] * 100) if total_b["revenue"] > 0 else 0
                    color = "#22c55e" if rev_diff >= 0 else "#ef4444"
                    arrow = "▲" if rev_diff >= 0 else "▼"
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center; padding: 1.5rem;">
                        <div style="color: #94a3b8; font-size: 0.9rem;">총 정산매출</div>
                        <div style="font-size: 1.8rem; font-weight: bold; margin: 0.5rem 0;">{total_a["revenue"]:,.0f}원</div>
                        <div style="color: {color}; font-size: 1rem;">{arrow} {abs(rev_pct):.1f}% ({rev_diff:+,.0f}원)</div>
                        <div style="color: #64748b; font-size: 0.8rem;">vs {total_b["revenue"]:,.0f}원</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    prof_diff = total_a["profit"] - total_b["profit"]
                    prof_pct = (prof_diff / total_b["profit"] * 100) if total_b["profit"] > 0 else 0
                    color = "#22c55e" if prof_diff >= 0 else "#ef4444"
                    arrow = "▲" if prof_diff >= 0 else "▼"
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center; padding: 1.5rem;">
                        <div style="color: #94a3b8; font-size: 0.9rem;">총 순이익</div>
                        <div style="font-size: 1.8rem; font-weight: bold; margin: 0.5rem 0;">{total_a["profit"]:,.0f}원</div>
                        <div style="color: {color}; font-size: 1rem;">{arrow} {abs(prof_pct):.1f}% ({prof_diff:+,.0f}원)</div>
                        <div style="color: #64748b; font-size: 0.8rem;">vs {total_b["profit"]:,.0f}원</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    rate_a = (total_a["profit"] / total_a["revenue"] * 100) if total_a["revenue"] > 0 else 0
                    rate_b = (total_b["profit"] / total_b["revenue"] * 100) if total_b["revenue"] > 0 else 0
                    rate_diff = rate_a - rate_b
                    color = "#22c55e" if rate_diff >= 0 else "#ef4444"
                    arrow = "▲" if rate_diff >= 0 else "▼"
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center; padding: 1.5rem;">
                        <div style="color: #94a3b8; font-size: 0.9rem;">평균 순이익률</div>
                        <div style="font-size: 1.8rem; font-weight: bold; margin: 0.5rem 0;">{rate_a:.1f}%</div>
                        <div style="color: {color}; font-size: 1rem;">{arrow} {abs(rate_diff):.1f}%p</div>
                        <div style="color: #64748b; font-size: 0.8rem;">vs {rate_b:.1f}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                
                # 채널별 상세 비교
                st.markdown("## 📊 채널별 상세 비교")
                
                for ch in ["이베이", "11번가", "B2B"]:
                    ch_a = data_a.get(ch)
                    ch_b = data_b.get(ch)
                    
                    if not ch_a and not ch_b:
                        continue
                    
                    with st.expander(f"🏪 {ch}", expanded=True):
                        col1, col2 = st.columns(2)
                        
                        # 기간 A 데이터
                        with col1:
                            st.markdown(f"#### {period_a_name}")
                            if ch_a:
                                st.markdown(f"""
                                <div class="metric-card" style="padding: 1rem;">
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">정산매출</span>
                                        <span style="font-weight: bold;">{ch_a['revenue']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">순이익</span>
                                        <span style="font-weight: bold;">{ch_a['profit']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">순이익률</span>
                                        <span style="font-weight: bold;">{ch_a['profit_rate']:.1f}%</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">광고비</span>
                                        <span style="font-weight: bold;">{ch_a['ad_cost']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between;">
                                        <span style="color: #94a3b8;">ROAS</span>
                                        <span style="font-weight: bold;">{ch_a['roas']*100:.0f}%</span>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.info("데이터 없음")
                        
                        # 기간 B 데이터
                        with col2:
                            st.markdown(f"#### {period_b_name}")
                            if ch_b:
                                st.markdown(f"""
                                <div class="metric-card" style="padding: 1rem;">
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">정산매출</span>
                                        <span style="font-weight: bold;">{ch_b['revenue']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">순이익</span>
                                        <span style="font-weight: bold;">{ch_b['profit']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">순이익률</span>
                                        <span style="font-weight: bold;">{ch_b['profit_rate']:.1f}%</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                        <span style="color: #94a3b8;">광고비</span>
                                        <span style="font-weight: bold;">{ch_b['ad_cost']:,.0f}원</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between;">
                                        <span style="color: #94a3b8;">ROAS</span>
                                        <span style="font-weight: bold;">{ch_b['roas']*100:.0f}%</span>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.info("데이터 없음")
                        
                        # 변화량 표시
                        if ch_a and ch_b:
                            st.markdown("##### 📈 변화량")
                            metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
                            
                            with metrics_col1:
                                diff = ch_a['revenue'] - ch_b['revenue']
                                pct = (diff / ch_b['revenue'] * 100) if ch_b['revenue'] > 0 else 0
                                st.metric("매출 변화", f"{diff:+,.0f}원", f"{pct:+.1f}%")
                            
                            with metrics_col2:
                                diff = ch_a['profit'] - ch_b['profit']
                                pct = (diff / ch_b['profit'] * 100) if ch_b['profit'] > 0 else 0
                                st.metric("순이익 변화", f"{diff:+,.0f}원", f"{pct:+.1f}%")
                            
                            with metrics_col3:
                                diff = ch_a['profit_rate'] - ch_b['profit_rate']
                                st.metric("이익률 변화", f"{diff:+.1f}%p", "")
                            
                            with metrics_col4:
                                diff = (ch_a['roas'] - ch_b['roas']) * 100
                                st.metric("ROAS 변화", f"{diff:+.0f}%p", "")
                
                # 비교 차트
                st.markdown("---")
                st.markdown("## 📊 시각화 비교")
                
                # 매출 비교 차트
                chart_data = []
                for ch in ["이베이", "11번가", "B2B"]:
                    if ch in data_a:
                        chart_data.append({"채널": ch, "기간": period_a_name.replace("📁 ", "").replace("🔴 ", ""), "매출": data_a[ch]["revenue"], "순이익": data_a[ch]["profit"]})
                    if ch in data_b:
                        chart_data.append({"채널": ch, "기간": period_b_name.replace("📁 ", "").replace("🔴 ", ""), "매출": data_b[ch]["revenue"], "순이익": data_b[ch]["profit"]})
                
                if chart_data:
                    chart_df = pd.DataFrame(chart_data)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        fig_rev = px.bar(chart_df, x="채널", y="매출", color="기간", barmode="group",
                                        title="채널별 매출 비교", text="매출")
                        fig_rev.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
                        fig_rev.update_layout(template="plotly_dark", height=400)
                        fig_rev.update_yaxes(tickformat=",")
                        st.plotly_chart(fig_rev, use_container_width=True, key="detail_rev_chart")
                    
                    with col2:
                        fig_prof = px.bar(chart_df, x="채널", y="순이익", color="기간", barmode="group",
                                         title="채널별 순이익 비교", text="순이익")
                        fig_prof.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
                        fig_prof.update_layout(template="plotly_dark", height=400)
                        fig_prof.update_yaxes(tickformat=",")
                        st.plotly_chart(fig_prof, use_container_width=True, key="detail_prof_chart")