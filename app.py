import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from database import init_database, save_monthly_data, get_available_months, get_monthly_summary, delete_month_data, save_archive_metadata
import io
import json
import os

# ============================
# 0. 기본 설정
# ============================

SHEET_ID = "1lIiU5_agxG4PLsvMEIcGAJ6eVqHxLBBlzwxjiKX1mHE"
JSON_PATH = "supermurray-dashboard-1ee87560d47f.json"

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
# 1. 스타일 (프리미엄 다크 UI)
# ============================

def inject_css():
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at top left, #1f2933 0, #020617 45%, #000000 100%);
            color: #e5e7eb;
        }
        /* 메인 헤더 */
        .main-title {
            font-size: 2.4rem;
            font-weight: 800;
            display: flex;
            align-items: center;
            gap: 0.65rem;
        }
        .main-title span.icon {
            font-size: 2.2rem;
        }
        .main-subtitle {
            font-size: 0.9rem;
            color: #9ca3af;
            margin-top: 0.25rem;
        }
        /* 탭 라벨 */
        button[role="tab"] {
            font-weight: 600 !important;
        }
        /* 안내 문구 박스 (Info Box) */
        .info-box {
            background: rgba(59, 130, 246, 0.1); 
            border-left: 4px solid #f59e0b; /* 노란색 포인트 */
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1.5rem;
            font-size: 0.9rem;
            color: #d1d5db;
            line-height: 1.6;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .info-box p {
            margin: 0;
        }
        /* KPI 카드 영역 */
        .metric-row {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            margin-bottom: 0.75rem;
        }
        .metric-card {
            flex: 1;
            min-width: 200px;
            padding: 1.0rem 1.2rem;
            border-radius: 0.9rem;
            background: radial-gradient(circle at top left, #1e293b, #020617);
            box-shadow: 0 18px 40px rgba(0,0,0,0.55);
            border: 1px solid rgba(148, 163, 184, 0.35);
        }
        .metric-label {
            font-size: 0.85rem;
            color: #9ca3af;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .metric-main {
            display: flex;
            align-items: baseline;
            gap: 0.4rem;
            margin-top: 0.2rem;
        }
        .metric-value {
            font-size: 1.45rem;
            font-weight: 700;
        }
        .metric-unit {
            font-size: 0.8rem;
            color: #9ca3af;
        }
        .metric-delta {
            font-size: 0.8rem;
            margin-top: 0.25rem;
            color: #6ee7b7;
        }
        .metric-delta.neg {
            color: #fca5a5;
        }
        .metric-chip {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            font-size: 0.7rem;
            background: rgba(59,130,246,0.18);
            color: #93c5fd;
            margin-left: 0.4rem;
        }
        /* 섹션 타이틀 */
        .section-title {
            font-size: 1.15rem;
            font-weight: 700;
            display: flex;
            align-items: center;
            gap: 0.4rem;
            margin-bottom: 0.2rem;
            margin-top: 0.4rem;
        }
        .section-caption {
            font-size: 0.8rem;
            color: #9ca3af;
            margin-bottom: 0.4rem;
        }
        /* 작은 뱃지 */
        .pill {
            display: inline-block;
            padding: 0.12rem 0.45rem;
            border-radius: 999px;
            font-size: 0.7rem;
            border: 1px solid rgba(148,163,184,0.6);
            color: #e5e7eb;
            background: rgba(15,23,42,0.8);
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

@st.cache_data(ttl=300)  # 5분 캐싱
def load_sheet(sheet_name: str) -> pd.DataFrame:
    try:
        gc = get_gc()
        if gc is None: return pd.DataFrame()
        
        ws = gc.open_by_key(SHEET_ID).worksheet(sheet_name)
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
# 4. Streamlit 레이아웃
# ============================

st.set_page_config(page_title="머레이 통합 대시보드", page_icon="📊", layout="wide")
inject_css()

current_month = datetime.now().strftime("%Y년 %m월")

# 헤더
st.markdown(
    f"""
    <div class="main-title">
        <span class="icon">📊</span>
        <span>머레이 통합 대시보드</span>
    </div>
    <div class="main-subtitle">
        Google Sheets 실시간 연동 · 현재 월: {current_month}
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# 시트 로딩 & KPI 계산
sheet_dfs = {}
sheet_kpis = {}
for label, sheet_name in SHEETS.items():
    try:
        df = load_sheet(sheet_name)
        sheet_dfs[label] = df
        sheet_kpis[label] = calc_kpis(df) if not df.empty else None
    except Exception as e:
        sheet_dfs[label] = pd.DataFrame()
        sheet_kpis[label] = None

# 탭 구성
tab_labels = ["Overview"] + list(SHEETS.keys()) + list(PRODUCT_SHEETS.keys()) + ["📁 아카이빙", "📊 월별 비교"]
tabs = st.tabs(tab_labels)

# ============================
# 5. Overview 탭
# ============================
with tabs[0]:
    st.markdown("""<div class="section-title"><span>📊 채널 전체 Overview</span></div>""", unsafe_allow_html=True)
    channels_for_overview = ["이베이", "11번가", "B2B"]
    rows = []
    for ch in channels_for_overview:
        df = sheet_dfs.get(ch)
        kpi = sheet_kpis.get(ch)
        if df is None or kpi is None: continue
        rows.append({
            "채널": ch,
            "총 정산매출": kpi["total_revenue"],
            "총 순이익": kpi["total_profit"],
            "평균 순이익률(%)": kpi["avg_profit_rate"],
            "ROAS": kpi["roas"],
        })

    if rows:
        overview_df = pd.DataFrame(rows)
        col_left, col_right = st.columns([2.2, 2.8])
        with col_left:
            st.dataframe(overview_df.style.format({"총 정산매출": "{:,.0f}", "총 순이익": "{:,.0f}", "평균 순이익률(%)": "{:.2f}", "ROAS": lambda x: f"{x*100:,.0f}%"}), use_container_width=True, height=260)
        with col_right:
            fig_bar = px.bar(overview_df, x="채널", y=["총 정산매출", "총 순이익"], barmode="group", title="채널별 매출 / 순이익", height=320)
            fig_bar.update_yaxes(tickformat=",")
            fig_bar.update_traces(hovertemplate="%{y:,.0f} 원<extra></extra>", texttemplate="%{y:,.0f}", textposition="outside")
            fig_bar.update_layout(template="plotly_dark", margin=dict(l=10, r=10, t=40, b=30), xaxis_title=None, yaxis_title=None, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_bar, use_container_width=True, key="overview_bar")
        
        st.markdown("<br>", unsafe_allow_html=True)
        pie_left, pie_right = st.columns([1.4, 1.6])
        with pie_left:
            fig_pie = px.pie(overview_df, names="채널", values="총 정산매출", title="채널별 매출 비중", hole=0.50, height=380)
            fig_pie.update_traces(textinfo="percent", textfont_size=18, textposition="inside", hovertemplate="%{label}: %{value:,.0f}원 (%{percent})<extra></extra>")
            fig_pie.update_layout(template="plotly_dark", margin=dict(l=30, r=30, t=40, b=40), showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5))
            st.plotly_chart(fig_pie, use_container_width=True, key="overview_pie")
    else:
        st.info("채널별 데이터를 로딩하지 못했습니다.")

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
                st.markdown(f"""<div style="background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%); padding: 0.8rem 1.2rem; border-radius: 0.75rem; border-left: 4px solid #3b82f6; margin-bottom: 0.6rem; box-shadow: 0 4px 12px rgba(0,0,0,0.3); font-size: 0.95rem;">{analysis}</div>""", unsafe_allow_html=True)

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
            df = load_sheet(sheet_name)
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
# 7. 아카이빙 탭 (엑셀 업로드 제거)
# ============================
with tabs[-2]:
    st.markdown("""<div class="section-title"><span>📁 과거 데이터 아카이빙</span></div>""", unsafe_allow_html=True)
    try:
        init_database()
    except Exception as e:
        st.error(f"DB 초기화 실패: {e}")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🔄 구글 시트에서 자동 저장")
        col_month, col_btn = st.columns([1, 1])
        with col_month:
            now = datetime.now()
            year = st.selectbox("연도", range(2024, 2100), index=now.year - 2024, key="archive_year")
            month = st.selectbox("월", range(1, 13), index=now.month - 1, key="archive_month")
            selected_year_month = f"{year}-{month:02d}"
        with col_btn:
            st.write("")
            st.write("")
            if st.button("💾 DB에 저장", type="primary", use_container_width=True):
                with st.spinner("저장 중..."):
                    try:
                        from dataframe_to_db import save_dataframe_to_db
                        success_count = 0
                        channels_saved = []
                        channels_to_save = ["이베이", "11번가", "B2B"]
                        selected_year = int(year)
                        selected_month = int(month)
                        for channel in channels_to_save:
                            if channel in sheet_dfs and not sheet_dfs[channel].empty:
                                df = sheet_dfs[channel].copy()
                                if '날짜' in df.columns:
                                    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
                                    df = df[df['날짜'].notna()].copy()
                                    df_filtered = df[(df['날짜'].dt.year == selected_year) & (df['날짜'].dt.month == selected_month)].copy()
                                    if df_filtered.empty: continue
                                    df_to_save = df_filtered
                                else:
                                    df_to_save = df
                                if save_dataframe_to_db(df_to_save, channel, selected_year_month, save_monthly_data):
                                    success_count += 1
                                    channels_saved.append(channel)
                        if success_count > 0:
                            save_archive_metadata(selected_year_month, f"{selected_year_month}_from_sheets.db", channels_saved)
                            st.success(f"✅ {selected_year_month} 저장 완료! ({', '.join(channels_saved)})")
                        else:
                            st.error("저장할 데이터가 없습니다.")
                    except Exception as e:
                        st.error(f"오류: {e}")

    with col2:
        st.markdown("### 📚 저장된 데이터")
        available_months = get_available_months()
        if available_months:
            for month_item in available_months:
                month_data = get_monthly_summary(month_item)
                channels = month_data['channel'].unique() if not month_data.empty else []
                with st.expander(f"📅 {month_item} ({len(channels)}개 채널)"):
                    if not month_data.empty:
                        st.dataframe(month_data[['channel', 'total_revenue', 'total_profit']].rename(
                            columns={'channel': '채널', 'total_revenue': '매출', 'total_profit': '순이익'}
                        ), use_container_width=True)
                    if st.button(f"🗑️ {month_item} 삭제", key=f"delete_{month_item}"):
                        delete_month_data(month_item)
                        st.rerun()
        else:
            st.info("저장된 데이터가 없습니다.")

# ============================
# 8. 월별 비교 탭
# ============================
with tabs[-1]:
    st.markdown("""<div class="section-title"><span>📊 월별 비교 분석</span></div>""", unsafe_allow_html=True)
    
    available_months = get_available_months()
    
    if len(available_months) < 1:
        st.warning("⚠️ 비교할 과거 데이터가 없습니다.")
        st.info("""
        **👉 해결 방법:**
        1. **'📁 아카이빙'** 탭으로 이동하세요.
        2. **'💾 DB에 저장'** 버튼을 눌러 현재 데이터를 저장하세요.
        3. 데이터가 저장되면 이 탭에서 비교 분석이 가능해집니다.
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
                    
                    st.dataframe(
                        comp_df.style.format({
                            f"{label1} 매출": "{:,.0f}", f"{label2} 매출": "{:,.0f}", "매출 차액": "{:+,.0f}", "성장률(%)": "{:+.1f}%",
                            f"{label1} 순이익": "{:,.0f}", f"{label2} 순이익": "{:,.0f}", "이익 성장률(%)": "{:+.1f}%"
                        }).background_gradient(subset=["성장률(%)"], cmap="RdYlGn", vmin=-50, vmax=50),
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