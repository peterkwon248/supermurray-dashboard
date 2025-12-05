import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

DB_PATH = "dashboard_data.db"

def init_database():
    """데이터베이스 초기화 및 테이블 생성"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 월별 요약 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year_month TEXT NOT NULL,
            channel TEXT NOT NULL,
            total_ad_cost REAL,
            total_revenue REAL,
            total_manufacturing_cost REAL,
            total_profit REAL,
            avg_profit_rate REAL,
            roas REAL,
            ad_coverage REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year_month, channel)
        )
    """)
    
    # 일별 상세 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year_month TEXT NOT NULL,
            date TEXT NOT NULL,
            channel TEXT NOT NULL,
            ad_cost REAL,
            revenue REAL,
            manufacturing_cost REAL,
            profit REAL,
            profit_rate REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year_month, date, channel)
        )
    """)
    
    # 아카이빙 메타데이터 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archive_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year_month TEXT NOT NULL UNIQUE,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_name TEXT,
            channels TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ 데이터베이스 초기화 완료!")

def save_monthly_data(year_month: str, channel: str, summary_data: dict, daily_df: pd.DataFrame):
    """월별 데이터 저장"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 월별 요약 저장
        cursor.execute("""
            INSERT OR REPLACE INTO monthly_summary 
            (year_month, channel, total_ad_cost, total_revenue, 
             total_manufacturing_cost, total_profit, avg_profit_rate, roas, ad_coverage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            year_month,
            channel,
            summary_data.get('total_ad_cost', 0),
            summary_data.get('total_revenue', 0),
            summary_data.get('total_manufacturing_cost', 0),
            summary_data.get('total_profit', 0),
            summary_data.get('avg_profit_rate', 0),
            summary_data.get('roas', 0),
            summary_data.get('ad_coverage', 0)
        ))
        
        # 일별 상세 저장
        for _, row in daily_df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO daily_details
                (year_month, date, channel, ad_cost, revenue, 
                 manufacturing_cost, profit, profit_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                year_month,
                row.get('date', ''),
                channel,
                row.get('ad_cost', 0),
                row.get('revenue', 0),
                row.get('manufacturing_cost', 0),
                row.get('profit', 0),
                row.get('profit_rate', 0)
            ))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 데이터 저장 실패: {e}")
        return False
    finally:
        conn.close()

def get_available_months():
    """저장된 월 목록 조회"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT year_month 
        FROM monthly_summary 
        ORDER BY year_month DESC
    """)
    
    months = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return months

def get_monthly_summary(year_month: str = None):
    """월별 요약 데이터 조회"""
    conn = sqlite3.connect(DB_PATH)
    
    if year_month:
        query = f"SELECT * FROM monthly_summary WHERE year_month = '{year_month}'"
    else:
        query = "SELECT * FROM monthly_summary ORDER BY year_month DESC"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def get_daily_details(year_month: str, channel: str = None):
    """일별 상세 데이터 조회"""
    conn = sqlite3.connect(DB_PATH)
    
    if channel:
        query = f"""
            SELECT * FROM daily_details 
            WHERE year_month = '{year_month}' AND channel = '{channel}'
            ORDER BY date
        """
    else:
        query = f"""
            SELECT * FROM daily_details 
            WHERE year_month = '{year_month}'
            ORDER BY date, channel
        """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def delete_month_data(year_month: str):
    """특정 월 데이터 삭제"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM monthly_summary WHERE year_month = ?", (year_month,))
        cursor.execute("DELETE FROM daily_details WHERE year_month = ?", (year_month,))
        cursor.execute("DELETE FROM archive_metadata WHERE year_month = ?", (year_month,))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 데이터 삭제 실패: {e}")
        return False
    finally:
        conn.close()

def save_archive_metadata(year_month: str, file_name: str, channels: list):
    """아카이빙 메타데이터 저장"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO archive_metadata 
            (year_month, file_name, channels)
            VALUES (?, ?, ?)
        """, (year_month, file_name, ','.join(channels)))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 메타데이터 저장 실패: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    # 테스트: 데이터베이스 초기화
    init_database()