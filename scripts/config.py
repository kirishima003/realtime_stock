# config.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2 import OperationalError

from dotenv import load_dotenv
import os
#.envファイル読み込み関数作成
#def get_alpaca_key():
#関数内で変数を定義するとローカル変数になり、外部から読み込みができないため、関数外す。
# .env ファイルを読み込む
load_dotenv()

# 環境変数を取得
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL")

print("API Key 取得完了API Key:", ALPACA_API_KEY[:5] + "*****") 

# ----------------------------
# 1. PostgreSQL接続設定
# ----------------------------
DB_USER = "admin"
DB_PASSWORD = "pass"
DB_NAME = "kaggle_db"
DB_HOST = "localhost"  # Dockerのポートをホスト側に公開
DB_PORT = "5433"

# ----------------------------
# 2. SQLAlchemy 接続用
# ----------------------------
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=False)

def test_sqlalchemy_connection():
    """
    SQLAlchemyを使った接続テスト
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ SQLAlchemy 接続成功！")
            for row in result:
                print(f"PostgreSQL バージョン: {row[0]}")
    except SQLAlchemyError as e:
        print("❌ SQLAlchemy 接続エラー:", e)

# ----------------------------
# 3. psycopg2 接続用
# ----------------------------
def get_connection():
    """
    psycopg2を使ってPostgreSQLに直接接続する関数。
    """
    conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    return conn

def test_psycopg2_connection():
    """
    psycopg2を使った接続テスト
    """
    try:
        with get_connection() as conn:
            print("✅ psycopg2 接続成功！")
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print(f"PostgreSQL バージョン: {version[0]}")
    except OperationalError as e:
        print("❌ psycopg2 接続エラー:", e)

# ----------------------------
# 4. 直接実行時のテスト
# ----------------------------
if __name__ == "__main__":
    print("=== SQLAlchemy 接続テスト ===")
    test_sqlalchemy_connection()
    print("\n=== psycopg2 接続テスト ===")
    test_psycopg2_connection()
    # print("\n=== APIkey 取得テスト ===")
    # get_alpaca_key()