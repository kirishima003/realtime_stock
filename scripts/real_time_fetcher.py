import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from alpaca_trade_api import REST, Stream
import asyncio
import config

# ======================================================
# 1. Alpaca API 設定
# ======================================================
api = REST(
    config.ALPACA_API_KEY,
    config.ALPACA_SECRET_KEY,
    base_url=config.ALPACA_BASE_URL
)

TABLE_NAME = "real_time_prices"
TICKERS = ["AAPL", "MSFT", "GOOG"]  # 取得したいティッカー
SAVE_INTERVAL = 5  # 秒ごとに保存

# ======================================================
# 2. DB初期化
# ======================================================
def init_table():
    """PostgreSQLにリアルタイム株価テーブルを作成（存在しなければ）"""
    create_table_query = text(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        ticker TEXT,
        price FLOAT,
        volume BIGINT,
        timestamp TIMESTAMP,
        fetch_time TIMESTAMP
    );
    """)
    with config.engine.begin() as conn:
        conn.execute(create_table_query)
    print(f"🧱 テーブル '{TABLE_NAME}' の準備完了。")

# ======================================================
# 3. 株価保存処理
# ======================================================
def save_to_db(df: pd.DataFrame):
    if df.empty:
        return
    try:
        with config.engine.begin() as conn:
            df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        print(f"💾 {len(df)}件のデータを保存しました。")
    except Exception as e:
        print("❌ DB保存エラー:", e)

# ======================================================
# 4. 非同期リアルタイム取得処理
# ======================================================
async def stream_realtime_prices():
    """
    AlpacaのWebSocketストリームからリアルタイム株価を取得し、DBへ保存。
    """
    init_table()
    stream = Stream(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY, base_url=config.ALPACA_BASE_URL, data_feed="iex")

    buffer = []

    async def on_quote(q):
        """新しい株価が届くたびに呼ばれるコールバック"""
        print(f"📈 {q.symbol}: {q.bid_price} @ {q.timestamp}")
        buffer.append({
            "ticker": q.symbol,
            "price": q.bid_price,
            "volume": q.bid_size,
            "timestamp": datetime.fromtimestamp(q.timestamp.timestamp()),
            "fetch_time": datetime.now()
        })

        # 一定間隔でDB保存
        if len(buffer) >= SAVE_INTERVAL:
            df = pd.DataFrame(buffer)
            save_to_db(df)
            buffer.clear()

    # --- 購読設定 ---
    for t in TICKERS:
        stream.subscribe_quotes(on_quote, t)

    print(f"🚀 {TICKERS} のリアルタイム株価を取得中...")
    await stream.run()

# ======================================================
# 5. メイン実行
# ======================================================
if __name__ == "__main__":
    try:
        asyncio.run(stream_realtime_prices())
    except KeyboardInterrupt:
        print("\n🛑 ユーザーによる停止。")
    except Exception as e:
        print("❌ 予期せぬエラー:", e)
