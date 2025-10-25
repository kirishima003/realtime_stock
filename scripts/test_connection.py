from dotenv import load_dotenv
import os
import requests

# .env ファイルを読み込む
load_dotenv()

# 環境変数を取得
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL")

print("API Key:", ALPACA_API_KEY[:5] + "*****") 



response = requests.get(
    "https://paper-api.alpaca.markets/v2/account",
    headers={
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
    }
)

print(response.json())
