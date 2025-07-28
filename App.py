# -- coding: utf-8 --
"""
仮想通貨ポートフォリオ管理Streamlitアプリケーション (Cookieによる永続ログイン対応)

このアプリケーションは、ユーザーの仮想通貨取引履歴を記録・管理し、
現在の資産状況をリアルタイムで可視化するためのツールです。

主な機能:
- ★Cookieを利用した永続ログイン機能
- アカウント作成、ログイン、ログアウト機能
- ユーザーごとのポートフォリオ、ウォッチリスト管理
- CoinGecko APIを利用したリアルタイム価格取得
- Google BigQueryをバックエンドとした取引履歴の永続化
- 取引履歴の追加、編集、削除
"""

# === 1. ライブラリのインポート ===
import streamlit as st
import pandas as pd
import numpy as np
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta # ★ timedeltaを追加
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, Tuple, List, Optional
import re
import bcrypt
import uuid
import secrets # ★ 安全なトークン生成のために追加
from streamlit_cookies_manager import EncryptedCookieManager # ★ Cookie Managerを追加

# === 2. 定数・グローバル設定 ===
# --- BigQuery関連 ---
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_USERS = "users"
TABLE_TRANSACTIONS = "transactions"
TABLE_WATCHLIST = "watchlist"
TABLE_USERS_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_USERS}"
TABLE_TRANSACTIONS_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_TRANSACTIONS}"
TABLE_WATCHLIST_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_WATCHLIST}"

# ★★★ `users`テーブルのスキーマ変更 ★★★
BIGQUERY_SCHEMA_USERS = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("password_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("auth_token", "STRING", mode="NULLABLE"), # 認証トークン
    bigquery.SchemaField("token_expiry", "TIMESTAMP", mode="NULLABLE"), # トークン有効期限
]
BIGQUERY_SCHEMA_TRANSACTIONS = [
    bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("transaction_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coin_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("exchange", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("transaction_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quantity", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("price_jpy", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("fee_jpy", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("total_jpy", "FLOAT64", mode="REQUIRED"),
]
BIGQUERY_SCHEMA_WATCHLIST = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sort_order", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("added_at", "TIMESTAMP", mode="REQUIRED"),
]

COLUMN_NAME_MAP_JA = {
    'transaction_id': '取引ID', 'transaction_date': '登録日', 'coin_name': 'コイン名', 
    'exchange': '取引所', 'transaction_type': '登録種別', 'quantity': '数量', 
    'price_jpy': '価格(JPY)', 'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)', 
    'coin_id': 'コインID'
}

# --- アプリケーションUI関連 ---
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']
EXCHANGES_ORDERED = ['SBIVC', 'BITPOINT', 'Binance', 'bitbank', 'GMOコイン', 'Bybit']

# --- CSSスタイル ---
BLACK_THEME_CSS = """
<style>
body, .main, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
    background-color: #000000;
    color: #E0E0E0;
}
[data-testid="stSidebar"] { background-color: #0E0E0E; }
h1, h2, h3, h4, h5, h6 { color: #FFFFFF; }
[data-testid="stTabs"] { color: #E0E0E0; }
button[data-baseweb="tab"] { color: #9E9E9E; }
button[data-baseweb="tab"][aria-selected="true"] { color: #FFFFFF; border-bottom: 2px solid #FFFFFF; }
[data-testid="stDataFrame"] thead th { background-color: #1E1E1E; color: #FFFFFF; }
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] { border: 1px solid #444444 !important; }
[data-testid="stSelectbox"] > div { background-color: #2a2a2a; border-radius: 8px; border: none; }
[data-testid="stSelectbox"] > div > div { color: #FFFFFF; }
</style>
"""

# === 3. 初期設定 & クライアント初期化 ===
st.set_page_config(page_title="仮想通貨ポートフォリオ", page_icon="🪙", layout="wide")

@st.cache_resource
def get_clients() -> Tuple[Optional[bigquery.Client], Optional[EncryptedCookieManager]]:
    try:
        # BigQuery Client
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        bq_client = bigquery.Client(credentials=creds, project=creds.project_id)

        # Cookie Manager
        cookies = EncryptedCookieManager(
            password=st.secrets["COOKIE_SECRET_KEY"],
        )
        return bq_client, cookies
    except (KeyError, FileNotFoundError) as e:
        st.error(f"設定ファイル `secrets.toml` に必要なキーがありません: {e}")
        return None, None

cg_client = CoinGeckoAPI()
bq_client, cookies = get_clients()

# === 4. 認証関連関数 (Cookie対応) ===
def hash_password(password: str) -> bytes:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def verify_password(plain_password: str, hashed_password: bytes) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def get_user_from_bq(user_id: str) -> Optional[Any]:
    if not bq_client: return None
    query = f"SELECT * FROM `{TABLE_USERS_FULL_ID}` WHERE user_id = @user_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
    ])
    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())
        return results[0] if results else None
    except google.api_core.exceptions.NotFound:
        return None

def create_user_in_bq(user_id: str, password: str) -> bool:
    if not bq_client: return False
    if get_user_from_bq(user_id):
        st.error("このユーザーIDは既に使用されています。")
        return False
    
    hashed_password = hash_password(password)
    user_data = {
        "user_id": user_id,
        "password_hash": hashed_password.decode('utf-8'),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "auth_token": None,
        "token_expiry": None,
    }
    errors = bq_client.insert_rows_json(TABLE_USERS_FULL_ID, [user_data])
    return not errors

def generate_and_store_token(user_id: str) -> str:
    """安全なトークンを生成し、DBに保存して返す"""
    if not bq_client: return ""
    token = secrets.token_hex(32)
    expiry_date = datetime.now(timezone.utc) + timedelta(days=30) # 30日間有効

    query = f"""
    UPDATE `{TABLE_USERS_FULL_ID}`
    SET auth_token = @auth_token, token_expiry = @token_expiry
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("auth_token", "STRING", token),
            bigquery.ScalarQueryParameter("token_expiry", "TIMESTAMP", expiry_date),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()
    return token

def validate_token(token: str) -> Optional[str]:
    """トークンを検証し、有効であればuser_idを返す"""
    if not bq_client or not token: return None
    
    query = f"""
    SELECT user_id FROM `{TABLE_USERS_FULL_ID}`
    WHERE auth_token = @auth_token AND token_expiry > CURRENT_TIMESTAMP()
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("auth_token", "STRING", token)]
    )
    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())
        if results:
            return results[0].user_id
        return None
    except Exception:
        return None

def clear_token(user_id: str):
    """DBからトークンを削除する"""
    if not bq_client: return
    query = f"""
    UPDATE `{TABLE_USERS_FULL_ID}`
    SET auth_token = NULL, token_expiry = NULL
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )
    bq_client.query(query, job_config=job_config).result()

# === 5. BigQuery 操作関数 ===
def init_bigquery_table(table_full_id: str, schema: List[bigquery.SchemaField]):
    if not bq_client: return
    try:
        bq_client.get_table(table_full_id)
    except google.api_core.exceptions.NotFound:
        table_name = table_full_id.split('.')[-1]
        st.toast(f"BigQueryテーブル '{table_name}' を新規作成します。")
        table = bigquery.Table(table_full_id, schema=schema)
        bq_client.create_table(table)
        st.toast(f"テーブル '{table_name}' を作成しました。")

def add_transaction_to_bq(user_id: str, transaction_data: Dict[str, Any]) -> bool:
    if not bq_client: return False
    transaction_id = str(uuid.uuid4())
    query = f"""
    INSERT INTO `{TABLE_TRANSACTIONS_FULL_ID}`
    (transaction_id, user_id, transaction_date, coin_id, coin_name, exchange, transaction_type, quantity, price_jpy, fee_jpy, total_jpy)
    VALUES
    (@transaction_id, @user_id, @transaction_date, @coin_id, @coin_name, @exchange, @transaction_type, @quantity, @price_jpy, @fee_jpy, @total_jpy)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("transaction_date", "TIMESTAMP", transaction_data['transaction_date']),
            bigquery.ScalarQueryParameter("coin_id", "STRING", transaction_data['coin_id']),
            bigquery.ScalarQueryParameter("coin_name", "STRING", transaction_data['coin_name']),
            bigquery.ScalarQueryParameter("exchange", "STRING", transaction_data['exchange']),
            bigquery.ScalarQueryParameter("transaction_type", "STRING", transaction_data['transaction_type']),
            bigquery.ScalarQueryParameter("quantity", "FLOAT64", transaction_data['quantity']),
            bigquery.ScalarQueryParameter("price_jpy", "FLOAT64", transaction_data['price_jpy']),
            bigquery.ScalarQueryParameter("fee_jpy", "FLOAT64", transaction_data['fee_jpy']),
            bigquery.ScalarQueryParameter("total_jpy", "FLOAT64", transaction_data['total_jpy']),
        ]
    )
    try:
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        if query_job.errors:
            st.error(f"履歴の登録中にエラーが発生しました: {query_job.errors}")
            return False
        return True
    except Exception as e:
        st.error(f"履歴の登録中に予期せぬエラーが発生しました: {e}")
        return False

def delete_transaction_from_bq(user_id: str, transaction_id: str) -> bool:
    if not bq_client: return False
    query = f"""
    DELETE FROM `{TABLE_TRANSACTIONS_FULL_ID}`
    WHERE user_id = @user_id AND transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"履歴の削除中にエラーが発生しました: {e}")
        return False

def update_transaction_in_bq(user_id: str, transaction_id: str, updated_data: Dict[str, Any]) -> bool:
    if not bq_client: return False
    query = f"""
    UPDATE `{TABLE_TRANSACTIONS_FULL_ID}`
    SET
        transaction_date = @transaction_date,
        exchange = @exchange,
        quantity = @quantity,
        price_jpy = @price_jpy,
        fee_jpy = @fee_jpy,
        total_jpy = @total_jpy
    WHERE
        user_id = @user_id AND transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_date", "TIMESTAMP", updated_data['transaction_date']),
            bigquery.ScalarQueryParameter("exchange", "STRING", updated_data['exchange']),
            bigquery.ScalarQueryParameter("quantity", "FLOAT64", updated_data['quantity']),
            bigquery.ScalarQueryParameter("price_jpy", "FLOAT64", updated_data['price_jpy']),
            bigquery.ScalarQueryParameter("fee_jpy", "FLOAT64", updated_data['fee_jpy']),
            bigquery.ScalarQueryParameter("total_jpy", "FLOAT64", updated_data['total_jpy']),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
        ]
    )
    try:
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        if query_job.errors:
            st.error(f"履歴の更新中にエラーが発生しました: {query_job.errors}")
            return False
        return True
    except Exception as e:
        st.error(f"履歴の更新中に予期せぬエラーが発生しました: {e}")
        return False

def get_transactions_from_bq(user_id: str) -> pd.DataFrame:
    if not bq_client: return pd.DataFrame()
    query = f"SELECT * FROM `{TABLE_TRANSACTIONS_FULL_ID}` WHERE user_id = @user_id ORDER BY transaction_date DESC"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=False)
        if df.empty: return pd.DataFrame()
        df = df.drop(columns=['user_id'], errors='ignore')
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_convert('Asia/Tokyo')
        return df.rename(columns=COLUMN_NAME_MAP_JA)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_watchlist_from_bq(user_id: str) -> pd.DataFrame:
    if not bq_client: return pd.DataFrame()
    query = f"SELECT coin_id, sort_order FROM `{TABLE_WATCHLIST_FULL_ID}` WHERE user_id = @user_id ORDER BY sort_order ASC"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)])
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)
        return pd.DataFrame()

def update_watchlist_in_bq(user_id: str, ordered_coin_ids: List[str]):
    if not bq_client: return
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )
    delete_query = f"DELETE FROM `{TABLE_WATCHLIST_FULL_ID}` WHERE user_id = @user_id"
    bq_client.query(delete_query, job_config=job_config).result()
    if not ordered_coin_ids: return
    rows_to_insert = [
        {"user_id": user_id, "coin_id": coin_id, "sort_order": i, "added_at": datetime.now(timezone.utc).isoformat()}
        for i, coin_id in enumerate(ordered_coin_ids)
    ]
    errors = bq_client.insert_rows_json(TABLE_WATCHLIST_FULL_ID, rows_to_insert)
    if errors:
        st.error(f"ウォッチリストの更新に失敗しました: {errors}")

# === 6. API & データ処理関数 ===
@st.cache_data(ttl=300)
def get_full_market_data(currency='jpy') -> pd.DataFrame:
    try:
        data = cg_client.get_coins_markets(
            vs_currency=currency, order='market_cap_desc', per_page=250, page=1, sparkline=True
        )
        df = pd.DataFrame(data)
        cols = ['id', 'symbol', 'name', 'image', 'current_price', 'price_change_percentage_24h', 'market_cap', 'sparkline_in_7d']
        df = df[[col for col in cols if col in df.columns]]
        return df
    except Exception as e:
        st.error(f"市場価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_exchange_rate(target_currency: str) -> float:
    if target_currency.lower() == 'jpy': return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency.lower()}')
        return prices['bitcoin'][target_currency.lower()] / prices['bitcoin']['jpy']
    except Exception as e:
        st.warning(f"{target_currency.upper()}の為替レート取得に失敗しました: {e}")
        return 1.0

def calculate_portfolio(transactions_df: pd.DataFrame, market_data: pd.DataFrame) -> Tuple[Dict, float, float]:
    price_map = market_data.set_index('id')['current_price'].to_dict()
    yesterday_price_map = market_data.set_index('id').apply(
        lambda row: row['current_price'] / (1 + row.get('price_change_percentage_24h', 0) / 100) if row.get('price_change_percentage_24h') is not None and (1 + row.get('price_change_percentage_24h', 0) / 100) != 0 else row['current_price'],
        axis=1
    ).to_dict()

    portfolio, total_asset_jpy, total_change_24h_jpy = {}, 0.0, 0.0
    if transactions_df.empty: return portfolio, total_asset_jpy, total_change_24h_jpy
    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity

        if current_quantity > 1e-9:
            price = price_map.get(coin_id, 0)
            yesterday_price = yesterday_price_map.get(coin_id, price)
            change_24h = price - yesterday_price
            value = current_quantity * price
            
            try:
                coin_name = market_data.set_index('id').at[coin_id, 'name']
            except KeyError:
                coin_name = coin_id

            portfolio[(coin_id, exchange)] = {"コイン名": coin_name, "取引所": exchange, "保有数量": current_quantity, "現在価格(JPY)": price, "評価額(JPY)": value, "コインID": coin_id}
            total_asset_jpy += value
            total_change_24h_jpy += current_quantity * change_24h
    return portfolio, total_asset_jpy, total_change_24h_jpy

def summarize_portfolio_by_coin(portfolio: Dict, market_data: pd.DataFrame) -> pd.DataFrame:
    if not portfolio: return pd.DataFrame()
    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    summary = df.groupby('コインID').agg(コイン名=('コイン名', 'first'), 保有数量=('保有数量', 'sum'), 評価額_jpy=('評価額(JPY)', 'sum'), アカウント数=('取引所', 'nunique')).sort_values(by='評価額_jpy', ascending=False)
    market_subset = market_data[['id', 'symbol', 'name', 'price_change_percentage_24h', 'image']].rename(columns={'id': 'コインID'})
    summary = summary.reset_index().merge(market_subset, on='コインID', how='left')
    summary['price_change_percentage_24h'] = summary['price_change_percentage_24h'].fillna(0)
    summary.fillna({'symbol': '', 'image': '', 'name': ''}, inplace=True)
    summary = summary[summary['保有数量'] > 1e-9]
    return summary

def summarize_portfolio_by_exchange(portfolio: Dict) -> pd.DataFrame:
    if not portfolio: return pd.DataFrame()
    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    summary = df.groupby('取引所').agg(
        評価額_jpy=('評価額(JPY)', 'sum'),
        コイン数=('コイン名', 'nunique')
    ).sort_values(by='評価額_jpy', ascending=False).reset_index()
    return summary

def calculate_btc_value(total_asset_jpy: float, market_data: pd.DataFrame) -> float:
    try:
        btc_price_jpy = market_data.set_index('id').at['bitcoin', 'current_price']
        return total_asset_jpy / btc_price_jpy if btc_price_jpy > 0 else 0.0
    except KeyError:
        return 0.0
        
# === 7. UIコンポーネント & ヘルパー関数 ===
def format_price(price: float, symbol: str) -> str:
    if price >= 1:
        formatted = f"{price:,.2f}"
    else:
        formatted = f"{price:,.8f}"
    formatted = re.sub(r'\.0+$', '', formatted)
    formatted = re.sub(r'(\.\d*?[1-9])0+$', r'\1', formatted)
    return f"{symbol}{formatted}"

def format_market_cap(value: float, symbol: str) -> str:
    if symbol == '¥':
        if value >= 1_000_000_000_000: return f"{symbol}{value / 1_000_000_000_000:.2f}兆"
        if value >= 100_000_000: return f"{symbol}{value / 100_000_000:.2f}億"
        if value >= 1_000_000: return f"{symbol}{value / 10_000:,.1f}万"
        return f"{symbol}{value:,.0f}"
    if value >= 1_000_000_000: return f"{symbol}{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000: return f"{symbol}{value / 1_000_000:.2f}M"
    return f"{symbol}{value:,.0f}"

def generate_sparkline_svg(data: List[float], color: str = 'grey', width: int = 80, height: int = 35) -> str:
    if not data or len(data) < 2: return ""
    min_val, max_val = min(data), max(data)
    range_val = max_val - min_val if max_val > min_val else 1
    points = [f"{i * width / (len(data) - 1):.2f},{height - ((d - min_val) / range_val * (height - 4)) - 2:.2f}" for i, d in enumerate(data)]
    path_d = "M " + " L ".join(points)
    return f'<svg width="{width}" height="{height}" viewbox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="overflow: visible;"><path d="{path_d}" stroke="{color}" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round" /></svg>'

def display_summary_card(total_asset_jpy: float, total_asset_btc: float, total_change_24h_jpy: float, currency: str, rate: float):
    is_hidden = st.session_state.get('balance_hidden', False)
    if is_hidden:
        asset_display, btc_display, change_display, pct_display = f"{CURRENCY_SYMBOLS[currency]} *******", "≈ ***** BTC", "*****", "**.**%"
        card_top_bg, card_bottom_bg, change_text_color = "#1E1E1E", "#2A2A2A", "#9E9E9E"
    else:
        yesterday_asset = total_asset_jpy - total_change_24h_jpy
        change_pct = (total_change_24h_jpy / yesterday_asset * 100) if yesterday_asset != 0 else 0
        symbol, is_positive = CURRENCY_SYMBOLS[currency], total_change_24h_jpy >= 0
        card_top_bg, card_bottom_bg = ("#16B583", "#129B72") if is_positive else ("#FF5252", "#E54A4A")
        change_text_color, change_sign = "#FFFFFF", "+" if is_positive else ""
        asset_display = f"{symbol}{(total_asset_jpy * rate):,.2f} {currency.upper()}"
        btc_display = f"≈ {total_asset_btc:.8f} BTC"
        change_display = f"{change_sign}{(total_change_24h_jpy * rate):,.2f} {currency.upper()}"
        pct_display = f"{change_sign}{change_pct:.2f}%"
    card_html = f"""
    <div style="border-radius: 10px; overflow: hidden; font-family: sans-serif;">
        <div style="padding: 20px; background-color: {card_top_bg};">
            <p style="font-size: 0.9em; margin: 0; color: #FFFFFF; opacity: 0.8;">残高</p>
            <p style="font-size: clamp(1.6em, 5vw, 2.2em); font-weight: bold; margin: 0; line-height: 1.2; color: #FFFFFF;">{asset_display}</p>
            <p style="font-size: clamp(0.9em, 2.5vw, 1.1em); font-weight: 500; margin-top: 5px; color: #FFFFFF; opacity: 0.9;">{btc_display}</p>
        </div>
        <div style="padding: 15px 20px; background-color: {card_bottom_bg}; display: flex; align-items: start;">
            <div style="flex-basis: 50%;"><p style="font-size: 0.9em; margin: 0; color: #FFFFFF; opacity: 0.8;">24h 変動額</p><p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {change_text_color};">{change_display}</p></div>
            <div style="flex-basis: 50%;"><p style="font-size: 0.9em; margin: 0; color: #FFFFFF; opacity: 0.8;">24h 変動率</p><p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {change_text_color};">{pct_display}</p></div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

def display_asset_list_new(summary_df: pd.DataFrame, currency: str, rate: float):
    st.subheader("保有資産")
    if summary_df.empty:
        st.info("保有資産はありません。"); return
    
    symbol, is_hidden = CURRENCY_SYMBOLS[currency], st.session_state.get('balance_hidden', False)
    for _, row in summary_df.iterrows():
        change_pct = row.get('price_change_percentage_24h', 0)
        is_positive = change_pct >= 0
        change_color, change_sign = ("#16B583", "▲") if is_positive else ("#FF5252", "▼")
        change_display, image_url = f"{abs(change_pct):.2f}%", row.get('image', '')
        price_per_unit = (row['評価額_jpy'] / row['保有数量']) * rate if row['保有数量'] > 0 else 0
        
        if is_hidden:
            quantity_display, value_display, price_display = "*****", f"{symbol}*****", f"{symbol}*****"
        else:
            quantity_display = f"{row['保有数量']:,.8f}".rstrip('0').rstrip('.')
            value_display = f"{symbol}{row['評価額_jpy'] * rate:,.2f}"
            price_display = f"{symbol}{price_per_unit:,.2f}"
        
        card_html = f"""
        <div style="background-color: #1E1E1E; border: 1px solid #444444; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">
            <div style="display: grid; grid-template-columns: 3fr 3fr 4fr; align-items: center; gap: 10px;">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <img src="{image_url}" width="24" height="24" style="border-radius: 50%;">
                    <div>
                        <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{row["symbol"].upper()}</p>
                        <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{row["アカウント数"]} 取引所</p>
                    </div>
                </div>
                <div style="text-align: right;"><p style="font-size: clamp(0.9em, 2.2vw, 1em); font-weight: 500; margin: 0; color: #E0E0E0;">{quantity_display}</p><p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{price_display}</p></div>
                <div style="text-align: right;"><p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{value_display}</p><p style="font-size: clamp(0.8em, 2vw, 0.9em); color: {change_color}; margin: 0;">{change_sign} {change_display}</p></div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)

def display_exchange_list(summary_exchange_df: pd.DataFrame, currency: str, rate: float):
    st.subheader("取引所別資産")
    symbol = CURRENCY_SYMBOLS[currency]
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if summary_exchange_df.empty:
        st.info("保有資産はありません。"); return

    for _, row in summary_exchange_df.iterrows():
        value_display = f"{symbol}*****" if is_hidden else f"{symbol}{row['評価額_jpy'] * rate:,.2f}"
        card_html = f"""
        <div style="background-color: #1E1E1E; border: 1px solid #444444; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">🏦 {row["取引所"]}</p>
                    <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{row["コイン数"]} 銘柄</p>
                </div>
                <div style="text-align: right;"><p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{value_display}</p></div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)

def display_add_transaction_form(user_id: str, market_data: pd.DataFrame, currency: str):
    with st.expander("新しい取引履歴を追加", expanded=False):
        coin_options = {row['id']: f"{row['name']} ({row['symbol'].upper()})" for _, row in market_data.iterrows()}
        name_map = market_data.set_index('id')['name'].to_dict()
        with st.form(key=f"transaction_form_{currency}", clear_on_submit=True):
            st.subheader("履歴の登録")
            c1, c2, c3 = st.columns(3)
            with c1:
                date = st.date_input("取引日", datetime.now(), key=f"date_{currency}")
                coin_disp = st.selectbox("コイン", options=list(coin_options.keys()), format_func=lambda x: coin_options.get(x,x), key=f"coin_{currency}")
            with c2:
                trans_type = st.selectbox("種別", ["購入", "売却"], key=f"type_{currency}")
                exchange = st.selectbox("取引所", options=EXCHANGES_ORDERED, key=f"exchange_{currency}")
            with c3:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f", key=f"qty_{currency}")
                price = st.number_input("価格 (JPY)", min_value=0.0, format="%.2f", key=f"price_{currency}")
                fee = st.number_input("手数料 (JPY)", min_value=0.0, format="%.2f", key=f"fee_{currency}")
            
            if st.form_submit_button("この内容で登録する"):
                transaction = {
                    "transaction_date": datetime.combine(date, datetime.min.time()),
                    "coin_id": coin_disp, "coin_name": name_map.get(coin_disp, coin_disp),
                    "exchange": exchange, "transaction_type": trans_type, "quantity": quantity,
                    "price_jpy": price, "fee_jpy": fee, "total_jpy": quantity * price
                }
                if add_transaction_to_bq(user_id, transaction):
                    st.success(f"{transaction['coin_name']}の{trans_type}履歴を登録しました。")
                    st.rerun()

def display_transaction_history(user_id: str, transactions_df: pd.DataFrame):
    st.subheader("🗒️ 登録履歴一覧")
    if transactions_df.empty:
        st.info("まだ登録履歴がありません。")
        return
    
    for index, row in transactions_df.iterrows():
        transaction_id = row['取引ID']
        
        if st.session_state.get('editing_transaction_id') == transaction_id:
            with st.form(key=f"edit_form_{transaction_id}"):
                st.markdown(f"**{row['コイン名']}** - {row['登録種別']} の履歴を編集中...")
                cols = st.columns(3)
                with cols[0]:
                    edit_date = st.date_input("取引日", value=row['登録日'], key=f"edit_date_{transaction_id}")
                    edit_exchange = st.selectbox("取引所", options=EXCHANGES_ORDERED, index=EXCHANGES_ORDERED.index(row['取引所']) if row['取引所'] in EXCHANGES_ORDERED else 0, key=f"edit_exchange_{transaction_id}")
                with cols[1]:
                    edit_quantity = st.number_input("数量", min_value=0.0, value=row['数量'], format="%.8f", key=f"edit_qty_{transaction_id}")
                with cols[2]:
                    edit_price = st.number_input("価格 (JPY)", min_value=0.0, value=row['価格(JPY)'], format="%.2f", key=f"edit_price_{transaction_id}")
                    edit_fee = st.number_input("手数料 (JPY)", min_value=0.0, value=row['手数料(JPY)'], format="%.2f", key=f"edit_fee_{transaction_id}")

                btn_cols = st.columns(2)
                with btn_cols[0]:
                    if st.form_submit_button("保存する", use_container_width=True):
                        updated_data = {
                            "transaction_date": datetime.combine(edit_date, datetime.min.time()),
                            "exchange": edit_exchange,
                            "quantity": edit_quantity,
                            "price_jpy": edit_price,
                            "fee_jpy": edit_fee,
                            "total_jpy": edit_quantity * edit_price,
                        }
                        if update_transaction_in_bq(user_id, transaction_id, updated_data):
                            st.toast("履歴を更新しました。", icon="✅")
                            st.session_state.editing_transaction_id = None
                            st.rerun()
                with btn_cols[1]:
                     if st.form_submit_button("キャンセル", use_container_width=True, type="secondary"):
                        st.session_state.editing_transaction_id = None
                        st.rerun()
        else:
            with st.container(border=True):
                cols = st.columns([4, 2, 2])
                with cols[0]:
                    st.markdown(f"**{row['コイン名']}** - {row['登録種別']}")
                    st.caption(f"{row['登録日'].strftime('%Y/%m/%d')} | {row['取引所']}")
                    st.text(f"数量: {row['数量']:.8f}".rstrip('0').rstrip('.'))
                with cols[1]:
                    if st.button("編集 ✏️", key=f"edit_{transaction_id}", use_container_width=True):
                        st.session_state.editing_transaction_id = transaction_id
                        st.rerun()
                with cols[2]:
                    if st.button("削除 🗑️", key=f"del_{transaction_id}", use_container_width=True):
                        if delete_transaction_from_bq(user_id, transaction_id):
                            st.toast(f"履歴を削除しました。", icon="🗑️")
                            st.rerun()

# === 8. ページ描画関数 ===
def render_portfolio_page(user_id: str, jpy_market_data: pd.DataFrame, currency: str, rate: float):
    transactions_df = get_transactions_from_bq(user_id)
    
    portfolio, total_asset_jpy, total_change_jpy = calculate_portfolio(transactions_df, jpy_market_data)
    total_asset_btc = calculate_btc_value(total_asset_jpy, jpy_market_data)
    summary_df = summarize_portfolio_by_coin(portfolio, jpy_market_data)
    summary_exchange_df = summarize_portfolio_by_exchange(portfolio)
    
    col1, col2 = st.columns([0.9, 0.1])
    with col1: 
        display_summary_card(total_asset_jpy, total_asset_btc, total_change_jpy, currency, rate)
    with col2:
        st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
        if st.button("👁️", key=f"toggle_visibility_{currency}", help="残高の表示/非表示", use_container_width=True):
            st.session_state.balance_hidden = not st.session_state.get('balance_hidden', False)
            st.rerun()
        
        button_label, new_currency = (CURRENCY_SYMBOLS['usd'], "usd") if currency == 'jpy' else (CURRENCY_SYMBOLS['jpy'], "jpy")

        if st.button(button_label, key=f"currency_toggle_main_{currency}", help=f"{new_currency.upper()}表示に切り替え", use_container_width=True):
            st.session_state.currency = new_currency
            st.rerun()

        if st.button("🔄", key=f"refresh_data_{currency}", help="市場価格を更新", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
            
    st.divider()
    
    tab_coin, tab_exchange, tab_history = st.tabs(["コイン", "取引所", "履歴"])
    with tab_coin:
        display_asset_list_new(summary_df, currency, rate)
    with tab_exchange:
        display_exchange_list(summary_exchange_df, currency, rate)
    with tab_history:
        display_transaction_history(user_id, transactions_df)
        display_add_transaction_form(user_id, jpy_market_data, currency)

def render_watchlist_row(row_data: pd.Series, currency: str, rate: float, rank: str = " "):
    currency_symbol = CURRENCY_SYMBOLS.get(currency, '$')
    is_positive = row_data.get('price_change_percentage_24h', 0) >= 0
    change_color, change_icon = ("#16B583", "▲") if is_positive else ("#FF5252", "▼")
    
    price_val = row_data.get('current_price', 0) * rate
    mcap_val = row_data.get('market_cap', 0) * rate
    sparkline_prices = row_data.get('sparkline_in_7d', {}).get('price', [])
    formatted_price_str = format_price(price_val, currency_symbol)

    card_html = f"""
    <div style="display: grid; grid-template-columns: 4fr 2fr 3fr; align-items: center; padding: 10px; font-family: sans-serif; border-bottom: 1px solid #1E1E1E;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <div style="color: #9E9E9E; width: 20px; text-align: left;">{rank}</div>
            <img src="{row_data.get('image', '')}" width="36" height="36" style="border-radius: 50%;">
            <div>
                <div style="font-weight: bold; font-size: 1.1em; color: #FFFFFF;">{row_data.get('symbol', '').upper()}</div>
                <div style="font-size: 0.9em; color: #9E9E9E;">{format_market_cap(mcap_val, currency_symbol)}</div>
            </div>
        </div>
        <div style="text-align: right; font-weight: 500; font-size: 1.1em; color: #E0E0E0;">{formatted_price_str}</div>
        <div style="display: flex; justify-content: flex-end; align-items: center; gap: 10px;">
            <div style="width: 70px; height: 35px;">{generate_sparkline_svg(sparkline_prices, change_color)}</div>
            <div style="font-weight: bold; color: {change_color}; min-width: 65px; text-align:right;">
                {change_icon} {abs(row_data.get('price_change_percentage_24h', 0)):.2f}%
            </div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

def render_market_cap_watchlist(market_data: pd.DataFrame, currency: str, rate: float):
    if market_data.empty:
        st.warning("データが取得できませんでした。"); return
    
    for index, row in market_data.head(100).iterrows():
        render_watchlist_row(row, currency, rate, rank=str(index + 1))

def render_custom_watchlist(user_id: str, market_data: pd.DataFrame, currency: str, rate: float):
    watchlist_db = get_watchlist_from_bq(user_id)
    
    if not watchlist_db.empty:
        watchlist_df = watchlist_db.merge(market_data, left_on='coin_id', right_on='id', how='left').dropna(subset=['id'])
        for _, row in watchlist_df.iterrows():
            render_watchlist_row(row, currency, rate)
    else:
        st.info("カスタムウォッチリストは空です。下の編集エリアから銘柄を追加してください。")
    
    st.divider()
    with st.container(border=True):
        st.subheader("ウォッチリストの編集")
        st.info("銘柄の追加・削除が可能です。リストの順番は選択した順になります。")
        
        current_list_ids = watchlist_db['coin_id'].tolist() if not watchlist_db.empty else []
        all_coins_options = {row['id']: f"{row['name']} ({row['symbol'].upper()})" for _, row in market_data.iterrows()}
        
        selected_coins = st.multiselect(
            "銘柄リスト",
            options=all_coins_options.keys(),
            format_func=lambda x: all_coins_options.get(x, x),
            default=current_list_ids,
            label_visibility="collapsed"
        )
        
        if st.button("この内容でウォッチリストを保存"):
            update_watchlist_in_bq(user_id, selected_coins)
            st.toast("ウォッチリストを更新しました。")
            st.cache_data.clear()
            st.rerun()

def render_watchlist_page(user_id: str, jpy_market_data: pd.DataFrame):
    _, col_btn = st.columns([0.9, 0.1])
    with col_btn:
        vs_currency = st.session_state.watchlist_currency
        button_label, new_currency = (CURRENCY_SYMBOLS['usd'], "usd") if vs_currency == 'jpy' else (CURRENCY_SYMBOLS['jpy'], "jpy")

        if st.button(button_label, key="currency_toggle_watchlist", use_container_width=True, help=f"{new_currency.upper()}表示に切り替え"):
            st.session_state.watchlist_currency = new_currency
            st.rerun()

    rate = get_exchange_rate(vs_currency) if vs_currency == 'usd' else 1.0
    
    tab_mcap, tab_custom = st.tabs(["時価総額", "カスタム"])
    
    with tab_mcap:
        render_market_cap_watchlist(jpy_market_data, vs_currency, rate)
    with tab_custom:
        render_custom_watchlist(user_id, jpy_market_data, vs_currency, rate)

# === 9. 認証画面描画関数 ===
def render_auth_page():
    st.title("🪙 仮想通貨ポートフォリオへようこそ")
    st.markdown("アカウントを作成して、あなたの資産を記録・管理しましょう。")

    tab_login, tab_register = st.tabs(["ログイン", "新規アカウント作成"])

    with tab_login:
        with st.form("login_form"):
            user_id = st.text_input("ユーザーID (メールアドレスなど)")
            password = st.text_input("パスワード", type="password")
            submitted = st.form_submit_button("ログイン")
            if submitted:
                if not user_id or not password:
                    st.warning("ユーザーIDとパスワードを入力してください。")
                    return
                user_data = get_user_from_bq(user_id)
                if user_data and verify_password(password, user_data['password_hash'].encode('utf-8')):
                    token = generate_and_store_token(user_id)
                    cookies['auth_token'] = token
                    cookies.save()
                    
                    st.session_state.authenticated = True
                    st.session_state.user_id = user_id
                    st.toast("ログインしました！", icon="🎉")
                    st.rerun()
                else:
                    st.error("ユーザーIDまたはパスワードが正しくありません。")

    with tab_register:
        with st.form("register_form"):
            new_user_id = st.text_input("ユーザーID (メールアドレスなど)", key="reg_id")
            new_password = st.text_input("パスワード", type="password", key="reg_pass")
            confirm_password = st.text_input("パスワード（確認用）", type="password", key="reg_pass_conf")
            submitted = st.form_submit_button("アカウント作成")

            if submitted:
                if not new_user_id or not new_password:
                    st.warning("ユーザーIDとパスワードを入力してください。")
                elif new_password != confirm_password:
                    st.error("パスワードが一致しません。")
                elif len(new_password) < 8:
                    st.warning("パスワードは8文字以上で設定してください。")
                else:
                    if create_user_in_bq(new_user_id, new_password):
                        st.success("アカウントが作成されました！ログインタブからログインしてください。")

# === 10. メイン処理 ===
def main():
    st.markdown(BLACK_THEME_CSS, unsafe_allow_html=True)
    
    if not bq_client or not cookies:
        st.stop()

    st.session_state.setdefault('authenticated', False)
    st.session_state.setdefault('user_id', None)
    st.session_state.setdefault('balance_hidden', False)
    st.session_state.setdefault('currency', 'jpy')
    st.session_state.setdefault('watchlist_currency', 'jpy')
    st.session_state.setdefault('editing_transaction_id', None)
    
    if not st.session_state.authenticated:
        auth_token = cookies.get('auth_token')
        if auth_token:
            user_id = validate_token(auth_token)
            if user_id:
                st.session_state.authenticated = True
                st.session_state.user_id = user_id
            else:
                del cookies['auth_token']
                cookies.save()
    
    if not st.session_state.authenticated:
        init_bigquery_table(TABLE_USERS_FULL_ID, BIGQUERY_SCHEMA_USERS)
        render_auth_page()
        st.stop()

    user_id = st.session_state.user_id

    with st.sidebar:
        st.success(f"{user_id} でログイン中")
        if st.button("ログアウト", use_container_width=True):
            clear_token(user_id)
            if 'auth_token' in cookies:
                del cookies['auth_token']
                cookies.save()
            
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.toast("ログアウトしました。")
            st.rerun()
        st.divider()
        st.write("表示設定")

    try:
        init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
        init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)
    except Exception as e:
        st.error(f"データベースの初期化中にエラーが発生しました: {e}")
        st.stop()

    jpy_market_data = get_full_market_data(currency='jpy')
    if jpy_market_data.empty:
        st.error("市場データを取得できませんでした。"); st.stop()
    
    usd_rate = get_exchange_rate('usd')

    portfolio_tab, watchlist_tab = st.tabs(["ポートフォリオ", "ウォッチリスト"])

    with portfolio_tab:
        current_currency = st.session_state.currency
        current_rate = usd_rate if current_currency == 'usd' else 1.0
        render_portfolio_page(user_id, jpy_market_data, currency=current_currency, rate=current_rate)

    with watchlist_tab:
        render_watchlist_page(user_id, jpy_market_data)

if __name__ == "__main__":
    main()
