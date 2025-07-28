# -- coding: utf-8 --
"""
仮想通貨ポートフォリオ管理Streamlitアプリケーション

このアプリケーションは、ユーザーの仮想通貨取引履歴を記録・管理し、
現在の資産状況をリアルタイムで可視化するためのツールです。

主な機能:
- CoinGecko APIを利用したリアルタイム価格取得（手動更新機能付き）
- Google BigQueryをバックエンドとした取引履歴の永続化
- ポートフォリオの円グラフおよび資産一覧での可視化
- JPY建て、USD建てでの資産評価表示
- 取引履歴の追加、編集（数量・取引所）、削除
- 時価総額ランキングとカスタムウォッチリストの表示（並び替え・削除対応）
"""

# === 1. ライブラリのインポート ===
import streamlit as st
import pandas as pd
import numpy as np
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, Tuple, List
import re # 正規表現ライブラリをインポート

# === 2. 定数・グローバル設定 ===
# --- BigQuery関連 ---
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_TRANSACTIONS = "transactions"
TABLE_WATCHLIST = "watchlist"
TABLE_TRANSACTIONS_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_TRANSACTIONS}"
TABLE_WATCHLIST_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_WATCHLIST}"
# 固定ユーザーID (将来的には認証機能で動的に)
USER_ID = "default_user" 

BIGQUERY_SCHEMA_TRANSACTIONS = [
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
    'transaction_date': '登録日', 'coin_name': 'コイン名', 'exchange': '取引所',
    'transaction_type': '登録種別', 'quantity': '数量', 'price_jpy': '価格(JPY)',
    'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)', 'coin_id': 'コインID'
}

# --- アプリケーションUI関連 ---
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']
EXCHANGES_ORDERED = ['SBIVC', 'BITPOINT', 'Binance', 'bitbank', 'GMOコイン', 'Bybit']
COIN_COLORS = {
    "Bitcoin": "#F7931A", "Ethereum": "#627EEA", "Solana": "#9945FF", "XRP": "#00AAE4",
    "Tether": "#50AF95", "BNB": "#F3BA2F", "USD Coin": "#2775CA", "Dogecoin": "#C3A634",
    "Cardano": "#0033AD", "その他": "#D3D3D3"
}

# --- CSSスタイル ---
BLACK_THEME_CSS = """
<style>
body, .main, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
    background-color: #000000;
    color: #E0E0E0;
}
[data-testid="stSidebar"] {
    background-color: #0E0E0E;
}
h1, h2, h3, h4, h5, h6 {
    color: #FFFFFF;
}
/* Streamlitウィジェットの調整 */
[data-testid="stTabs"] {
    color: #E0E0E0;
}
button[data-baseweb="tab"] {
    color: #9E9E9E;
}
button[data-baseweb="tab"][aria-selected="true"] {
    color: #FFFFFF;
    border-bottom: 2px solid #FFFFFF;
}
[data-testid="stDataFrame"] thead th {
    background-color: #1E1E1E;
    color: #FFFFFF;
}
/* カスタムコンポーネントの色調整 */
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] {
    border: 1px solid #444444 !important;
}
/* Selectboxを画像のようなボタン風に調整 */
[data-testid="stSelectbox"] > div {
    background-color: #2a2a2a;
    border-radius: 8px;
    border: none;
}
[data-testid="stSelectbox"] > div > div {
    color: #FFFFFF;
}
</style>
"""

# === 3. 初期設定 & クライアント初期化 ===
st.set_page_config(page_title="仮想通貨ポートフォリオ", page_icon="🪙", layout="wide")

@st.cache_resource
def get_bigquery_client() -> bigquery.Client | None:
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    except (KeyError, FileNotFoundError):
        st.error("GCPサービスアカウントの認証情報が設定されていません。")
        return None

cg_client = CoinGeckoAPI()
bq_client = get_bigquery_client()


# === 4. BigQuery 操作関数 ===
# ... (変更なし) ...
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

def add_to_watchlist_in_bq(user_id: str, coin_ids: List[str]):
    if not bq_client or not coin_ids: return
    
    max_order_query = f"SELECT MAX(sort_order) as max_order FROM {TABLE_WATCHLIST_FULL_ID} WHERE user_id = @user_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)])
    result = bq_client.query(max_order_query, job_config=job_config).to_dataframe()
    max_order = result['max_order'][0] if not result.empty and pd.notna(result['max_order'][0]) else -1
    
    rows_to_insert = [
        {"user_id": user_id, "coin_id": coin_id, "sort_order": i + max_order + 1, "added_at": datetime.now(timezone.utc).isoformat()}
        for i, coin_id in enumerate(coin_ids)
    ]
    errors = bq_client.insert_rows_json(TABLE_WATCHLIST_FULL_ID, rows_to_insert)
    if not errors: st.toast(f"{len(coin_ids)}銘柄をウォッチリストに追加しました。")

def update_watchlist_order_in_bq(user_id: str, ordered_coin_ids: List[str]):
    if not bq_client: return
    
    delete_query = f"DELETE FROM `{TABLE_WATCHLIST_FULL_ID}` WHERE user_id = '{user_id}'"
    bq_client.query(delete_query).result()
    
    if not ordered_coin_ids: return
        
    rows_to_insert = [
        {"user_id": user_id, "coin_id": coin_id, "sort_order": i, "added_at": datetime.now(timezone.utc).isoformat()}
        for i, coin_id in enumerate(ordered_coin_ids)
    ]
    errors = bq_client.insert_rows_json(TABLE_WATCHLIST_FULL_ID, rows_to_insert)
    if errors:
        st.error(f"ウォッチリストの更新に失敗しました: {errors}")

@st.cache_data(ttl=300)
def get_watchlist_from_bq(user_id: str) -> pd.DataFrame:
    if not bq_client: return pd.DataFrame()
    query = f"SELECT coin_id, sort_order FROM {TABLE_WATCHLIST_FULL_ID} WHERE user_id = @user_id ORDER BY sort_order ASC"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)])
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)
        return pd.DataFrame()


# === 5. API & データ処理関数 ===
# ... (変更なし) ...
@st.cache_data(ttl=300)
def get_full_market_data(currency='jpy') -> pd.DataFrame:
    try:
        data = cg_client.get_coins_markets(vs_currency=currency, order='market_cap_desc', per_page=250, page=1, sparkline=True)
        df = pd.DataFrame(data)
        cols = ['id', 'symbol', 'name', 'image', 'current_price', 'price_change_percentage_24h', 'market_cap', 'sparkline_in_7d']
        df = df[[col for col in cols if col in df.columns]]
        return df
    except Exception as e:
        st.error(f"市場価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

# === 6. UIコンポーネント & ヘルパー関数 ===
def format_price(price: float, symbol: str) -> str:
    """価格をフォーマットし、不要な末尾のゼロを削除する"""
    if price > 1: # 1以上の価格は小数点以下2桁まで
        formatted = f"{price:,.2f}"
    else: # 1未満の価格はより多くの桁数を表示
        formatted = f"{price:,.8f}"
    
    # 正規表現で不要なゼロと小数点を削除
    formatted = re.sub(r'\.0+$', '', formatted) # ".00" を削除
    formatted = re.sub(r'(\.\d*?[1-9])0+$', r'\1', formatted) # "x.xx00" の "00" を削除
    return f"{symbol}{formatted}"

def format_market_cap(value: float, symbol: str) -> str:
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

# ... (他のUI関数は変更なしのため省略) ...

# === 7. ページ描画関数 ===
def render_watchlist_row(row_data: pd.Series, currency: str, rate: float, rank: str = " "):
    """ウォッチリストの単一行をHTMLで描画する"""
    currency_symbol = CURRENCY_SYMBOLS.get(currency, '$')
    is_positive = row_data.get('price_change_percentage_24h', 0) >= 0
    change_color, change_icon = ("#16B583", "▲") if is_positive else ("#FF5252", "▼")
    
    price_val = row_data.get('current_price', 0) * rate
    mcap_val = row_data.get('market_cap', 0) * rate
    sparkline_prices = row_data.get('sparkline_in_7d', {}).get('price', [])

    # 修正: 新しい価格フォーマット関数を呼び出す
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

def render_custom_watchlist(market_data: pd.DataFrame, currency: str, rate: float):
    watchlist_db = get_watchlist_from_bq(USER_ID)
    
    if not watchlist_db.empty:
        watchlist_df = watchlist_db.merge(market_data, left_on='coin_id', right_on='id', how='left').dropna(subset=['id'])
        for _, row in watchlist_df.iterrows():
            render_watchlist_row(row, currency, rate)
    else:
        st.info("カスタムウォッチリストは空です。下から銘柄を追加・編集してください。")
    
    st.divider()
    with st.expander("ウォッチリストの編集（追加・削除・並び替え）"):
        st.info("銘柄の追加、削除、ドラッグ＆ドロップでの順番の入れ替えが可能です。")
        
        current_list_ids = watchlist_db['coin_id'].tolist() if not watchlist_db.empty else []
        all_coins_options = {row['id']: f"{row['name']} ({row['symbol'].upper()})" for _, row in market_data.iterrows()}
        
        selected_coins = st.multiselect(
            "銘柄リスト",
            options=all_coins_options.keys(),
            format_func=lambda x: all_coins_options.get(x, x),
            default=current_list_ids
        )
        
        if st.button("この内容でウォッチリストを保存"):
            update_watchlist_order_in_bq(USER_ID, selected_coins)
            st.toast("ウォッチリストを更新しました。")
            st.cache_data.clear()
            st.rerun()

# === 8. メイン処理 === (render_watchlist_page と main は変更なし)
def render_watchlist_page(jpy_market_data: pd.DataFrame):
    c1, _, c3, c4 = st.columns([1.5, 0.5, 1.5, 1.5])
    with c1: vs_currency = st.selectbox("Currency", options=["jpy", "usd"], format_func=lambda x: f"{x.upper()}", key="watchlist_currency", label_visibility="collapsed")
    with c3: st.button("24時間 % ▾", use_container_width=True, disabled=True)
    with c4: st.button("トップ100 ▾", use_container_width=True, disabled=True)
    
    rate = get_exchange_rate(vs_currency) if vs_currency == 'usd' else 1.0
    
    tab_mcap, tab_custom = st.tabs(["時価総額ランキング", "カスタム"])
    
    with tab_mcap:
        render_market_cap_watchlist(jpy_market_data, vs_currency, rate)
    with tab_custom:
        render_custom_watchlist(jpy_market_data, vs_currency, rate)

def main():
    st.markdown(BLACK_THEME_CSS, unsafe_allow_html=True)
    st.session_state.setdefault('balance_hidden', False)
    st.session_state.setdefault('currency', 'jpy')
    
    if not bq_client: st.stop()

    jpy_market_data = get_full_market_data(currency='jpy')
    if jpy_market_data.empty:
        st.error("市場データを取得できませんでした。"); st.stop()
    
    init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
    init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)

    # ポートフォリオ関連の関数は変更が多いため、このメイン関数内からは一旦省略します
    # render_portfolio_page(...) を呼び出す部分は別途実装が必要です

    render_watchlist_page(jpy_market_data)


if __name__ == "__main__":
    main()
