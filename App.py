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
- 時価総額ランキングとカスタムウォッチリストの表示
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
def init_bigquery_table(table_full_id: str, schema: List[bigquery.SchemaField]):
    if not bq_client: return
    try:
        bq_client.get_table(table_full_id)
    except google.api_core.exceptions.NotFound:
        table = bigquery.Table(table_full_id, schema=schema)
        bq_client.create_table(table)
        st.toast(f"BigQueryテーブル '{table_full_id}' を新規作成しました。")

def add_transaction_to_bq(transaction_data: Dict[str, Any]) -> bool:
    if not bq_client: return False
    transaction_data["transaction_date"] = datetime.now(timezone.utc).isoformat()
    errors = bq_client.insert_rows_json(TABLE_TRANSACTIONS_FULL_ID, [transaction_data])
    return not errors

def delete_transaction_from_bq(transaction: pd.Series) -> bool:
    # ... (この関数は変更なし)
    if not bq_client: return False
    query = f"""
    DELETE FROM {TABLE_TRANSACTIONS_FULL_ID}
    WHERE transaction_date = @transaction_date AND coin_id = @coin_id AND exchange = @exchange
    AND transaction_type = @transaction_type AND quantity = @quantity
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_date", "TIMESTAMP", transaction['登録日']),
            bigquery.ScalarQueryParameter("coin_id", "STRING", transaction['コインID']),
            bigquery.ScalarQueryParameter("exchange", "STRING", transaction['取引所']),
            bigquery.ScalarQueryParameter("transaction_type", "STRING", transaction['登録種別']),
            bigquery.ScalarQueryParameter("quantity", "FLOAT64", transaction['数量']),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"履歴の削除中にエラーが発生しました: {e}")
        return False

def update_transaction_in_bq(original_transaction: pd.Series, updated_data: Dict[str, Any]) -> bool:
    # ... (この関数は変更なし)
    if not bq_client: return False
    set_clauses, query_params = [], []
    for key, value in updated_data.items():
        set_clauses.append(f"{key} = @{key}")
        field_type = next((field.field_type for field in BIGQUERY_SCHEMA_TRANSACTIONS if field.name == key), "STRING")
        query_params.append(bigquery.ScalarQueryParameter(key, field_type, value))
    
    if not set_clauses:
        st.warning("更新する項目がありません。")
        return False

    set_sql = ", ".join(set_clauses)
    where_params = [
        bigquery.ScalarQueryParameter("where_transaction_date", "TIMESTAMP", original_transaction['登録日']),
        bigquery.ScalarQueryParameter("where_coin_id", "STRING", original_transaction['コインID']),
        bigquery.ScalarQueryParameter("where_exchange", "STRING", original_transaction['取引所']),
        bigquery.ScalarQueryParameter("where_transaction_type", "STRING", original_transaction['登録種別']),
        bigquery.ScalarQueryParameter("where_quantity", "FLOAT64", original_transaction['数量']),
    ]
    query = f"""
    UPDATE {TABLE_TRANSACTIONS_FULL_ID} SET {set_sql}
    WHERE transaction_date = @where_transaction_date AND coin_id = @where_coin_id
    AND exchange = @where_exchange AND transaction_type = @where_transaction_type
    AND quantity = @where_quantity
    """
    job_config = bigquery.QueryJobConfig(query_parameters=query_params + where_params)
    try:
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        return query_job.num_dml_affected_rows is None or query_job.num_dml_affected_rows > 0
    except Exception as e:
        st.error(f"履歴の更新中にエラーが発生しました: {e}")
        return False

def get_transactions_from_bq() -> pd.DataFrame:
    if not bq_client: return pd.DataFrame()
    query = f"SELECT * FROM {TABLE_TRANSACTIONS_FULL_ID} ORDER BY transaction_date DESC"
    try:
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
        if df.empty: return pd.DataFrame()
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_convert('Asia/Tokyo')
        return df.rename(columns=COLUMN_NAME_MAP_JA)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
        return pd.DataFrame()

# --- ウォッチリスト用 BigQuery 操作関数 ---
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

def add_to_watchlist_in_bq(user_id: str, coin_ids: List[str]):
    if not bq_client or not coin_ids: return
    
    max_order_query = f"SELECT MAX(sort_order) as max_order FROM {TABLE_WATCHLIST_FULL_ID} WHERE user_id = @user_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)])
    result = bq_client.query(max_order_query, job_config=job_config).to_dataframe()
    max_order = result['max_order'][0] if not result.empty and pd.notna(result['max_order'][0]) else -1
    
    rows_to_insert = [
        {"user_id": user_id, "coin_id": coin_id, "sort_order": i + max_order + 1, "added_at": datetime.now(timezone.utc)}
        for i, coin_id in enumerate(coin_ids)
    ]
    errors = bq_client.insert_rows_json(TABLE_WATCHLIST_FULL_ID, rows_to_insert)
    if not errors: st.toast(f"{len(coin_ids)}銘柄をウォッチリストに追加しました。")
    
def remove_from_watchlist_in_bq(user_id: str, coin_id: str):
    if not bq_client: return
    query = f"DELETE FROM {TABLE_WATCHLIST_FULL_ID} WHERE user_id = @user_id AND coin_id = @coin_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("coin_id", "STRING", coin_id)
    ])
    bq_client.query(query, job_config=job_config).result()

def update_watchlist_order_in_bq(user_id: str, ordered_coin_ids: List[str]):
    if not bq_client or not ordered_coin_ids: return
    
    # MERGE文を使って一括更新
    updates_sql = ",\n".join([f"('{coin_id}', {i})" for i, coin_id in enumerate(ordered_coin_ids)])
    query = f"""
    MERGE {TABLE_WATCHLIST_FULL_ID} T
    USING (SELECT * FROM UNNEST(ARRAY<STRUCT<coin_id STRING, new_order INT64>>[
        {updates_sql}
    ])) S
    ON T.user_id = @user_id AND T.coin_id = S.coin_id
    WHEN MATCHED THEN
      UPDATE SET sort_order = S.new_order;
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)])
    bq_client.query(query, job_config=job_config).result()

# === 5. API & データ処理関数 (変更なし) ===
@st.cache_data(ttl=600)
def get_market_data() -> pd.DataFrame:
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=100, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'image', 'current_price', 'price_change_24h', 'price_change_percentage_24h', 'market_cap'])
        return df.rename(columns={'current_price': 'price_jpy', 'price_change_24h': 'price_change_24h_jpy'})
    except Exception as e:
        st.error(f"市場価格データの取得に失敗しました: {e}")
        return pd.DataFrame()
# ... 他のデータ処理関数は変更なしのため省略 ...
def get_exchange_rate(target_currency: str) -> float:
    if target_currency.lower() == 'jpy': return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency.lower()}')
        return prices['bitcoin'][target_currency.lower()] / prices['bitcoin']['jpy']
    except Exception as e:
        st.warning(f"{target_currency.upper()}の為替レート取得に失敗しました: {e}")
        return 1.0
def calculate_portfolio(transactions_df: pd.DataFrame, price_map: Dict[str, float],price_change_map: Dict[str, float], name_map: Dict[str, str]) -> Tuple[Dict, float, float]:
    portfolio, total_asset_jpy, total_change_24h_jpy = {}, 0.0, 0.0
    if transactions_df.empty: return portfolio, total_asset_jpy, total_change_24h_jpy
    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity
        if current_quantity > 1e-9:
            price, change_24h = price_map.get(coin_id, 0), price_change_map.get(coin_id, 0)
            value = current_quantity * price
            portfolio[(coin_id, exchange)] = {"コイン名": name_map.get(coin_id, coin_id), "取引所": exchange, "保有数量": current_quantity, "現在価格(JPY)": price, "評価額(JPY)": value, "コインID": coin_id}
            total_asset_jpy += value
            total_change_24h_jpy += current_quantity * change_24h
    return portfolio, total_asset_jpy, total_change_24h_jpy
def summarize_portfolio_by_coin(portfolio: Dict, market_data: pd.DataFrame) -> pd.DataFrame:
    if not portfolio: return pd.DataFrame()
    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    summary = df.groupby('コインID').agg(コイン名=('コイン名', 'first'), 保有数量=('保有数量', 'sum'), 評価額_jpy=('評価額(JPY)', 'sum'), アカウント数=('取引所', 'nunique')).sort_values(by='評価額_jpy', ascending=False)
    market_subset = market_data[['id', 'symbol', 'price_change_percentage_24h', 'image']].rename(columns={'id': 'コインID'})
    summary = summary.reset_index().merge(market_subset, on='コインID', how='left')
    summary['price_change_percentage_24h'] = summary['price_change_percentage_24h'].fillna(0)
    summary['symbol'], summary['image'] = summary['symbol'].fillna(''), summary['image'].fillna('')
    summary = summary[summary['保有数量'] > 1e-9]
    return summary
def summarize_portfolio_by_exchange(portfolio: Dict) -> pd.DataFrame:
    if not portfolio: return pd.DataFrame()
    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    return df.groupby('取引所').agg(評価額_jpy=('評価額(JPY)', 'sum'), コイン数=('コイン名', 'nunique')).sort_values(by='評価額_jpy', ascending=False).reset_index()
def calculate_btc_value(total_asset_jpy: float, price_map: Dict[str, float]) -> float:
    btc_price_jpy = price_map.get('bitcoin', 0)
    return total_asset_jpy / btc_price_jpy if btc_price_jpy > 0 else 0.0

# === 6. UIコンポーネント & ヘルパー関数 (変更なし) ===
# ... 既存のUI関数は変更なしのため省略 ...
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
# ... 他のUI関数も同様に省略 ...
def display_summary_card(total_asset_jpy: float, total_asset_btc: float, total_change_24h_jpy: float, currency: str, rate: float):
    # (変更なし)
    is_hidden = st.session_state.get('balance_hidden', False)
    if is_hidden:
        asset_display, btc_display, change_display, pct_display = f"{CURRENCY_SYMBOLS[currency]} *******", "≈ ***** BTC", "*****", "**.**%"
        card_top_bg, card_bottom_bg, change_text_color = "#1E1E1E", "#2A2A2A", "#9E9E9E"
    else:
        yesterday_asset = total_asset_jpy - total_change_24h_jpy
        change_pct = (total_change_24h_jpy / yesterday_asset * 100) if yesterday_asset > 0 else 0
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

def display_composition_bar(summary_df: pd.DataFrame):
    # (変更なし)
    if summary_df.empty or summary_df['評価額_jpy'].sum() <= 0: return
    total_value = summary_df['評価額_jpy'].sum()
    top_n = 5
    display_df = summary_df.head(top_n).copy()
    if len(summary_df) > top_n:
        other_value = summary_df.tail(len(summary_df) - top_n)['評価額_jpy'].sum()
        other_row = pd.DataFrame([{'コイン名': 'その他', '評価額_jpy': other_value, 'symbol': 'その他'}])
        display_df = pd.concat([display_df, other_row], ignore_index=True)

    display_df['percentage'] = (display_df['評価額_jpy'] / total_value) * 100
    display_df['color'] = display_df['コイン名'].map(COIN_COLORS).fillna("#D3D3D3")
    
    legend_parts = ['<div style="display: flex; flex-wrap: nowrap; gap: 15px; overflow-x: auto; padding-bottom: 5px;">']
    for _, row in display_df.iterrows():
        display_text = row['symbol'].upper() if row['コイン名'] != 'その他' else 'その他'
        legend_parts.append(f'<div style="display: flex; align-items: center; flex-shrink: 0;"><div style="width: 12px; height: 12px; background-color: {row["color"]}; border-radius: 3px; margin-right: 5px;"></div><span style="font-size: clamp(0.75em, 2vw, 0.9em); color: #E0E0E0;">{display_text} {row["percentage"]:.2f}%</span></div>')
    legend_parts.append('</div>')
    st.markdown("".join(legend_parts), unsafe_allow_html=True)

    bar_html = '<div style="display: flex; width: 100%; height: 12px; border-radius: 5px; overflow: hidden; margin-top: 10px;">' + "".join([f'<div style="width: {row["percentage"]}%; background-color: {row["color"]};"></div>' for _, row in display_df.iterrows()]) + '</div>'
    st.markdown(bar_html, unsafe_allow_html=True)

def display_asset_list_new(summary_df: pd.DataFrame, currency: str, rate: float):
    # (変更なし)
    st.subheader("保有資産")
    if summary_df.empty:
        st.info("保有資産はありません。")
        return
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
                <div style="display: flex; align-items: center; gap: 12px;"><img src="{image_url}" width="36" height="36" style="border-radius: 50%;"><div><p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{row["symbol"].upper()}</p><p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{row["アカウント数"]} 取引所</p></div></div>
                <div style="text-align: right;"><p style="font-size: clamp(0.9em, 2.2vw, 1em); font-weight: 500; margin: 0; color: #E0E0E0;">{quantity_display}</p><p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{price_display}</p></div>
                <div style="text-align: right;"><p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{value_display}</p><p style="font-size: clamp(0.8em, 2vw, 0.9em); color: {change_color}; margin: 0;">{change_sign} {change_display}</p></div>
            </div>
        </div>"""
        st.markdown(card_html, unsafe_allow_html=True)
# ... 他のUI関数も同様に省略 ...

# === 7. ページ描画関数 ===
def render_portfolio_page(transactions_df: pd.DataFrame, market_data: pd.DataFrame, currency: str, rate: float):
    # ... (変更なし)
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    portfolio, total_asset_jpy, total_change_jpy = calculate_portfolio(transactions_df, price_map, price_change_map, name_map)
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    summary_df = summarize_portfolio_by_coin(portfolio, market_data)
    summary_exchange_df = summarize_portfolio_by_exchange(portfolio)

    col1, col2 = st.columns([0.9, 0.1])
    with col1: display_summary_card(total_asset_jpy, total_asset_btc, total_change_jpy, currency, rate)
    with col2:
        st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
        if st.button("👁️", key=f"toggle_visibility_{currency}", help="残高の表示/非表示"):
            st.session_state.balance_hidden = not st.session_state.get('balance_hidden', False)
            st.rerun()
        button_label, new_currency = ("USD", "usd") if currency == 'jpy' else ("JPY", "jpy")
        if st.button(button_label, key=f"currency_toggle_main_{currency}"):
            st.session_state.currency = new_currency
            st.rerun()
        if st.button("🔄", key=f"refresh_data_{currency}", help="市場価格を更新"):
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    tab_coin, tab_exchange, tab_history = st.tabs(["コイン", "取引所", "履歴"])
    with tab_coin:
        display_composition_bar(summary_df)
        st.markdown("<br>", unsafe_allow_html=True) 
        display_asset_list_new(summary_df, currency, rate)
    # ... 他のタブも変更なし ...

def render_market_cap_watchlist(market_data: pd.DataFrame, vs_currency: str):
    """時価総額ランキングのウォッチリストを描画"""
    @st.cache_data(ttl=600)
    def get_sparkline_data(currency: str) -> pd.DataFrame:
        try:
            data = cg_client.get_coins_markets(vs_currency=currency, order='market_cap_desc', per_page=100, page=1, sparkline=True)
            return pd.DataFrame(data)
        except Exception: return pd.DataFrame()
    
    watchlist_df = get_sparkline_data(vs_currency)
    if watchlist_df.empty:
        st.warning("データが取得できませんでした。")
        return

    currency_symbol = CURRENCY_SYMBOLS.get(vs_currency, '$')
    for index, row in watchlist_df.iterrows():
        # ... (この部分は変更なし) ...
        rank, image_url, symbol = index + 1, row.get('image', ''), row.get('symbol', '').upper()
        mcap_val, price_val = row.get('market_cap', 0), row.get('current_price', 0)
        change_pct = row.get('price_change_percentage_24h', 0) or 0
        sparkline_prices = row.get('sparkline_in_7d', {}).get('price', [])
        is_positive = change_pct >= 0
        change_color, change_icon = ("#16B583", "▲") if is_positive else ("#FF5252", "▼")
        formatted_price = f"{currency_symbol}{price_val:,.4f}"
        formatted_mcap = format_market_cap(mcap_val, currency_symbol)
        sparkline_svg = generate_sparkline_svg(sparkline_prices, change_color)

        card_html = f"""
        <div style="display: grid; grid-template-columns: 4fr 2fr 3fr; align-items: center; padding: 10px; font-family: sans-serif; border-bottom: 1px solid #1E1E1E;">
            <div style="display: flex; align-items: center; gap: 12px;">
                <div style="color: #9E9E9E; width: 20px; text-align: left;">{rank}</div>
                <img src="{image_url}" width="36" height="36" style="border-radius: 50%;">
                <div><div style="font-weight: bold; font-size: 1.1em; color: #FFFFFF;">{symbol}</div><div style="font-size: 0.9em; color: #9E9E9E;">{formatted_mcap}</div></div>
            </div>
            <div style="text-align: right; font-weight: 500; font-size: 1.1em; color: #E0E0E0;">{formatted_price}</div>
            <div style="display: flex; justify-content: flex-end; align-items: center; gap: 10px;">
                <div style="width: 70px; height: 35px;">{sparkline_svg}</div>
                <div style="font-weight: bold; color: {change_color}; min-width: 65px; text-align:right;">{change_icon} {abs(change_pct):.2f}%</div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)


def render_custom_watchlist(market_data: pd.DataFrame, vs_currency: str):
    """カスタムウォッチリストを描画"""
    st.subheader("銘柄の追加")
    watchlist_df = get_watchlist_from_bq(USER_ID)
    
    existing_coin_ids = set(watchlist_df['coin_id'])
    
    # 銘柄追加フォーム
    with st.form("add_coin_form"):
        coin_options = {row['id']: f"{row['name']} ({row['symbol'].upper()})" for _, row in market_data.iterrows() if row['id'] not in existing_coin_ids}
        coins_to_add = st.multiselect("ウォッチリストに追加する銘柄を選択", options=coin_options.keys(), format_func=lambda x: coin_options[x])
        if st.form_submit_button("追加する"):
            if coins_to_add:
                add_to_watchlist_in_bq(USER_ID, coins_to_add)
                st.cache_data.clear() # BQキャッシュをクリア
                st.rerun()

    st.divider()

    if watchlist_df.empty:
        st.info("カスタムウォッチリストは空です。上のフォームから銘柄を追加してください。")
        return

    # 市場データとマージ
    watchlist_df = watchlist_df.merge(market_data, left_on='coin_id', right_on='id', how='left')
    watchlist_df.dropna(subset=['id'], inplace=True) # 市場データがないものは除外

    # 銘柄リスト表示
    for i, row in watchlist_df.iterrows():
        c1, c2, c3 = st.columns([8, 1, 1])
        with c1:
            # 既存のカード表示ロジックを再利用
            price_val = row.get(f'price_{vs_currency}', row.get('price_jpy', 0)) # 通貨対応
            change_pct = row.get('price_change_percentage_24h', 0) or 0
            is_positive = change_pct >= 0
            change_color, change_icon = ("#16B583", "▲") if is_positive else ("#FF5252", "▼")
            formatted_price = f"{CURRENCY_SYMBOLS.get(vs_currency, '$')}{price_val:,.4f}"

            card_html = f"""
            <div style="display: grid; grid-template-columns: 4fr 3fr 3fr; align-items: center; padding: 5px 0; font-family: sans-serif;">
                <div style="display: flex; align-items: center; gap: 12px;"><img src="{row['image']}" width="32" height="32" style="border-radius: 50%;"><div><div style="font-weight: bold; color: #FFFFFF;">{row['symbol'].upper()}</div><div style="font-size: 0.9em; color: #9E9E9E;">{row['name']}</div></div></div>
                <div style="text-align: right; font-weight: 500; color: #E0E0E0;">{formatted_price}</div>
                <div style="text-align: right; font-weight: bold; color: {change_color};">{change_icon} {abs(change_pct):.2f}%</div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)
        
        # 並び替え・削除ボタン
        with c2:
            if st.button("▲", key=f"up_{row['id']}", use_container_width=True, disabled=(i == 0)):
                current_ids = watchlist_df['coin_id'].tolist()
                current_ids.insert(i-1, current_ids.pop(i))
                update_watchlist_order_in_bq(USER_ID, current_ids)
                st.cache_data.clear()
                st.rerun()
            if st.button("▼", key=f"down_{row['id']}", use_container_width=True, disabled=(i == len(watchlist_df) - 1)):
                current_ids = watchlist_df['coin_id'].tolist()
                current_ids.insert(i+1, current_ids.pop(i))
                update_watchlist_order_in_bq(USER_ID, current_ids)
                st.cache_data.clear()
                st.rerun()
        with c3:
            if st.button("🗑️", key=f"del_{row['id']}", use_container_width=True):
                remove_from_watchlist_in_bq(USER_ID, row['id'])
                st.cache_data.clear()
                st.rerun()
        st.markdown("<hr style='margin: 2px 0; border-color: #222;'>", unsafe_allow_html=True)

def render_watchlist_page(market_data):
    """ウォッチリストページ全体を描画"""
    c1, _, c2, c3, c4 = st.columns([1.5, 0.5, 1.5, 1.5, 1])
    with c1: vs_currency = st.selectbox("Currency", options=["jpy", "usd"], format_func=lambda x: f"{x.upper()}", key="watchlist_currency", label_visibility="collapsed")
    with c3: st.button("24時間 % ▾", use_container_width=True, disabled=True)
    with c4: st.button("トップ100 ▾", use_container_width=True, disabled=True)
    
    # ヘッダー
    st.markdown("""
    <div style="display: grid; grid-template-columns: 4fr 2fr 3fr; align-items: center; padding: 0 10px; margin-top: 15px; font-size: 0.8em; color: #9E9E9E; font-family: sans-serif;">
        <span style="text-align: left;">#   時価総額</span><span style="text-align: right;">価格</span><span style="text-align: right;">24時間 %</span>
    </div><hr style="margin: 5px 0 10px 0; border-color: #333333;">""", unsafe_allow_html=True)

    tab_mcap, tab_custom = st.tabs(["時価総額ランキング", "カスタム"])

    with tab_mcap:
        render_market_cap_watchlist(market_data, vs_currency)
    with tab_custom:
        render_custom_watchlist(market_data, vs_currency)


# === 8. メイン処理 ===
def main():
    st.markdown(BLACK_THEME_CSS, unsafe_allow_html=True)
    st.session_state.setdefault('balance_hidden', False)
    st.session_state.setdefault('currency', 'jpy')
    
    if not bq_client: st.stop()

    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。")
        st.stop()
    
    # テーブル初期化
    init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
    init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)

    transactions_df = get_transactions_from_bq()
    usd_rate = get_exchange_rate('usd')

    # タブ設定
    portfolio_tab, watchlist_tab = st.tabs(["ポートフォリオ", "ウォッチリスト"])

    with portfolio_tab:
        current_currency = st.session_state.currency
        current_rate = usd_rate if current_currency == 'usd' else 1.0
        render_portfolio_page(transactions_df, market_data, currency=current_currency, rate=current_rate)

    with watchlist_tab:
        render_watchlist_page(market_data)

if __name__ == "__main__":
    main()
