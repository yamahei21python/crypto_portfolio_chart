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
- 時価総額ランキング（ウォッチリスト）の表示
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
TABLE_ID = "transactions"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

BIGQUERY_SCHEMA = [
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
    """Streamlit SecretsからGCPサービスアカウント情報を読み込み、BigQueryクライアントを初期化して返す。"""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    except (KeyError, FileNotFoundError):
        st.error("GCPサービスアカウントの認証情報が設定されていません。StreamlitのSecretsを確認してください。")
        return None

cg_client = CoinGeckoAPI()
bq_client = get_bigquery_client()


# === 4. BigQuery 操作関数 ===
def init_bigquery_table():
    """BigQueryに取引履歴テーブルが存在しない場合、スキーマに基づき新規作成します。"""
    if not bq_client: return
    try:
        bq_client.get_table(TABLE_FULL_ID)
    except google.api_core.exceptions.NotFound:
        table = bigquery.Table(TABLE_FULL_ID, schema=BIGQUERY_SCHEMA)
        bq_client.create_table(table)
        st.toast(f"BigQueryテーブル '{TABLE_ID}' を新規作成しました。")

def add_transaction_to_bq(transaction_data: Dict[str, Any]) -> bool:
    """取引データをBigQueryテーブルに追加します。"""
    if not bq_client: return False
    if isinstance(transaction_data["transaction_date"], datetime) and transaction_data["transaction_date"].tzinfo is None:
        transaction_data["transaction_date"] = transaction_data["transaction_date"].replace(tzinfo=timezone.utc)
    transaction_data["transaction_date"] = transaction_data["transaction_date"].isoformat()
    errors = bq_client.insert_rows_json(TABLE_FULL_ID, [transaction_data])
    if errors:
        st.error(f"データベースへの追加に失敗しました: {errors}")
        return False
    return True

def delete_transaction_from_bq(transaction: pd.Series) -> bool:
    """指定された取引履歴をBigQueryテーブルから削除します。"""
    if not bq_client: return False
    query = f"""
    DELETE FROM {TABLE_FULL_ID}
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
    """指定された取引履歴をBigQueryテーブルで更新します。"""
    if not bq_client: return False
    set_clauses, query_params = [], []
    for key, value in updated_data.items():
        set_clauses.append(f"{key} = @{key}")
        field_type = next((field.field_type for field in BIGQUERY_SCHEMA if field.name == key), "STRING")
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
    UPDATE {TABLE_FULL_ID} SET {set_sql}
    WHERE transaction_date = @where_transaction_date AND coin_id = @where_coin_id
    AND exchange = @where_exchange AND transaction_type = @where_transaction_type
    AND quantity = @where_quantity
    """
    job_config = bigquery.QueryJobConfig(query_parameters=query_params + where_params)
    try:
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        if query_job.num_dml_affected_rows is None or query_job.num_dml_affected_rows > 0:
            return True
        else:
            st.error("更新対象の履歴が見つかりませんでした。ページを再読み込みしてください。")
            return False
    except Exception as e:
        st.error(f"履歴の更新中にエラーが発生しました: {e}")
        return False

def get_transactions_from_bq() -> pd.DataFrame:
    """BigQueryから全ての取引履歴を取得し、表示用に整形したDataFrameを返します。"""
    if not bq_client: return pd.DataFrame()
    query = f"SELECT * FROM {TABLE_FULL_ID} ORDER BY transaction_date DESC"
    try:
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table()
        return pd.DataFrame()
    
    if df.empty:
        return pd.DataFrame()
        
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_convert('Asia/Tokyo')
    return df.rename(columns=COLUMN_NAME_MAP_JA)


# === 5. API & データ処理関数 ===
@st.cache_data(ttl=600)
def get_market_data() -> pd.DataFrame:
    """CoinGecko APIから時価総額上位50銘柄の市場データを取得します。"""
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=50, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'image', 'current_price', 'price_change_24h', 'price_change_percentage_24h', 'market_cap'])
        return df.rename(columns={'current_price': 'price_jpy', 'price_change_24h': 'price_change_24h_jpy'})
    except Exception as e:
        st.error(f"市場価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_exchange_rate(target_currency: str) -> float:
    """指定した通貨の対JPY為替レートを取得します。"""
    if target_currency.lower() == 'jpy': return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency.lower()}')
        return prices['bitcoin'][target_currency.lower()] / prices['bitcoin']['jpy']
    except Exception as e:
        st.warning(f"{target_currency.upper()}の為替レート取得に失敗しました: {e}")
        return 1.0

def calculate_portfolio(
    transactions_df: pd.DataFrame, price_map: Dict[str, float],
    price_change_map: Dict[str, float], name_map: Dict[str, str]
) -> Tuple[Dict, float, float]:
    """取引履歴から現在のポートフォリオ、総資産、24時間変動額を計算します。"""
    portfolio = {}
    total_asset_jpy, total_change_24h_jpy = 0.0, 0.0
    
    if transactions_df.empty:
        return portfolio, total_asset_jpy, total_change_24h_jpy

    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['登録種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity

        if current_quantity > 1e-9: # 浮動小数点誤差を考慮
            price = price_map.get(coin_id, 0)
            change_24h = price_change_map.get(coin_id, 0)
            value = current_quantity * price
            
            portfolio[(coin_id, exchange)] = {
                "コイン名": name_map.get(coin_id, coin_id), "取引所": exchange, "保有数量": current_quantity,
                "現在価格(JPY)": price, "評価額(JPY)": value, "コインID": coin_id
            }
            total_asset_jpy += value
            total_change_24h_jpy += current_quantity * change_24h
    return portfolio, total_asset_jpy, total_change_24h_jpy

def summarize_portfolio_by_coin(portfolio: Dict, market_data: pd.DataFrame) -> pd.DataFrame:
    """ポートフォリオデータをコインごとに集計し、市場データをマージします。"""
    if not portfolio:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)

    summary = df.groupby('コインID').agg(
        コイン名=('コイン名', 'first'),
        保有数量=('保有数量', 'sum'),
        評価額_jpy=('評価額(JPY)', 'sum'),
        アカウント数=('取引所', 'nunique')
    ).sort_values(by='評価額_jpy', ascending=False)

    # 修正: 'image'カラムもマージするように変更
    market_subset = market_data[['id', 'symbol', 'price_change_percentage_24h', 'image']].rename(columns={'id': 'コインID'})
    summary = summary.reset_index().merge(market_subset, on='コインID', how='left')
    
    summary['price_change_percentage_24h'] = summary['price_change_percentage_24h'].fillna(0)
    summary['symbol'] = summary['symbol'].fillna('')
    summary['image'] = summary['image'].fillna('') # imageがNaNになる場合への対応

    summary = summary[summary['保有数量'] > 1e-9]
    return summary

def summarize_portfolio_by_exchange(portfolio: Dict) -> pd.DataFrame:
    """ポートフォリオデータを取引所ごとに集計します。"""
    if not portfolio:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)

    summary = df.groupby('取引所').agg(
        評価額_jpy=('評価額(JPY)', 'sum'),
        コイン数=('コイン名', 'nunique')
    ).sort_values(by='評価額_jpy', ascending=False).reset_index()

    return summary

def calculate_btc_value(total_asset_jpy: float, price_map: Dict[str, float]) -> float:
    """総資産をBTC換算で計算します。"""
    btc_price_jpy = price_map.get('bitcoin', 0)
    return total_asset_jpy / btc_price_jpy if btc_price_jpy > 0 else 0.0


# === 6. UIコンポーネント & ヘルパー関数 ===
def format_market_cap(value: float, symbol: str) -> str:
    """数値をB(Billion)やM(Million)付きの文字列にフォーマットします。"""
    if value >= 1_000_000_000:
        return f"{symbol}{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{symbol}{value / 1_000_000:.2f}M"
    return f"{symbol}{value:,.0f}"

def generate_sparkline_svg(data: List[float], color: str = 'grey', width: int = 80, height: int = 35) -> str:
    """価格リストからスパークラインのSVG文字列を生成します。"""
    if not data or len(data) < 2:
        return ""
    min_val, max_val = min(data), max(data)
    range_val = max_val - min_val if max_val > min_val else 1
    
    points = []
    for i, d in enumerate(data):
        x = i * width / (len(data) - 1)
        y = height - ((d - min_val) / range_val * (height - 4)) - 2
        points.append(f"{x:.2f},{y:.2f}")

    path_d = "M " + " L ".join(points)
    return f'<svg width="{width}" height="{height}" viewbox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="overflow: visible;"><path d="{path_d}" stroke="{color}" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round" /></svg>'

def display_summary_card(total_asset_jpy: float, total_asset_btc: float, total_change_24h_jpy: float, currency: str, rate: float):
    """サマリーカードを動的な背景色で表示します。"""
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if is_hidden:
        asset_display, btc_display, change_display, pct_display = f"{CURRENCY_SYMBOLS[currency]} *******", "≈ ***** BTC", "*****", "**.**%"
        card_top_bg, card_bottom_bg, change_text_color = "#1E1E1E", "#2A2A2A", "#9E9E9E"
    else:
        yesterday_asset = total_asset_jpy - total_change_24h_jpy
        change_pct = (total_change_24h_jpy / yesterday_asset * 100) if yesterday_asset > 0 else 0
        symbol = CURRENCY_SYMBOLS[currency]
        is_positive = total_change_24h_jpy >= 0
        
        card_top_bg, card_bottom_bg = ("#16B583", "#129B72") if is_positive else ("#FF5252", "#E54A4A")
        change_text_color, change_sign = "#FFFFFF", "+" if is_positive else ""
        
        asset_display = f"{symbol}{(total_asset_jpy * rate):,.2f} {currency.upper()}"
        btc_display = f"≈ {total_asset_btc:.8f} BTC"
        change_display = f"{change_sign}{(total_change_24h_jpy * rate):,.2f} {currency.upper()}"
        pct_display = f"{change_sign}{change_pct:.2f}%"

    card_html = f"""
    <div style="border-radius: 10px; overflow: hidden; font-family: sans-serif;">
        <div style="padding: 20px; background-color: {card_top_bg};">
            <p style="font-size: 0.9em; margin: 0; padding: 0; color: #FFFFFF; opacity: 0.8;">残高</p>
            <p style="font-size: clamp(1.6em, 5vw, 2.2em); font-weight: bold; margin: 0; line-height: 1.2; color: #FFFFFF;">{asset_display}</p>
            <p style="font-size: clamp(0.9em, 2.5vw, 1.1em); font-weight: 500; margin-top: 5px; color: #FFFFFF; opacity: 0.9;">{btc_display}</p>
        </div>
        <div style="padding: 15px 20px; background-color: {card_bottom_bg}; display: flex; align-items: start;">
            <div style="flex-basis: 50%; min-width: 0;">
                <p style="font-size: 0.9em; margin: 0; color: #FFFFFF; opacity: 0.8;">24h 変動額</p>
                <p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {change_text_color};">{change_display}</p>
            </div>
            <div style="flex-basis: 50%; min-width: 0;">
                <p style="font-size: 0.9em; margin: 0; color: #FFFFFF; opacity: 0.8;">24h 変動率</p>
                <p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {change_text_color};">{pct_display}</p>
            </div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

def display_composition_bar(summary_df: pd.DataFrame):
    """資産構成バーと凡例を表示します。"""
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
        legend_parts.append(f"""
        <div style="display: flex; align-items: center; flex-shrink: 0;">
            <div style="width: 12px; height: 12px; background-color: {row["color"]}; border-radius: 3px; margin-right: 5px;"></div>
            <span style="font-size: clamp(0.75em, 2vw, 0.9em); color: #E0E0E0;">{display_text} {row["percentage"]:.2f}%</span>
        </div>""")
    legend_parts.append('</div>')
    st.markdown("".join(legend_parts), unsafe_allow_html=True)

    bar_html = '<div style="display: flex; width: 100%; height: 12px; border-radius: 5px; overflow: hidden; margin-top: 10px;">'
    for _, row in display_df.iterrows():
        bar_html += f'<div style="width: {row["percentage"]}%; background-color: {row["color"]};"></div>'
    st.markdown(bar_html + '</div>', unsafe_allow_html=True)

def display_asset_list_new(summary_df: pd.DataFrame, currency: str, rate: float):
    """保有資産をカード形式で表示します。"""
    st.subheader("保有資産")
    symbol = CURRENCY_SYMBOLS[currency]
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if summary_df.empty:
        st.info("保有資産はありません。")
        return

    for _, row in summary_df.iterrows():
        change_pct = row.get('price_change_percentage_24h', 0)
        is_positive = change_pct >= 0
        change_color = "#16B583" if is_positive else "#FF5252"
        change_sign = "▲" if is_positive else "▼"
        change_display = f"{abs(change_pct):.2f}%"
        image_url = row.get('image', '')
        
        price_per_unit = (row['評価額_jpy'] / row['保有数量']) * rate if row['保有数量'] > 0 else 0
        
        if is_hidden:
            quantity_display, value_display, price_display = "*****", f"{symbol}*****", f"{symbol}*****"
        else:
            quantity_display = f"{row['保有数量']:,.8f}".rstrip('0').rstrip('.')
            value_display = f"{symbol}{row['評価額_jpy'] * rate:,.2f}"
            price_display = f"{symbol}{price_per_unit:,.2f}"

        # 修正: アイコン表示を追加し、レイアウトを調整
        card_html = f"""
        <div style="background-color: #1E1E1E; border: 1px solid #444444; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">
            <div style="display: grid; grid-template-columns: 3fr 3fr 4fr; align-items: center; gap: 10px;">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <img src="{image_url}" width="36" height="36" style="border-radius: 50%;">
                    <div>
                        <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{row["symbol"].upper()}</p>
                        <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{row["アカウント数"]} 取引所</p>
                    </div>
                </div>
                <div style="text-align: right;">
                    <p style="font-size: clamp(0.9em, 2.2vw, 1em); font-weight: 500; margin: 0; color: #E0E0E0;">{quantity_display}</p>
                    <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{price_display}</p>
                </div>
                <div style="text-align: right;">
                    <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{value_display}</p>
                    <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: {change_color}; margin: 0;">{change_sign} {change_display}</p>
                </div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)

def display_exchange_list(summary_exchange_df: pd.DataFrame, currency: str, rate: float):
    """取引所別資産をカード形式で表示します。"""
    st.subheader("取引所別資産")
    symbol = CURRENCY_SYMBOLS[currency]
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if summary_exchange_df.empty:
        st.info("保有資産はありません。")
        return

    for _, row in summary_exchange_df.iterrows():
        value_display = f"{symbol}*****" if is_hidden else f"{symbol}{row['評価額_jpy'] * rate:,.2f}"
        card_html = f"""
        <div style="background-color: #1E1E1E; border: 1px solid #444444; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">🏦 {row["取引所"]}</p>
                    <p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #9E9E9E; margin: 0;">{row["コイン数"]} 銘柄</p>
                </div>
                <div style="text-align: right;">
                    <p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; color: #FFFFFF;">{value_display}</p>
                </div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)

def display_add_transaction_form(coin_options: Dict[str, str], name_map: Dict[str, str], currency: str):
    """新しい取引履歴を登録するためのフォームを表示します。"""
    with st.expander("新しい取引履歴を追加", expanded=False):
        with st.form(key=f"transaction_form_{currency}", clear_on_submit=True):
            st.subheader("履歴の登録")
            c1, c2, c3 = st.columns(3)
            with c1:
                date = st.date_input("取引日", datetime.now(), key=f"date_{currency}")
                coin_disp = st.selectbox("コイン", options=list(coin_options.keys()), key=f"coin_{currency}")
            with c2:
                trans_type = st.selectbox("種別", ["購入", "売却"], key=f"type_{currency}")
                exchange = st.selectbox("取引所", options=EXCHANGES_ORDERED, key=f"exchange_{currency}")
            with c3:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f", key=f"qty_{currency}")
                price = st.number_input("価格 (JPY)", min_value=0.0, format="%.2f", key=f"price_{currency}")
                fee = st.number_input("手数料 (JPY)", min_value=0.0, format="%.2f", key=f"fee_{currency}")
            
            if st.form_submit_button("この内容で登録する"):
                coin_id = coin_options[coin_disp]
                transaction = {
                    "transaction_date": datetime.combine(date, datetime.min.time()),
                    "coin_id": coin_id, "coin_name": name_map.get(coin_id, coin_disp.split(' ')[0]),
                    "exchange": exchange, "transaction_type": trans_type, "quantity": quantity,
                    "price_jpy": price, "fee_jpy": fee, "total_jpy": quantity * price
                }
                if add_transaction_to_bq(transaction):
                    st.success(f"{transaction['coin_name']}の{trans_type}履歴を登録しました。")
                    st.rerun()

def display_transaction_history(transactions_df: pd.DataFrame, currency: str):
    """登録履歴の一覧、編集、削除機能を表示します。"""
    st.subheader("🗒️ 登録履歴一覧")
    if transactions_df.empty:
        st.info("まだ登録履歴がありません。")
        return
    
    if 'edit_transaction_data' in st.session_state and st.session_state.get('edit_form_currency') == currency:
        _render_edit_form(transactions_df, currency)

    for index, row in transactions_df.iterrows():
        unique_key = f"{currency}_{index}"
        with st.container(border=True):
            cols = st.columns([4, 2])
            with cols[0]:
                st.markdown(f"**{row['コイン名']}** - {row['登録種別']}")
                st.caption(f"{row['登録日'].strftime('%Y/%m/%d')} | {row['取引所']}")
                st.text(f"数量: {row['数量']:.8f}".rstrip('0').rstrip('.'))
            with cols[1]:
                if st.button("編集", key=f"edit_{unique_key}", use_container_width=True):
                    st.session_state['edit_transaction_data'] = {'index': index, 'currency': currency}
                    st.rerun()
                if st.button("削除 🗑️", key=f"del_{unique_key}", use_container_width=True, help="この履歴を削除します"):
                    if delete_transaction_from_bq(row):
                        st.toast(f"履歴を削除しました: {row['登録日'].strftime('%Y/%m/%d')}の{row['コイン名']}", icon="🗑️")
                        if 'edit_transaction_data' in st.session_state:
                            del st.session_state['edit_transaction_data']
                        st.rerun()

def _render_edit_form(transactions_df: pd.DataFrame, currency: str):
    """履歴編集用のフォームを描画します。"""
    with st.container(border=True):
        st.subheader("登録履歴の編集")
        original_row = transactions_df.loc[st.session_state['edit_transaction_data']['index']]
        
        with st.form(key=f"edit_form_{currency}"):
            st.info(f"編集対象: {original_row['登録日'].strftime('%Y/%m/%d')} の {original_row['コイン名']} 取引")
            c1, c2 = st.columns(2)
            new_qty = c1.number_input("数量", value=original_row['数量'], min_value=0.0, format="%.8f")
            new_ex = c2.selectbox("取引所", options=EXCHANGES_ORDERED, index=EXCHANGES_ORDERED.index(original_row['取引所']) if original_row['取引所'] in EXCHANGES_ORDERED else 0)
            
            s_col, c_col = st.columns(2)
            if s_col.form_submit_button("更新する", use_container_width=True):
                updates = {}
                if not np.isclose(new_qty, original_row['数量']): updates['quantity'] = new_qty
                if new_ex != original_row['取引所']: updates['exchange'] = new_ex
                
                if updates and update_transaction_in_bq(original_row, updates):
                    st.toast("履歴を更新しました。", icon="✅")
                    del st.session_state['edit_transaction_data']
                    st.rerun()
                else:
                    st.toast("変更がありませんでした。", icon="ℹ️")
                    del st.session_state['edit_transaction_data']
                    st.rerun()
            
            if c_col.form_submit_button("キャンセル", use_container_width=True):
                del st.session_state['edit_transaction_data']
                st.rerun()
    st.markdown("---")


# === 7. ページ描画関数 ===
def render_portfolio_page(transactions_df: pd.DataFrame, market_data: pd.DataFrame, currency: str, rate: float):
    """ポートフォリオページの全コンポーネントを描画します。"""
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    portfolio, total_asset_jpy, total_change_jpy = calculate_portfolio(transactions_df, price_map, price_change_map, name_map)
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    summary_df = summarize_portfolio_by_coin(portfolio, market_data)
    summary_exchange_df = summarize_portfolio_by_exchange(portfolio)

    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        display_summary_card(total_asset_jpy, total_asset_btc, total_change_jpy, currency, rate)
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
            st.toast("最新の市場データに更新しました。", icon="🔄")
            st.rerun()
    
    st.divider()

    tab_coin, tab_exchange, tab_history = st.tabs(["コイン", "取引所", "履歴"])

    with tab_coin:
        display_composition_bar(summary_df)
        st.markdown("<br>", unsafe_allow_html=True) 
        display_asset_list_new(summary_df, currency, rate)

    with tab_exchange:
        display_exchange_list(summary_exchange_df, currency, rate)

    with tab_history:
        display_transaction_history(transactions_df, currency=currency)
        st.markdown("---")
        display_add_transaction_form(coin_options, name_map, currency=currency)

def render_watchlist_page():
    """ウォッチリスト（時価総額ランキング）ページを画像のような新しいUIで描画します。"""
    c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.5, 1])
    with c1:
        vs_currency = st.selectbox(
            "Currency", options=["usd", "jpy"],
            format_func=lambda x: f"{x.upper()} / BTC",
            key="watchlist_currency", label_visibility="collapsed"
        )
    with c2: st.button("24時間 % ▾", use_container_width=True, disabled=True)
    with c3: st.button("トップ100 ▾", use_container_width=True, disabled=True)
    with c4: st.button("🎚️", help="フィルター", use_container_width=True)

    @st.cache_data(ttl=600)
    def get_watchlist_data(currency: str) -> pd.DataFrame:
        try:
            data = cg_client.get_coins_markets(
                vs_currency=currency, order='market_cap_desc', per_page=100, page=1, sparkline=True
            )
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"ウォッチリストデータの取得に失敗しました: {e}")
            return pd.DataFrame()

    watchlist_df = get_watchlist_data(vs_currency)
    if watchlist_df.empty: return

    st.markdown("""
    <div style="display: grid; grid-template-columns: 4fr 2fr 3fr; align-items: center; padding: 0 10px; margin-top: 15px; font-size: 0.8em; color: #9E9E9E; font-family: sans-serif;">
        <span style="text-align: left;">#   時価総額</span>
        <span style="text-align: right;">価格</span>
        <span style="text-align: right;">24時間 %</span>
    </div>
    <hr style="margin: 5px 0 10px 0; border-color: #333333;">
    """, unsafe_allow_html=True)

    currency_symbol = CURRENCY_SYMBOLS.get(vs_currency, '$')
    for index, row in watchlist_df.iterrows():
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
                <div>
                    <div style="font-weight: bold; font-size: 1.1em; color: #FFFFFF;">{symbol}</div>
                    <div style="font-size: 0.9em; color: #9E9E9E;">{formatted_mcap}</div>
                </div>
            </div>
            <div style="text-align: right; font-weight: 500; font-size: 1.1em; color: #E0E0E0;">{formatted_price}</div>
            <div style="display: flex; justify-content: flex-end; align-items: center; gap: 10px;">
                <div style="width: 70px; height: 35px;">{sparkline_svg}</div>
                <div style="font-weight: bold; color: {change_color}; min-width: 65px; text-align:right;">{change_icon} {abs(change_pct):.2f}%</div>
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)


# === 8. メイン処理 ===
def main():
    """アプリケーションのメインエントリポイント。"""
    st.markdown(BLACK_THEME_CSS, unsafe_allow_html=True)
    
    st.session_state.setdefault('balance_hidden', False)
    st.session_state.setdefault('currency', 'jpy')
    
    if not bq_client: st.stop()

    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。時間をおいてページを再読み込みしてください。")
        st.stop()

    init_bigquery_table()
    transactions_df = get_transactions_from_bq()
    usd_rate = get_exchange_rate('usd')

    st.markdown("""
        <h1 style="font-size: 1.5em; display: inline-block; margin-right: 20px;">コイン</h1>
        <div style="display: inline-block; border-bottom: 3px solid #FFFFFF; padding-bottom: 5px;">
            <h2 style="font-size: 1.2em; display: inline-block; color: #FFFFFF; margin: 0;">ウォッチリスト一覧</h2>
        </div>
        <span style="font-size: 1.2em; color: #9E9E9E; margin-left: 20px;">DexScan</span>
        <span style="font-size: 1.2em; color: #9E9E9E; margin-left: 20px;">概要</span>
    """, unsafe_allow_html=True)

    portfolio_tab, watchlist_tab = st.tabs(["ポートフォリオ", "ウォッチリスト"])

    with portfolio_tab:
        current_currency = st.session_state.currency
        current_rate = usd_rate if current_currency == 'usd' else 1.0
        render_portfolio_page(transactions_df, market_data, currency=current_currency, rate=current_rate)

    with watchlist_tab:
        render_watchlist_page()

if __name__ == "__main__":
    main()
