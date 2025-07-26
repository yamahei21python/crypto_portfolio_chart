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
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, Tuple, TypedDict

# === 2. 定数・グローバル設定 ===
# --- BigQuery関連 ---
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_ID = "transactions"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# BigQueryテーブルスキーマ定義 (内部的な列名は変更しないこと)
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

# DataFrame表示用の列名マッピング（日本語UI用）
COLUMN_NAME_MAP_JA = {
    'transaction_date': '登録日', 'coin_name': 'コイン名', 'exchange': '取引所',
    'transaction_type': '登録種別', 'quantity': '数量', 'price_jpy': '価格(JPY)',
    'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)', 'coin_id': 'コインID'
}

# --- アプリケーションUI関連 ---
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
COIN_EMOJIS = {"Bitcoin": "₿", "Ethereum": "♦️", "Solana": "☀️", "XRP": "涟", "BNB": "🔶", "Dogecoin": "🐶", "Cardano": "C"}

# 資産の増減を判定するための登録種別
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']

# 取引所の表示順
EXCHANGES_ORDERED = ['SBIVC', 'BITPOINT', 'Binance', 'bitbank', 'GMOコイン', 'Bybit']

# ポートフォリオ円グラフ用の配色
COIN_COLORS = {
    "Bitcoin": "#F7931A", "Ethereum": "#627EEA", "Solana": "#9945FF", "XRP": "#00AAE4",
    "Tether": "#50AF95", "BNB": "#F3BA2F", "USD Coin": "#2775CA", "Dogecoin": "#C3A634",
    "Cardano": "#0033AD", "その他": "#D3D3D3"
}

# --- CSSスタイル ---
# DataFrameの数値を右寄せにするためのカスタムCSS
RIGHT_ALIGN_STYLE = """
<style>
    .right-align-table .stDataFrame [data-testid="stDataFrameData-row"] > div {
        text-align: right !important;
        justify-content: flex-end !important;
    }
</style>
"""

# === 3. 型定義 ===
# (型定義は今回は変更なし)


# === 4. 初期設定 & クライアント初期化 ===
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

# === 5. BigQuery 操作関数 ===
# (このセクションの関数は変更なし)
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

def reset_bigquery_table():
    """BigQueryテーブルの全データを削除します（TRUNCATE）。"""
    if not bq_client: return
    query = f"TRUNCATE TABLE {TABLE_FULL_ID}"
    try:
        bq_client.query(query).result()
        st.success("すべての取引履歴がリセットされました。")
    except Exception as e:
        st.error(f"データベースのリセット中にエラーが発生しました: {e}")

# === 6. API & データ処理関数 ===
@st.cache_data(ttl=600)
def get_market_data() -> pd.DataFrame:
    """CoinGecko APIから時価総額上位50銘柄の市場データを取得します。"""
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=50, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'current_price', 'price_change_24h', 'price_change_percentage_24h', 'market_cap'])
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

def format_currency(value: float, symbol: str, precision: int = 0) -> str:
    """数値を桁区切りと通貨記号付きの文字列にフォーマットします。"""
    return f"{symbol}{value:,.{precision}f}"

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

    # 市場データからシンボル(symbol)も取得するように修正
    market_subset = market_data[['id', 'symbol', 'price_change_percentage_24h']].rename(columns={'id': 'コインID'})
    summary = summary.reset_index().merge(market_subset, on='コインID', how='left')
    
    # マージ後にNaNになる可能性がある列を埋める
    summary['price_change_percentage_24h'] = summary['price_change_percentage_24h'].fillna(0)
    summary['symbol'] = summary['symbol'].fillna('') # シンボルが取得できない場合に備える

    # 保有数量がゼロに近いコインを除外
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

# === 7. UIコンポーネント関数 ===
def display_summary_card(total_asset_jpy: float, total_asset_btc: float, total_change_24h_jpy: float, currency: str, rate: float):
    """画像上部のサマリーカードを模したUIを表示します。"""
    
    is_hidden = st.session_state.get('balance_hidden', False)
    
    # --- 表示用データの準備 ---
    if is_hidden:
        asset_display = f"{CURRENCY_SYMBOLS[currency]} *******"
        btc_display = "≈ ***** BTC"
        change_display = "*****"
        pct_display = "**.**%"
        dynamic_color = "#DCE5E4" # 非表示時はニュートラルな色
    else:
        yesterday_asset = total_asset_jpy - total_change_24h_jpy
        change_pct = (total_change_24h_jpy / yesterday_asset * 100) if yesterday_asset > 0 else 0
        symbol = CURRENCY_SYMBOLS[currency]
        
        is_positive = total_change_24h_jpy >= 0
        change_sign = "+" if is_positive else ""
        pct_sign = "+" if is_positive else ""
        dynamic_color = "#99FF99" if is_positive else "#FF9999"

        asset_display = f"{symbol}{(total_asset_jpy * rate):,.2f} {currency.upper()}"
        btc_display = f"≈ {total_asset_btc:.8f} BTC"
        change_display = f"{change_sign}{(total_change_24h_jpy * rate):,.2f} {currency.upper()}"
        pct_display = f"{pct_sign}{change_pct:.2f}%"

    # --- HTMLを一行の文字列として生成 ---
    html_parts = [
        '<div style="border-radius: 10px; overflow: hidden; font-family: sans-serif;">',
            '<div style="padding: 20px 20px 20px 20px; color: white; background-color: #1A594F;">',
                '<p style="font-size: 0.9em; margin: 0; padding: 0; color: #A7C5C1;">残高</p>',
                f'<p style="font-size: clamp(1.6em, 5vw, 2.2em); font-weight: bold; margin: 0; padding: 0; line-height: 1.2; white-space: nowrap;">{asset_display}</p>',
                f'<p style="font-size: clamp(0.9em, 2.5vw, 1.1em); font-weight: 500; margin-top: 5px; color: #DCE5E4; white-space: nowrap;">{btc_display}</p>',
            '</div>',
            '<div style="padding: 15px 20px; background-color: #247565;">',
                '<div style="display: flex; align-items: start;">',
                    '<div style="flex-basis: 50%; min-width: 0;">',
                        '<p style="font-size: 0.9em; margin: 0; padding: 0; color: #A7C5C1;">24h 変動額</p>',
                        f'<p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {dynamic_color}; white-space: nowrap;">{change_display}</p>',
                    '</div>',
                    '<div style="flex-basis: 50%; min-width: 0;">',
                        '<p style="font-size: 0.9em; margin: 0; padding: 0; color: #A7C5C1;">24h 変動率</p>',
                        f'<p style="font-size: clamp(1em, 3vw, 1.2em); font-weight: 600; margin-top: 5px; color: {dynamic_color}; white-space: nowrap;">{pct_display}</p>',
                    '</div>',
                '</div>',
            '</div>',
        '</div>'
    ]
    card_html = "".join(html_parts)
    st.markdown(card_html, unsafe_allow_html=True)

def display_composition_bar(summary_df: pd.DataFrame):
    """資産構成を水平の積み上げ棒グラフで表示します。"""
    if summary_df.empty: return

    total_value = summary_df['評価額_jpy'].sum()
    if total_value <= 0: return

    # 上位N件 + その他で集計
    top_n = 5
    if len(summary_df) > top_n:
        top_df = summary_df.head(top_n).copy()
        other_value = summary_df.tail(len(summary_df) - top_n)['評価額_jpy'].sum()
        other_row = pd.DataFrame([{'コイン名': 'その他', '評価額_jpy': other_value}])
        display_df = pd.concat([top_df, other_row], ignore_index=True)
    else:
        display_df = summary_df.copy()

    display_df['percentage'] = (display_df['評価額_jpy'] / total_value) * 100
    display_df['color'] = display_df['コイン名'].map(COIN_COLORS).fillna("#D3D3D3") # 未定義のコイン色をグレーに

    # 凡例表示
    cols = st.columns(len(display_df))
    for i, row in display_df.iterrows():
        with cols[i]:
            st.markdown(f"""
            <div style="display: flex; align-items: center; font-size: 0.9em;">
                <div style="width: 12px; height: 12px; background-color: {row['color']}; border-radius: 3px; margin-right: 5px;"></div>
                <span>{row['コイン名']} {row['percentage']:.2f}%</span>
            </div>
            """, unsafe_allow_html=True)
            
    # HTMLで積み上げ棒グラフを作成
    bar_html = '<div style="display: flex; width: 100%; height: 12px; border-radius: 5px; overflow: hidden; margin-top: 10px;">'
    for _, row in display_df.iterrows():
        bar_html += f'<div style="width: {row["percentage"]}%; background-color: {row["color"]};"></div>'
    bar_html += '</div>'

    st.markdown(bar_html, unsafe_allow_html=True)

def display_asset_list_new(summary_df: pd.DataFrame, currency: str, rate: float):
    """保有資産をレスポンシブなカード形式で表示します。"""
    st.subheader("保有資産")
    symbol = CURRENCY_SYMBOLS[currency]
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if summary_df.empty:
        st.info("保有資産はありません。")
        return

    for _, row in summary_df.iterrows():
        # --- 表示用データの準備 ---
        change_pct = row.get('price_change_percentage_24h', 0)
        change_color = "#00BFA5" if change_pct >= 0 else "#FF5252"
        change_sign = "▲" if change_pct >= 0 else "▼"
        change_display = f"{abs(change_pct):.2f}%"
        
        price_per_unit = (row['評価額_jpy'] / row['保有数量']) * rate if row['保有数量'] > 0 else 0
        
        if is_hidden:
            quantity_display = "*****"
            value_display = f"{symbol}*****"
            price_display = f"{symbol}*****"
        else:
            quantity_display = f"{row['保有数量']:,.8f}".rstrip('0').rstrip('.')
            value_display = f"{symbol}{row['評価額_jpy'] * rate:,.2f}"
            price_display = f"{symbol}{price_per_unit:,.2f}"

        emoji = COIN_EMOJIS.get(row['コイン名'], '🪙')

        # --- HTMLを一行の文字列として生成 (grid-template-columns を修正) ---
        html_parts = [
            '<div style="border: 1px solid #31333F; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">',
                # ここを 2fr 3fr 5fr (20%, 30%, 50%) に変更
                '<div style="display: grid; grid-template-columns: 2fr 3fr 5fr; align-items: center; gap: 10px;">',
                    # 左列: 20%
                    '<div>',
                        f'<p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; padding: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{emoji} {row["symbol"].upper()}</p>',
                        f'<p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #808495; margin: 0; padding: 0;">{row["アカウント数"]} 取引所</p>',
                    '</div>',
                    # 中央列: 30%
                    '<div style="text-align: right;">',
                        f'<p style="font-size: clamp(0.9em, 2.2vw, 1em); font-weight: 500; margin: 0; padding: 0; white-space: nowrap;">{quantity_display}</p>',
                        f'<p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #808495; margin: 0; padding: 0; white-space: nowrap;">{price_display}</p>',
                    '</div>',
                    # 右列: 50%
                    '<div style="text-align: right;">',
                        f'<p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; padding: 0; white-space: nowrap;">{value_display}</p>',
                        f'<p style="font-size: clamp(0.8em, 2vw, 0.9em); color: {change_color}; margin: 0; padding: 0; white-space: nowrap;">{change_sign} {change_display}</p>',
                    '</div>',
                '</div>',
            '</div>'
        ]
        card_html = "".join(html_parts)
        st.markdown(card_html, unsafe_allow_html=True)


def display_exchange_list(summary_exchange_df: pd.DataFrame, currency: str, rate: float):
    """取引所別資産をレスポンシブなカード形式で表示します。"""
    st.subheader("取引所別資産")
    symbol = CURRENCY_SYMBOLS[currency]
    is_hidden = st.session_state.get('balance_hidden', False)
    
    if summary_exchange_df.empty:
        st.info("保有資産はありません。")
        return

    for _, row in summary_exchange_df.iterrows():
        # --- 表示用データの準備 ---
        if is_hidden:
            value_display = f"{symbol}*****"
        else:
            total_value = row['評価額_jpy'] * rate
            value_display = f"{symbol}{total_value:,.2f}"
            
        # --- HTMLを一行の文字列として生成 ---
        html_parts = [
            '<div style="border: 1px solid #31333F; border-radius: 10px; padding: 15px 20px; margin-bottom: 12px;">',
                '<div style="display: flex; justify-content: space-between; align-items: center;">',
                    '<div>',
                        f'<p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; padding: 0; white-space: nowrap;">🏦 {row["取引所"]}</p>',
                        f'<p style="font-size: clamp(0.8em, 2vw, 0.9em); color: #808495; margin: 0; padding: 0;">{row["コイン数"]} 銘柄</p>',
                    '</div>',
                    '<div style="text-align: right;">',
                        f'<p style="font-size: clamp(1em, 2.5vw, 1.1em); font-weight: bold; margin: 0; padding: 0; white-space: nowrap;">{value_display}</p>',
                    '</div>',
                '</div>',
            '</div>'
        ]
        card_html = "".join(html_parts)
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
    
    # 編集フォームの表示
    if 'edit_transaction_data' in st.session_state:
        # フォームのキーも通貨ごとにユニークにする
        if st.session_state.get('edit_form_currency') == currency:
            _render_edit_form(transactions_df, currency)

    # 履歴一覧の表示
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
                    st.session_state['edit_transaction_data'] = {'index': index}
                    st.session_state['edit_form_currency'] = currency
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
        edit_info = st.session_state['edit_transaction_data']
        original_row = transactions_df.loc[edit_info['index']]
        
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
                
                if updates:
                    if update_transaction_in_bq(original_row, updates):
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


# === 8. ページ描画関数 ===
def render_portfolio_page(transactions_df: pd.DataFrame, market_data: pd.DataFrame, currency: str, rate: float):
    """ポートフォリオページの全コンポーネントを描画します。"""
    # データを準備
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    portfolio, total_asset_jpy, total_change_jpy = calculate_portfolio(transactions_df, price_map, price_change_map, name_map)
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    summary_df = summarize_portfolio_by_coin(portfolio, market_data)
    summary_exchange_df = summarize_portfolio_by_exchange(portfolio)

    # UIコンポーネントを描画
    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        display_summary_card(total_asset_jpy, total_asset_btc, total_change_jpy, currency, rate)
    with col2:
        st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
        if st.button("👁️", key=f"toggle_visibility_{currency}", help="残高の表示/非表示"):
            st.session_state.balance_hidden = not st.session_state.get('balance_hidden', False)
            st.rerun()
        
        # 通貨切替ボタンのラベルを定義
        if currency == 'jpy':
            button_label = "USD"
            new_currency = 'usd'
        else:
            button_label = "JPY"
            new_currency = 'jpy'

        # 通貨切替ボタン
        if st.button(button_label, key=f"currency_toggle_main_{currency}"):
            st.session_state.currency = new_currency
            st.rerun()
            
        # データ更新ボタン
        if st.button("🔄", key=f"refresh_data_{currency}", help="市場価格を更新"):
            st.cache_data.clear()
            st.toast("最新の市場データに更新しました。", icon="🔄")
            st.rerun()

    st.divider()

    tab_coin, tab_exchange, tab_history = st.tabs(["コイン", "取引所", "履歴"])

    with tab_coin:
        display_composition_bar(summary_df)
        st.markdown("<br>", unsafe_allow_html=True) # 見た目のためのスペース
        display_asset_list_new(summary_df, currency, rate)

    with tab_exchange:
        display_exchange_list(summary_exchange_df, currency, rate)

    with tab_history:
        # キーが重複しないように、現在のページの通貨を渡す
        display_transaction_history(transactions_df, currency=currency)
        st.markdown("---")
        display_add_transaction_form(coin_options, name_map, currency=currency)

def render_watchlist_tab(market_data: pd.DataFrame, currency: str, rate: float):
    """ウォッチリスト（時価総額ランキング）タブを描画します。"""
    st.header(f"時価総額ランキング ({currency.upper()})")
    if 'market_cap' not in market_data.columns:
        st.warning("時価総額データが取得できませんでした。")
        return
    
    watchlist_df = market_data.copy().head(20) # 上位20に限定
    symbol = CURRENCY_SYMBOLS[currency]
    watchlist_df['現在価格'] = (watchlist_df['price_jpy'] * rate).apply(lambda x: format_currency(x, symbol, 4 if currency == 'jpy' else 2))
    watchlist_df['時価総額'] = (watchlist_df['market_cap'] * rate).apply(lambda x: format_currency(x, symbol))
    watchlist_df.rename(columns={'name': '銘柄', 'price_change_percentage_24h': '24h変動率'}, inplace=True)

    df_to_display = watchlist_df.sort_values(by='market_cap', ascending=False)[['銘柄', '現在価格', '時価総額', '24h変動率']]

    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    st.dataframe(
        df_to_display, hide_index=True, use_container_width=True,
        column_config={"24h変動率": st.column_config.NumberColumn(format="%.2f%%")}
    )
    st.markdown('</div>', unsafe_allow_html=True)

# === 9. メイン処理 ===
def main():
    """アプリケーションのメインエントリポイント。"""
    # セッション状態で管理する変数を初期化
    if 'balance_hidden' not in st.session_state:
        st.session_state.balance_hidden = False
    if 'currency' not in st.session_state:
        st.session_state.currency = 'jpy'
    
    st.markdown(RIGHT_ALIGN_STYLE, unsafe_allow_html=True)
    if not bq_client:
        st.stop()

    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。時間をおいてページを再読み込みしてください。")
        st.stop()

    init_bigquery_table()
    transactions_df = get_transactions_from_bq()
    usd_rate = get_exchange_rate('usd')

    main_tab, watchlist_tab = st.tabs(["ポートフォリオ", "ウォッチリスト"])

    with main_tab:
        # 現在の通貨状態に基づいて一度だけページを描画
        current_currency = st.session_state.currency
        current_rate = usd_rate if current_currency == 'usd' else 1.0
        render_portfolio_page(transactions_df, market_data, currency=current_currency, rate=current_rate)

    with watchlist_tab:
        # ウォッチリストは従来通りJPY/USDタブで切り替え
        jpy_watchlist_tab, usd_watchlist_tab = st.tabs(["JPY", "USD"])
        with jpy_watchlist_tab:
            render_watchlist_tab(market_data, currency='jpy', rate=1.0)
        with usd_watchlist_tab:
            render_watchlist_tab(market_data, currency='usd', rate=usd_rate)

if __name__ == "__main__":
    main()
