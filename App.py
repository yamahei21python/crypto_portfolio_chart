# -*- coding: utf-8 -*-
"""
仮想通貨ポートフォリオ管理Streamlitアプリケーション

このアプリケーションは、ユーザーの仮想通貨取引履歴を記録・管理し、
現在の資産状況を可視化するためのツールです。

主な機能:
- CoinGecko APIを利用したリアルタイム価格取得（手動更新機能付き）
- Google BigQueryをバックエンドとした取引履歴の永続化
- ポートフォリオの円グラフおよび資産一覧での可視化
- JPY建て、USD建てでの資産評価表示
- 取引履歴の追加、編集（数量調整）、削除
- 時価総額ランキング（ウォッチリスト）の表示
"""

# === 1. ライブラリのインポート ===
import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, List, Tuple, TypedDict


# === 2. 定数・グローバル設定 ===

# --- BigQuery関連 ---
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_ID = "transactions"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# BigQueryテーブルスキーマ定義
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

# --- アプリケーションUI関連 ---
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']
EXCHANGES_ORDERED = ['SBIVC', 'BITPOINT', 'Binance', 'bitbank', 'GMOコイン', 'Bybit']
# ポートフォリオ円グラフ用の配色
COIN_COLORS = {
    "Bitcoin": "#F7931A", "Ethereum": "#3C3C3D", "XRP": "#00AAE4",
    "Tether": "#50AF95", "BNB": "#F3BA2F", "Solana": "#9945FF",
    "USD Coin": "#2775CA", "Dogecoin": "#C3A634", "Cardano": "#0033AD",
    "TRON": "#EF0027", "Chainlink": "#2A5ADA", "Avalanche": "#E84142",
    "Shiba Inu": "#FFC001", "Polkadot": "#E6007A", "Bitcoin Cash": "#8DC351",
    "Toncoin": "#0098EA", "Polygon": "#8247E5", "Litecoin": "#345D9D",
    "NEAR Protocol": "#000000", "Internet Computer": "#3B00B9"
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
class Deltas(TypedDict):
    """24時間変動データ用の型定義。"""
    jpy_delta_str: str
    jpy_delta_color: str
    btc_delta_str: str
    btc_delta_color: str


# === 4. 初期設定 & クライアント初期化 ===

st.set_page_config(page_title="仮想通貨ポートフォリオ管理", page_icon="🪙", layout="wide")

@st.cache_resource
def get_bigquery_client() -> bigquery.Client | None:
    """
    Streamlit SecretsからGCPサービスアカウント情報を読み込み、
    BigQueryクライアントを初期化して返します。
    
    Returns:
        bigquery.Client | None: 成功した場合はBigQueryクライアント、失敗した場合はNone。
    """
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    except (KeyError, FileNotFoundError):
        st.error("BigQueryの認証情報が設定されていません。StreamlitのSecretsを確認してください。")
        return None

# APIクライアントのインスタンス化
cg_client = CoinGeckoAPI()
bq_client = get_bigquery_client()


# === 5. BigQuery 操作関数 ===

def init_bigquery_table():
    """
    BigQueryに取引履歴テーブルが存在しない場合、スキーマに基づき新規作成します。
    """
    if not bq_client: return
    try:
        bq_client.get_table(TABLE_FULL_ID)
    except google.api_core.exceptions.NotFound:
        table = bigquery.Table(TABLE_FULL_ID, schema=BIGQUERY_SCHEMA)
        bq_client.create_table(table)
        st.toast(f"BigQueryテーブル '{TABLE_ID}' を作成しました。")

def add_transaction_to_bq(transaction_data: Dict[str, Any]) -> bool:
    """
    取引データをBigQueryテーブルに追加します。
    日付データはUTCに変換してISOフォーマットで格納します。

    Args:
        transaction_data: 追加する取引データ（辞書形式）。

    Returns:
        bool: データの追加が成功した場合はTrue、失敗した場合はFalse。
    """
    if not bq_client: return False
    
    # タイムゾーン情報がない場合はUTCとして扱う
    if isinstance(transaction_data["transaction_date"], datetime) and transaction_data["transaction_date"].tzinfo is None:
        transaction_data["transaction_date"] = transaction_data["transaction_date"].replace(tzinfo=timezone.utc)
    
    # BigQueryのTIMESTAMP型に合わせてISO 8601形式に変換
    transaction_data["transaction_date"] = transaction_data["transaction_date"].isoformat()
    
    errors = bq_client.insert_rows_json(TABLE_FULL_ID, [transaction_data])
    if errors:
        st.error(f"データの追加に失敗しました: {errors}")
        return False
    return True

def delete_transaction_from_bq(transaction: pd.Series) -> bool:
    """
    指定された取引データをBigQueryテーブルから削除します。
    SQLインジェクションを防ぐため、パラメータ化クエリを使用します。

    Args:
        transaction: 削除対象の取引データ（pandas.Series）。

    Returns:
        bool: 削除が成功した場合はTrue、失敗した場合はFalse。
    """
    if not bq_client: return False
    query = f"""
        DELETE FROM `{TABLE_FULL_ID}`
        WHERE transaction_date = @transaction_date
          AND coin_id = @coin_id
          AND exchange = @exchange
          AND transaction_type = @transaction_type
          AND quantity = @quantity
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_date", "TIMESTAMP", transaction['取引日']),
            bigquery.ScalarQueryParameter("coin_id", "STRING", transaction['コインID']),
            bigquery.ScalarQueryParameter("exchange", "STRING", transaction['取引所']),
            bigquery.ScalarQueryParameter("transaction_type", "STRING", transaction['売買種別']),
            bigquery.ScalarQueryParameter("quantity", "FLOAT64", transaction['数量']),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"取引の削除中にエラーが発生しました: {e}")
        return False

def get_transactions_from_bq() -> pd.DataFrame:
    """
    BigQueryから全ての取引履歴を取得し、表示用に整形したDataFrameを返します。

    Returns:
        pd.DataFrame: 整形済みの取引履歴データ。
    """
    if not bq_client: return pd.DataFrame()
    
    query = f"SELECT * FROM `{TABLE_FULL_ID}` ORDER BY transaction_date DESC"
    try:
        # create_bqstorage_client=False は、環境による権限エラーを避けるための設定
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table()
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # タイムゾーンを日本時間に変換
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_convert('Asia/Tokyo')
    
    # 列名を日本語にリネーム
    rename_map = {
        'transaction_date': '取引日', 'coin_name': 'コイン名', 'exchange': '取引所',
        'transaction_type': '売買種別', 'quantity': '数量', 'price_jpy': '価格(JPY)',
        'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)', 'coin_id': 'コインID'
    }
    return df.rename(columns=rename_map)

def reset_bigquery_table():
    """BigQueryテーブルの全データを削除します（TRUNCATE）。"""
    if not bq_client: return
    query = f"TRUNCATE TABLE `{TABLE_FULL_ID}`"
    try:
        bq_client.query(query).result()
        st.success("すべての取引履歴がリセットされました。")
    except Exception as e:
        st.error(f"データベースのリセット中にエラーが発生しました: {e}")


# === 6. API & データ処理関数 ===

@st.cache_data(ttl=600)  # 10分間キャッシュ
def get_market_data() -> pd.DataFrame:
    """
    CoinGecko APIから仮想通貨の市場データを取得します。

    Returns:
        pd.DataFrame: 上位20銘柄の市場データ。
    """
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=20, page=1)
        df = pd.DataFrame(data, columns=[
            'id', 'symbol', 'name', 'current_price', 'price_change_24h', 
            'price_change_percentage_24h', 'market_cap'
        ])
        return df.rename(columns={'current_price': 'price_jpy', 'price_change_24h': 'price_change_24h_jpy'})
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)  # 10分間キャッシュ
def get_exchange_rate(target_currency: str) -> float:
    """
    指定された通貨の対JPY為替レートを取得します。

    Args:
        target_currency: 取得したい通貨のシンボル（例: 'usd'）。

    Returns:
        float: 対JPYの為替レート。
    """
    if target_currency.lower() == 'jpy':
        return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency.lower()}')
        jpy_price = prices['bitcoin']['jpy']
        target_price = prices['bitcoin'][target_currency.lower()]
        return target_price / jpy_price if jpy_price > 0 else 1.0
    except Exception as e:
        st.warning(f"為替レートの取得に失敗しました ({target_currency}): {e}")
        return 1.0

def format_currency(value: float, symbol: str, precision: int = 0) -> str:
    """
    数値を指定された通貨形式の文字列にフォーマットします。

    Args:
        value: フォーマットする数値。
        symbol: 通貨記号（例: '¥', '$'）。
        precision: 小数点以下の桁数。

    Returns:
        str: フォーマット済みの通貨文字列。
    """
    return f"{symbol}{value:,.{precision}f}"

def calculate_portfolio(
    transactions_df: pd.DataFrame, price_map: Dict[str, float],
    price_change_map: Dict[str, float], name_map: Dict[str, str]
) -> Tuple[Dict, float, float]:
    """
    取引履歴から現在のポートフォリオ、総資産、24時間変動額を計算します。

    Args:
        transactions_df: 全取引履歴データ。
        price_map: コインIDと現在価格(JPY)の辞書。
        price_change_map: コインIDと24時間価格変動(JPY)の辞書。
        name_map: コインIDとコイン名の辞書。

    Returns:
        Tuple[Dict, float, float]: (ポートフォリオ辞書, 総資産(JPY), 24時間総変動額(JPY))
    """
    portfolio = {}
    total_asset_jpy = 0.0
    total_change_24h_jpy = 0.0

    if transactions_df.empty:
        return portfolio, total_asset_jpy, total_change_24h_jpy

    # コインIDと取引所ごとにグループ化して保有数量を計算
    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity

        # 浮動小数点数の誤差を考慮し、ごくわずかな数量は無視
        if current_quantity > 1e-9:
            current_price_jpy = price_map.get(coin_id, 0)
            current_value_jpy = current_quantity * current_price_jpy
            change_24h_per_coin = price_change_map.get(coin_id, 0)
            asset_change_24h_jpy = current_quantity * change_24h_per_coin

            portfolio[(coin_id, exchange)] = {
                "コイン名": name_map.get(coin_id, coin_id),
                "取引所": exchange,
                "保有数量": current_quantity,
                "現在価格(JPY)": current_price_jpy,
                "評価額(JPY)": current_value_jpy,
                "コインID": coin_id
            }
            total_asset_jpy += current_value_jpy
            total_change_24h_jpy += asset_change_24h_jpy

    return portfolio, total_asset_jpy, total_change_24h_jpy

def calculate_btc_value(total_asset_jpy: float, price_map: Dict[str, float]) -> float:
    """総資産(JPY)を現在のBTC価格で換算します。"""
    btc_price_jpy = price_map.get('bitcoin', 0)
    return total_asset_jpy / btc_price_jpy if btc_price_jpy > 0 else 0.0

def calculate_deltas(
    total_asset_jpy: float, total_change_24h_jpy: float, rate: float,
    symbol: str, price_map: Dict, price_change_map: Dict
) -> Deltas:
    """
    総資産の24時間変動（対法定通貨、対BTC）を計算し、表示用文字列と色を返します。
    """
    # 法定通貨建ての変動計算
    display_total_change = total_change_24h_jpy * rate
    yesterday_asset_jpy = total_asset_jpy - total_change_24h_jpy
    change_pct = (total_change_24h_jpy / yesterday_asset_jpy * 100) if yesterday_asset_jpy > 0 else 0
    delta_display_str = f"{symbol}{display_total_change:,.2f} ({change_pct:+.2f}%)"
    jpy_delta_color = "green" if total_change_24h_jpy >= 0 else "red"

    # BTC建ての変動計算
    delta_btc_str, btc_delta_color = "N/A", "grey"
    btc_price_jpy = price_map.get('bitcoin', 0)
    if btc_price_jpy > 0:
        total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
        btc_change_24h_jpy = price_change_map.get('bitcoin', 0)
        btc_price_24h_ago_jpy = btc_price_jpy - btc_change_24h_jpy
        if btc_price_24h_ago_jpy > 0 and yesterday_asset_jpy > 0:
            total_asset_btc_24h_ago = yesterday_asset_jpy / btc_price_24h_ago_jpy
            change_btc = total_asset_btc - total_asset_btc_24h_ago
            change_btc_pct = (change_btc / total_asset_btc_24h_ago * 100) if total_asset_btc_24h_ago > 0 else 0
            delta_btc_str = f"{change_btc:+.8f} BTC ({change_btc_pct:+.2f}%)"
            btc_delta_color = "green" if change_btc >= 0 else "red"
    
    return {
        "jpy_delta_str": delta_display_str,
        "jpy_delta_color": jpy_delta_color,
        "btc_delta_str": delta_btc_str,
        "btc_delta_color": btc_delta_color
    }


# === 7. UIコンポーネント関数 ===

def display_asset_pie_chart(
    portfolio: Dict, rate: float, symbol: str, total_asset_jpy: float,
    total_asset_btc: float, deltas: Deltas
):
    """資産構成の円グラフ（ドーナツチャート）を表示します。"""
    st.subheader("📊 資産構成")
    if not portfolio:
        st.info("取引履歴を登録すると、ここにグラフが表示されます。")
        return

    # コインごとに評価額を集計
    pie_data = pd.DataFrame.from_dict(portfolio, orient='index')
    pie_data = pie_data.groupby("コイン名")["評価額(JPY)"].sum().reset_index()

    if pie_data.empty or pie_data["評価額(JPY)"].sum() <= 0:
        st.info("保有資産がありません。")
        return

    pie_data = pie_data.sort_values(by="評価額(JPY)", ascending=False)
    pie_data['評価額_display'] = pie_data['評価額(JPY)'] * rate

    fig = px.pie(
        pie_data, values='評価額_display', names='コイン名', color='コイン名',
        hole=0.5, color_discrete_map=COIN_COLORS
    )
    # グラフ内のテキスト表示設定
    fig.update_traces(
        textposition='inside',
        textinfo='text',
        texttemplate=f"%{{label}} (%{{percent}})<br>{symbol}%{{value:,.0f}}",
        textfont_size=12,
        marker=dict(line=dict(color='#FFFFFF', width=2)),
        direction='clockwise',
        rotation=0
    )
    
    # 中央に表示する総資産額とBTC換算額
    annotation_text = (
        f"<span style='font-size: 2.3em; color: {deltas['jpy_delta_color']};'>{symbol}{total_asset_jpy * rate:,.0f}</span><br><br>"
        f"<span style='font-size: 1.8em; color: {deltas['btc_delta_color']};'>{total_asset_btc:.4f} BTC</span>"
    )
    
    fig.update_layout(
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        showlegend=False,
        margin=dict(t=30, b=0, l=0, r=0),
        annotations=[dict(text=annotation_text, x=0.5, y=0.5, font_size=16, showarrow=False)]
    )
    st.plotly_chart(fig, use_container_width=True)

def display_asset_list(portfolio: Dict, currency: str, rate: float):
    """資産一覧をタブ（コイン別、取引所別、詳細）で表示します。"""
    st.subheader("📋 資産一覧")
    if not portfolio:
        st.info("保有資産はありません。")
        return

    portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    portfolio_df['評価額_display'] = portfolio_df['評価額(JPY)'] * rate
    
    tab_coin, tab_exchange, tab_detail = st.tabs(["コイン別", "取引所別", "詳細"])
    
    with tab_coin:
        _render_summary_by_coin(portfolio_df, currency, rate)
    with tab_exchange:
        _render_summary_by_exchange(portfolio_df, currency)
    with tab_detail:
        _render_detailed_portfolio(portfolio_df, currency, rate)

# --- 資産一覧のタブ内コンテンツをレンダリングする内部関数 ---

def _render_summary_by_coin(df: pd.DataFrame, currency: str, rate: float):
    """資産一覧（コイン別）タブをレンダリングします。"""
    summary_df = df.groupby("コイン名").agg(
        保有数量=('保有数量', 'sum'),
        評価額_display=('評価額_display', 'sum'),
        現在価格_jpy=('現在価格(JPY)', 'first')
    ).sort_values(by='評価額_display', ascending=False).reset_index()

    total_assets_display = summary_df['評価額_display'].sum()

    symbol = CURRENCY_SYMBOLS[currency]
    price_precision = 4 if currency == 'jpy' else 2
    
    # 表示用の列をフォーマット
    summary_df['評価額'] = summary_df['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
    
    if total_assets_display > 0:
        summary_df['割合'] = (summary_df['評価額_display'] / total_assets_display) * 100
    else:
        summary_df['割合'] = 0

    summary_df['現在価格'] = (summary_df['現在価格_jpy'] * rate).apply(lambda x: format_currency(x, symbol, price_precision))
    summary_df['保有数量'] = summary_df['保有数量'].apply(lambda x: f"{x:,.8f}".rstrip('0').rstrip('.'))
    
    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    st.dataframe(
        summary_df[['コイン名', '保有数量', '評価額', '割合', '現在価格']],
        column_config={
            "コイン名": "コイン名", 
            "保有数量": "保有数量",
            "評価額": f"評価額 ({currency.upper()})",
            "割合": st.column_config.NumberColumn("割合", format="%.2f%%"),
            "現在価格": f"現在価格 ({currency.upper()})"
        },
        hide_index=True, use_container_width=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

def _render_summary_by_exchange(df: pd.DataFrame, currency: str):
    """資産一覧（取引所別）タブをレンダリングします。"""
    summary_df = df.groupby("取引所")['評価額_display'].sum().sort_values(ascending=False).reset_index()
    
    total_assets_display = summary_df['評価額_display'].sum()

    symbol = CURRENCY_SYMBOLS[currency]
    summary_df['評価額'] = summary_df['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
    
    if total_assets_display > 0:
        summary_df['割合'] = (summary_df['評価額_display'] / total_assets_display) * 100
    else:
        summary_df['割合'] = 0

    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    st.dataframe(
        summary_df[['取引所', '評価額', '割合']],
        column_config={
            "取引所": "取引所", 
            "評価額": f"評価額 ({currency.upper()})",
            "割合": st.column_config.NumberColumn("割合", format="%.2f%%"),
        },
        hide_index=True, use_container_width=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

def _render_detailed_portfolio(df: pd.DataFrame, currency: str, rate: float):
    """資産一覧（詳細）タブをレンダリングし、数量の直接編集機能を提供します。"""
    display_df = df.copy().sort_values(by='評価額_display', ascending=False)
    symbol = CURRENCY_SYMBOLS[currency]
    price_precision = 4 if currency == 'jpy' else 2
    
    # 表示用の列をフォーマット
    display_df['現在価格_display'] = display_df['現在価格(JPY)'] * rate
    display_df['評価額_formatted'] = display_df['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
    display_df['現在価格_formatted'] = display_df['現在価格_display'].apply(lambda x: format_currency(x, symbol, price_precision))

    # st.data_editorの変更を検知するため、編集前の状態をセッションに保存
    session_key = f'before_edit_df_{currency}'
    if session_key not in st.session_state or not st.session_state[session_key].equals(display_df):
        st.session_state[session_key] = display_df.copy()

    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    edited_df = st.data_editor(
        display_df[['コイン名', '取引所', '保有数量', '評価額_formatted', '現在価格_formatted']],
        disabled=['コイン名', '取引所', '評価額_formatted', '現在価格_formatted'],
        column_config={
            "コイン名": "コイン名", "取引所": "取引所",
            "保有数量": st.column_config.NumberColumn("保有数量", format="%.8f"),
            "評価額_formatted": f"評価額 ({currency.upper()})",
            "現在価格_formatted": f"現在価格 ({currency.upper()})"
        },
        use_container_width=True,
        key=f"portfolio_editor_{currency}",
        hide_index=True
    )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 数量が変更されたかチェック
    if not edited_df['保有数量'].equals(st.session_state[session_key]['保有数量']):
        merged_df = pd.merge(st.session_state[session_key], edited_df, on=['コイン名', '取引所'], suffixes=('_before', '_after'))
        # 変更があった行をイテレート
        for _, row in merged_df.iterrows():
            if not np.isclose(row['保有数量_before'], row['保有数量_after']):
                quantity_diff = row['保有数量_after'] - row['保有数量_before']
                transaction_type = "調整（増）" if quantity_diff > 0 else "調整（減）"
                # 「調整」取引としてBigQueryに記録
                transaction = {
                    "transaction_date": datetime.now(timezone.utc),
                    "coin_id": row['コインID'],
                    "coin_name": row['コイン名'],
                    "exchange": row['取引所'],
                    "transaction_type": transaction_type,
                    "quantity": abs(quantity_diff),
                    "price_jpy": 0, "fee_jpy": 0, "total_jpy": 0
                }
                if add_transaction_to_bq(transaction):
                    st.toast(f"{row['コイン名']} ({row['取引所']}) の数量を調整: {quantity_diff:+.8f}", icon="✍️")
        
        del st.session_state[session_key]  # 処理後にセッションステートをクリア
        st.rerun()

def display_transaction_form(coin_options: Dict[str, str], name_map: Dict[str, str], currency: str):
    """新しい取引を登録するためのフォームを表示します。"""
    with st.expander("取引履歴の登録", expanded=False):
        with st.form(key=f"transaction_form_{currency}", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            c1, c2, c3 = st.columns(3)
            with c1:
                transaction_date = st.date_input("取引日", datetime.now(), key=f"date_{currency}")
                selected_coin_disp_name = st.selectbox("コイン種別", options=list(coin_options.keys()), key=f"coin_{currency}")
            with c2:
                transaction_type = st.selectbox("売買種別", ["購入", "売却"], key=f"type_{currency}")
                exchange = st.selectbox("取引所", options=EXCHANGES_ORDERED, index=0, key=f"exchange_{currency}")
            with c3:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f", key=f"qty_{currency}")
                price = st.number_input("価格(JPY)", min_value=0.0, format="%.2f", key=f"price_{currency}")
                fee = st.number_input("手数料(JPY)", min_value=0.0, format="%.2f", key=f"fee_{currency}")
            
            if st.form_submit_button("登録する"):
                coin_id = coin_options[selected_coin_disp_name]
                transaction = {
                    "transaction_date": datetime.combine(transaction_date, datetime.min.time()),
                    "coin_id": coin_id,
                    "coin_name": name_map.get(coin_id, selected_coin_disp_name.split(' ')[0]),
                    "exchange": exchange,
                    "transaction_type": transaction_type,
                    "quantity": quantity,
                    "price_jpy": price,
                    "fee_jpy": fee,
                    "total_jpy": quantity * price
                }
                if add_transaction_to_bq(transaction):
                    st.success(f"{transaction['coin_name']}の{transaction_type}取引を登録しました。")
                    st.rerun()

def display_transaction_history(transactions_df: pd.DataFrame, currency: str):
    """取引履歴の一覧と削除ボタンを表示します。"""
    st.subheader("🗒️ 取引履歴")
    if transactions_df.empty:
        st.info("まだ取引履歴がありません。")
        return
    
    # ヘッダー表示
    cols = st.columns([3, 2, 2, 2, 2, 1])
    headers = ["取引日時", "コイン名", "取引所", "売買種別", "数量", "操作"]
    for col, header in zip(cols, headers):
        col.markdown(f"**{header}**")
    
    # 履歴を1行ずつ表示
    for _, row in transactions_df.iterrows():
        unique_key = f"delete_{currency}_{row['取引日'].timestamp()}_{row['コインID']}_{row['数量']}"
        cols = st.columns([3, 2, 2, 2, 2, 1])
        cols[0].text(row['取引日'].strftime('%Y/%m/%d %H:%M'))
        cols[1].text(row['コイン名'])
        cols[2].text(row['取引所'])
        cols[3].text(row['売買種別'])
        cols[4].text(f"{row['数量']:.8f}".rstrip('0').rstrip('.'))
        if cols[5].button("削除", key=unique_key):
            if delete_transaction_from_bq(row):
                st.toast(f"取引を削除しました: {row['取引日'].strftime('%Y/%m/%d')}の{row['コイン名']}取引", icon="🗑️")
                st.rerun()

def display_database_management(currency: str):
    """データベースリセット（全データ削除）機能を表示します。"""
    st.subheader("⚙️ データベース管理")
    confirm_key = f'confirm_delete_{currency}'
    if confirm_key not in st.session_state:
        st.session_state[confirm_key] = False

    with st.expander("データベースリセット（危険）"):
        st.warning("**警告**: この操作はデータベース上のすべての取引履歴を完全に削除します。この操作は取り消せません。")
        
        # 削除ボタンが押された後の確認ステップ
        if st.session_state[confirm_key]:
            st.error("本当によろしいですか？最終確認です。")
            c1, c2 = st.columns(2)
            if c1.button("はい、すべてのデータを削除します", type="primary", key=f"confirm_delete_button_{currency}"):
                reset_bigquery_table()
                st.session_state[confirm_key] = False
                st.rerun()
            if c2.button("いいえ、キャンセルします", key=f"cancel_delete_button_{currency}"):
                st.session_state[confirm_key] = False
                st.rerun()
        # 初回の削除ボタン
        else:
            if st.button("すべての取引履歴をリセットする", key=f"reset_button_{currency}"):
                st.session_state[confirm_key] = True
                st.rerun()


# === 8. ページ描画関数 ===

def render_portfolio_page(transactions_df: pd.DataFrame, market_data: pd.DataFrame, currency: str, rate: float):
    """ポートフォリオページの全コンポーネントを描画します。"""
    symbol = CURRENCY_SYMBOLS[currency]
    
    # データ準備
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    # ポートフォリオ計算
    portfolio, total_asset_jpy, total_change_24h_jpy = calculate_portfolio(transactions_df, price_map, price_change_map, name_map)
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    deltas = calculate_deltas(total_asset_jpy, total_change_24h_jpy, rate, symbol, price_map, price_change_map)

    # UIレイアウト
    c1, c2 = st.columns([1, 1.2])
    with c1:
        display_asset_pie_chart(portfolio, rate, symbol, total_asset_jpy, total_asset_btc, deltas)
        st.markdown(f"""
        <div style="text-align: center; margin-top: 5px; line-height: 1.4;">
            <span style="font-size: 1.0rem; color: {deltas['jpy_delta_color']};">{deltas['jpy_delta_str']}</span>
            <span style="font-size: 1.0rem; color: {deltas['btc_delta_color']}; margin-left: 12px;">{deltas['btc_delta_str']}</span>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        display_asset_list(portfolio, currency, rate)
    
    st.markdown("---")
    display_transaction_form(coin_options, name_map, currency)
    display_transaction_history(transactions_df, currency)
    st.markdown("---")
    display_database_management(currency)

def render_watchlist_tab(market_data: pd.DataFrame, currency: str, rate: float):
    """ウォッチリスト（時価総額ランキング）タブを描画します。"""
    st.header(f"時価総額トップ20 ({currency.upper()})")
    
    if 'market_cap' not in market_data.columns:
        st.warning("時価総額データが取得できませんでした。")
        return
        
    watchlist_df = market_data.copy()
    symbol = CURRENCY_SYMBOLS[currency]
    price_precision = 4 if currency == 'jpy' else 2

    # 表示用データを整形
    watchlist_df['現在価格'] = (watchlist_df['price_jpy'] * rate).apply(lambda x: format_currency(x, symbol, price_precision))
    watchlist_df['時価総額'] = (watchlist_df['market_cap'] * rate).apply(lambda x: format_currency(x, symbol, 0))
    watchlist_df.rename(columns={'name': '銘柄', 'price_change_percentage_24h': '24h変動率'}, inplace=True)

    df_to_display = watchlist_df.sort_values(by='market_cap', ascending=False)[['銘柄', '現在価格', '時価総額', '24h変動率']]
    # DataFrameの高さを行数に応じて動的に調整
    height = (len(df_to_display) + 1) * 35 + 3
    
    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    st.dataframe(
        df_to_display,
        hide_index=True,
        use_container_width=True,
        height=height,
        column_config={
            "銘柄": "銘柄",
            "現在価格": f"現在価格 ({currency.upper()})",
            "時価総額": f"時価総額 ({currency.upper()})",
            "24h変動率": st.column_config.NumberColumn("24h変動率 (%)", format="%.2f%%")
        }
    )
    st.markdown('</div>', unsafe_allow_html=True)


# === 9. メイン処理 ===

def main():
    """アプリケーションのメインエントリポイント。"""
    # ★★★★★ ここからが変更箇所 ★★★★★
    
    # --- ページタイトルと更新ボタン ---
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("🪙 仮想通貨ポートフォリオ管理アプリ")
    with col2:
        # ボタンを垂直方向に中央揃えするためのスペーサー
        st.markdown("<div style='margin-top: 25px;'></div>", unsafe_allow_html=True)
        if st.button("🔄 データ更新", use_container_width=True, help="市場価格や為替レートを最新の情報に更新します。"):
            # st.cache_dataとst.cache_resourceでキャッシュされたデータをすべてクリア
            st.cache_data.clear()
            st.cache_resource.clear()
            st.toast("最新の市場データに更新しました。", icon="🔄")
            st.rerun()

    st.markdown(RIGHT_ALIGN_STYLE, unsafe_allow_html=True)
    
    # ★★★★★ ここまでが変更箇所 ★★★★★
    
    # BigQueryクライアントがなければ処理を停止
    if not bq_client:
        st.stop()

    # 必須データの取得
    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。しばらくしてから再読み込みしてください。")
        st.stop()

    # DB初期化とデータ取得
    init_bigquery_table()
    transactions_df = get_transactions_from_bq()
    
    # 為替レート取得
    usd_rate = get_exchange_rate('usd')

    # タブUIの作成
    tab_pf_jpy, tab_wl_jpy, tab_pf_usd, tab_wl_usd = st.tabs([
        "ポートフォリオ (JPY)", "ウォッチリスト (JPY)", 
        "ポートフォリオ (USD)", "ウォッチリスト (USD)"
    ])

    with tab_pf_jpy:
        render_portfolio_page(transactions_df, market_data, currency='jpy', rate=1.0)
    with tab_wl_jpy:
        render_watchlist_tab(market_data, currency='jpy', rate=1.0)
    with tab_pf_usd:
        render_portfolio_page(transactions_df, market_data, currency='usd', rate=usd_rate)
    with tab_wl_usd:
        render_watchlist_tab(market_data, currency='usd', rate=usd_rate)

if __name__ == "__main__":
    main()
