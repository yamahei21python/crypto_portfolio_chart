import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, List

# --- 定数定義 ---
# BigQuery関連
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_ID = "transactions"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# BigQueryテーブルスキーマ
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

# アプリケーション関連
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']


# --- 初期設定 & クライアント初期化 ---
st.set_page_config(page_title="仮想通貨ポートフォリオ管理", page_icon="🪙", layout="wide")

@st.cache_resource
def get_bigquery_client():
    """StreamlitのSecretsを使い、BigQueryクライアントを初期化して返す"""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    except (KeyError, FileNotFoundError):
        st.error("BigQueryの認証情報が設定されていません。StreamlitのSecretsを確認してください。")
        return None

# グローバルクライアント
cg_client = CoinGeckoAPI()
bq_client = get_bigquery_client()


# --- BigQuery関連関数 ---
def init_bigquery_table():
    """BigQueryにテーブルが存在しない場合に初期化する"""
    if not bq_client: return
    try:
        bq_client.get_table(TABLE_FULL_ID)
    except google.api_core.exceptions.NotFound:
        table = bigquery.Table(TABLE_FULL_ID, schema=BIGQUERY_SCHEMA)
        bq_client.create_table(table)
        st.toast(f"BigQueryテーブル '{TABLE_ID}' を作成しました。")

def add_transaction_to_bq(transaction_data: Dict[str, Any]):
    """取引データをBigQueryに追加する"""
    if not bq_client: return
    # タイムゾーン情報がない場合はUTCを付与
    if isinstance(transaction_data["transaction_date"], datetime) and transaction_data["transaction_date"].tzinfo is None:
        transaction_data["transaction_date"] = transaction_data["transaction_date"].replace(tzinfo=timezone.utc)
    
    # BigQueryはISOフォーマットの文字列を要求する
    transaction_data["transaction_date"] = transaction_data["transaction_date"].isoformat()

    errors = bq_client.insert_rows_json(TABLE_FULL_ID, [transaction_data])
    if errors:
        st.error(f"データの追加に失敗しました: {errors}")
        return False
    return True

def get_transactions_from_bq() -> pd.DataFrame:
    """BigQueryから全取引履歴を取得し、DataFrameとして返す"""
    if not bq_client: return pd.DataFrame()
    query = f"SELECT * FROM `{TABLE_FULL_ID}` ORDER BY transaction_date DESC"
    try:
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table()
        return pd.DataFrame()
    
    if not df.empty:
        # 列名を日本語にリネーム
        rename_map = {
            'transaction_date': '取引日', 'coin_id': 'コインID', 'coin_name': 'コイン名',
            'exchange': '取引所', 'transaction_type': '売買種別', 'quantity': '数量',
            'price_jpy': '価格(JPY)', 'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)'
        }
        df = df.rename(columns=rename_map)
        # タイムゾーンを日本時間に変換して表示
        df['取引日'] = df['取引日'].dt.tz_convert('Asia/Tokyo')
    return df

def reset_bigquery_table():
    """BigQueryテーブルの全データを削除する"""
    if not bq_client: return
    query = f"TRUNCATE TABLE `{TABLE_FULL_ID}`"
    try:
        bq_client.query(query).result()
        st.success("すべての取引履歴がリセットされました。")
    except Exception as e:
        st.error(f"データベースのリセット中にエラーが発生しました: {e}")


# --- API関連 & データ取得関数 ---
@st.cache_data(ttl=600)
def get_market_data() -> pd.DataFrame:
    """CoinGecko APIから仮想通貨の市場データを取得する"""
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=50, page=1)
        df = pd.DataFrame(data, columns=[
            'id', 'symbol', 'name', 'current_price',
            'price_change_24h', 'price_change_percentage_24h'
        ])
        return df.rename(columns={
            'current_price': 'price_jpy',
            'price_change_24h': 'price_change_24h_jpy'
        })
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_exchange_rate(target_currency: str) -> float:
    """JPYを基準とした指定通貨の為替レートを取得する"""
    if target_currency == 'jpy':
        return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency}')
        jpy_price = prices['bitcoin']['jpy']
        target_price = prices['bitcoin'][target_currency]
        return target_price / jpy_price if jpy_price > 0 else 1.0
    except Exception as e:
        st.warning(f"為替レートの取得に失敗しました: {e}")
        return 1.0


# --- データ処理関数 ---
def calculate_portfolio(transactions_df: pd.DataFrame, price_map: Dict, price_change_map: Dict, name_map: Dict) -> (Dict, float, float):
    """取引履歴から現在のポートフォリオ、総資産額、24時間変動額を計算する"""
    portfolio = {}
    total_asset_jpy = 0
    total_change_24h_jpy = 0

    if transactions_df.empty:
        return portfolio, total_asset_jpy, total_change_24h_jpy

    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity

        if current_quantity > 1e-9: # 浮動小数点誤差を考慮
            current_price_jpy = price_map.get(coin_id, 0)
            current_value_jpy = current_quantity * current_price_jpy
            
            change_24h_per_coin = price_change_map.get(coin_id, 0)
            asset_change_24h_jpy = current_quantity * change_24h_per_coin
            
            portfolio[(coin_id, exchange)] = {
                "コイン名": name_map.get(coin_id, coin_id),
                "取引所": exchange,
                "保有数量": current_quantity,
                "現在価格(JPY)": current_price_jpy,
                "評価額(JPY)": current_value_jpy
            }
            total_asset_jpy += current_value_jpy
            total_change_24h_jpy += asset_change_24h_jpy
            
    return portfolio, total_asset_jpy, total_change_24h_jpy


# --- UI描画関数 ---
# ▼▼▼▼▼【変更箇所】▼▼▼▼▼
def display_summary(total_asset_jpy: float, total_change_24h_jpy: float, currency: str, rate: float, symbol: str, price_map: Dict, price_change_map: Dict):
    """ポートフォリオのサマリーメトリクスを表示する"""
    st.header("📈 ポートフォリオサマリー")
    
    # --- 通貨建ての計算 ---
    display_total_asset = total_asset_jpy * rate
    display_total_change = total_change_24h_jpy * rate
    yesterday_asset_jpy = total_asset_jpy - total_change_24h_jpy
    change_pct = (total_change_24h_jpy / yesterday_asset_jpy * 100) if yesterday_asset_jpy > 0 else 0
    delta_display_str = f"{symbol}{display_total_change:,.2f} ({change_pct:+.2f}%)"

    # --- BTC建ての計算 ---
    total_asset_btc = 0
    delta_btc_str = "N/A"

    btc_price_jpy = price_map.get('bitcoin', 0)
    if btc_price_jpy > 0:
        # 現在のBTC建て総資産
        total_asset_btc = total_asset_jpy / btc_price_jpy
        
        # 24時間前のBTC価格を計算
        btc_change_24h_jpy = price_change_map.get('bitcoin', 0)
        btc_price_24h_ago_jpy = btc_price_jpy - btc_change_24h_jpy
        
        # 24時間前のBTC建て総資産を計算
        if btc_price_24h_ago_jpy > 0 and yesterday_asset_jpy > 0:
            total_asset_btc_24h_ago = yesterday_asset_jpy / btc_price_24h_ago_jpy
            change_btc = total_asset_btc - total_asset_btc_24h_ago
            
            # 変動率を計算
            if total_asset_btc_24h_ago > 0:
                change_btc_pct = (change_btc / total_asset_btc_24h_ago) * 100
                delta_btc_str = f"{change_btc:+.8f} BTC ({change_btc_pct:+.2f}%)"
            else: # 24時間前が0で現在資産がある場合
                delta_btc_str = f"{change_btc:+.8f} BTC"
    
    # --- メトリクス表示 ---
    col1, col2 = st.columns(2)
    with col1:
        col1.metric(
            label=f"保有資産合計 ({currency.upper()})",
            value=f"{symbol}{display_total_asset:,.2f}",
            delta=delta_display_str
        )
    with col2:
        col2.metric(
            label="保有資産合計 (BTC)",
            value=f"{total_asset_btc:.8f} BTC",
            delta=delta_btc_str
        )
# ▲▲▲▲▲【変更箇所】▲▲▲▲▲

def display_asset_pie_chart(portfolio: Dict, rate: float, symbol: str):
    """資産割合の円グラフを表示する"""
    st.subheader("📊 資産割合 (コイン別)")
    if not portfolio:
        st.info("取引履歴を登録すると、ここにグラフが表示されます。")
        return

    pie_data = pd.DataFrame.from_dict(portfolio, orient='index').groupby("コイン名")["評価額(JPY)"].sum().reset_index()
    
    if pie_data.empty or pie_data["評価額(JPY)"].sum() <= 0:
        st.info("保有資産がありません。")
        return

    pie_data['評価額_display'] = pie_data['評価額(JPY)'] * rate
    fig = px.pie(
        pie_data, 
        values='評価額_display',
        names='コイン名', 
        hole=0.3,
        title="コイン別資産構成"
    )
    fig.update_traces(
        textposition='inside',
        texttemplate=f'%{{label}}<br>%{{percent}}<br>{symbol}%{{value:,.0f}}'
    )
    fig.update_layout(
        uniformtext_minsize=12, 
        uniformtext_mode='hide',
        showlegend=False,
        margin=dict(t=30, b=0, l=0, r=0) # タイトルとグラフの余白調整
    )
    st.plotly_chart(fig, use_container_width=True)

def display_asset_list(portfolio: Dict, currency: str, rate: float, name_map: Dict):
    """保有資産一覧をdata_editorで表示し、数量調整機能を提供する"""
    st.subheader("📋 保有資産一覧")
    if not portfolio:
        st.info("保有資産はありません。")
        return

    portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    
    # 表示用DataFrameを作成
    df_display = portfolio_df.copy()
    df_display['現在価格'] = df_display['現在価格(JPY)'] * rate
    df_display['評価額'] = df_display['評価額(JPY)'] * rate
    df_display = df_display.sort_values(by='評価額', ascending=False)

    column_config = {
        "コイン名": "コイン名", "取引所": "取引所",
        "保有数量": st.column_config.NumberColumn(format="%.8f"),
        "現在価格": st.column_config.NumberColumn(format="%.8f"),
        #"現在価格": st.column_config.NumberColumn(f"現在価格 ({currency.upper()})", format="%,.2f"),
        "評価額": st.column_config.NumberColumn(f"評価額 ({currency.upper()})", format="%,.0f"),
    }

    # 変更前の状態を保存
    if 'before_edit_df' not in st.session_state:
        st.session_state.before_edit_df = df_display

    edited_df = st.data_editor(
        df_display[['コイン名', '取引所', '保有数量', '現在価格', '評価額']], 
        disabled=['コイン名', '取引所', '現在価格', '評価額'], 
        column_config=column_config, 
        use_container_width=True,
        key="portfolio_editor",
        hide_index=True
    )
    
    # 差分を検出して更新処理
    if not edited_df['保有数量'].equals(st.session_state.before_edit_df['保有数量']):
        # st.data_editorはDataFrame全体を返すため、元のDataFrameとインデックスを合わせて比較
        merged_df = pd.merge(st.session_state.before_edit_df, edited_df, on=['コイン名', '取引所'], suffixes=('_before', '_after'))
        
        for _, row in merged_df.iterrows():
            if not np.isclose(row['保有数量_before'], row['保有数量_after']):
                quantity_diff = row['保有数量_after'] - row['保有数量_before']
                coin_name, exchange = row['コイン名'], row['取引所']
                coin_id = next((cid for cid, name in name_map.items() if name == coin_name), None)

                if coin_id:
                    transaction_type = "調整（増）" if quantity_diff > 0 else "調整（減）"
                    transaction = {
                        "transaction_date": datetime.now(timezone.utc),
                        "coin_id": coin_id, "coin_name": coin_name, "exchange": exchange,
                        "transaction_type": transaction_type, "quantity": abs(quantity_diff),
                        "price_jpy": 0, "fee_jpy": 0, "total_jpy": 0,
                    }
                    if add_transaction_to_bq(transaction):
                        st.toast(f"{coin_name} ({exchange}) の数量を調整: {quantity_diff:+.8f}", icon="✍️")
        
        # 変更を反映するために再実行
        del st.session_state.before_edit_df
        st.rerun()

def display_transaction_form(coin_options: Dict, name_map: Dict):
    """取引履歴の登録フォームを表示する"""
    with st.expander("取引履歴の登録", expanded=False):
        with st.form("transaction_form", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            c1, c2, c3 = st.columns(3)
            with c1:
                transaction_date = st.date_input("取引日", datetime.now())
                selected_coin_disp_name = st.selectbox("コイン種別", options=coin_options.keys())
            with c2:
                transaction_type = st.selectbox("売買種別", ["購入", "売却"])
                exchange = st.text_input("取引所", "Binance")
            with c3:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f")
                price = st.number_input("価格(JPY)", min_value=0.0, format="%.2f")
                fee = st.number_input("手数料(JPY)", min_value=0.0, format="%.2f")

            if st.form_submit_button("登録する"):
                coin_id = coin_options[selected_coin_disp_name]
                transaction = {
                    "transaction_date": datetime.combine(transaction_date, datetime.min.time()),
                    "coin_id": coin_id,
                    "coin_name": name_map.get(coin_id, selected_coin_disp_name),
                    "exchange": exchange,
                    "transaction_type": transaction_type,
                    "quantity": quantity,
                    "price_jpy": price,
                    "fee_jpy": fee,
                    "total_jpy": quantity * price,
                }
                if add_transaction_to_bq(transaction):
                    st.success(f"{transaction['coin_name']}の{transaction_type}取引を登録しました。")
                    st.rerun()

def display_transaction_history(transactions_df: pd.DataFrame):
    """取引履歴の一覧を表示する"""
    st.subheader("🗒️ 取引履歴")
    if transactions_df.empty:
        st.info("まだ取引履歴がありません。")
        return

    history_config = {
        "取引日": st.column_config.DatetimeColumn("取引日時", format="YYYY/MM/DD HH:mm"), 
        "数量": st.column_config.NumberColumn(format="%.6f"), 
        "価格(JPY)": st.column_config.NumberColumn(format="%,.2f")
    }
    st.dataframe(
        transactions_df[['取引日', 'コイン名', '取引所', '売買種別', '数量', '価格(JPY)']],
        hide_index=True, use_container_width=True,
        column_config=history_config
    )

def display_database_management():
    """データベースのリセット機能を表示する"""
    st.subheader("⚙️ データベース管理")
    with st.expander("データベースリセット（危険）"):
        st.warning("**警告**: この操作はデータベース上のすべての取引履歴を完全に削除します。この操作は取り消せません。")
        
        if st.session_state.get('confirm_delete', False):
            st.error("本当によろしいですか？最終確認です。")
            c1, c2 = st.columns(2)
            if c1.button("はい、すべてのデータを削除します", type="primary"):
                reset_bigquery_table()
                st.session_state.confirm_delete = False
                st.rerun()
            if c2.button("いいえ、キャンセルします"):
                st.session_state.confirm_delete = False
                st.rerun()
        else:
            if st.button("すべての取引履歴をリセットする"):
                st.session_state.confirm_delete = True
                st.rerun()

def render_watchlist_tab(market_data: pd.DataFrame, currency: str, rate: float):
    """ウォッチリストタブのコンテンツを表示する"""
    st.header("⭐ ウォッチリスト")
    st.subheader(f"現在の仮想通貨価格 ({currency.upper()})")

    watchlist_df = market_data.copy()
    watchlist_df['現在価格'] = watchlist_df['price_jpy'] * rate
    
    column_config = {
        "symbol": "シンボル", "name": "コイン名",
        "現在価格": st.column_config.NumberColumn(f"現在価格 ({currency.upper()})", format="%,.2f"),
        "price_change_percentage_24h": st.column_config.NumberColumn("24h変動率 (%)", format="%.2f")
    }
    st.dataframe(
        watchlist_df.sort_values(by='price_jpy', ascending=False)[['symbol', 'name', '現在価格', 'price_change_percentage_24h']],
        hide_index=True, use_container_width=True,
        column_config=column_config
    )

# --- メイン処理 ---
def main():
    """アプリケーションのメイン実行関数"""
    if not bq_client:
        st.stop()
        
    st.title("🪙 仮想通貨ポートフォリオ管理アプリ")

    # セッションステートの初期化
    if 'currency' not in st.session_state:
        st.session_state.currency = 'jpy'
    if 'confirm_delete' not in st.session_state:
        st.session_state.confirm_delete = False
        
    # --- データ準備 ---
    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。しばらくしてから再読み込みしてください。")
        st.stop()
        
    # 汎用的なデータマップを作成
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    # --- UI ---
    selected_currency = st.radio(
        "表示通貨を選択",
        options=['jpy', 'usd'],
        format_func=lambda x: x.upper(),
        horizontal=True,
        key='currency'
    )
    exchange_rate = get_exchange_rate(selected_currency)
    currency_symbol = CURRENCY_SYMBOLS[selected_currency]
    
    # DBテーブルの存在確認と初期化
    init_bigquery_table()
    
    transactions_df = get_transactions_from_bq()

    tab1, tab2 = st.tabs(["ポートフォリオ", "ウォッチリスト"])

    with tab1:
        # ポートフォリオ計算
        portfolio, total_asset_jpy, total_change_24h_jpy = calculate_portfolio(
            transactions_df, price_map, price_change_map, name_map
        )

        # ▼▼▼▼▼【変更箇所】▼▼▼▼▼
        # サマリー表示
        display_summary(
            total_asset_jpy, total_change_24h_jpy, 
            selected_currency, exchange_rate, currency_symbol,
            price_map, price_change_map # BTC計算用にマップを渡す
        )
        # ▲▲▲▲▲【変更箇所】▲▲▲▲▲
        st.markdown("---")

        # 資産構成と一覧
        c1, c2 = st.columns([1, 1.2])
        with c1:
            display_asset_pie_chart(portfolio, exchange_rate, currency_symbol)
        with c2:
            display_asset_list(portfolio, selected_currency, exchange_rate, name_map)
        
        st.markdown("---")
        
        # 取引フォームと履歴
        display_transaction_form(coin_options, name_map)
        display_transaction_history(transactions_df)

        st.markdown("---")
        
        # DB管理
        display_database_management()

    with tab2:
        render_watchlist_tab(market_data, selected_currency, exchange_rate)


if __name__ == "__main__":
    main()
