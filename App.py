import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions
from typing import Dict, Any, List, Tuple

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

# 仮想通貨ごとのカラーコード
COIN_COLORS = {
    "Bitcoin": "#F7931A", "Ethereum": "#3C3C3D", "XRP": "#00AAE4",
    "Tether": "#50AF95", "BNB": "#F3BA2F", "Solana": "#9945FF",
    "USD Coin": "#2775CA", "Dogecoin": "#C3A634", "Cardano": "#0033AD",
    "TRON": "#EF0027", "Chainlink": "#2A5ADA", "Avalanche": "#E84142",
    "Shiba Inu": "#FFC001", "Polkadot": "#E6007A", "Bitcoin Cash": "#8DC351",
    "Toncoin": "#0098EA", "Polygon": "#8247E5", "Litecoin": "#345D9D",
    "NEAR Protocol": "#000000", "Internet Computer": "#3B00B9"
}

# アプリケーション関連
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
TRANSACTION_TYPES_BUY = ['購入', '調整（増）']
TRANSACTION_TYPES_SELL = ['売却', '調整（減）']
EXCHANGES_ORDERED = ['SBIVC', 'BITPOINT', 'Binance', 'bitbank', 'GMOコイン', 'Bybit']


# --- 初期設定 & クライアント初期化 ---
st.set_page_config(page_title="仮想通貨ポートフォリオ管理", page_icon="🪙", layout="wide")

@st.cache_resource
def get_bigquery_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    except (KeyError, FileNotFoundError):
        st.error("BigQueryの認証情報が設定されていません。StreamlitのSecretsを確認してください。")
        return None

cg_client = CoinGeckoAPI()
bq_client = get_bigquery_client()


# --- BigQuery関連関数 ---
def init_bigquery_table():
    if not bq_client: return
    try:
        bq_client.get_table(TABLE_FULL_ID)
    except google.api_core.exceptions.NotFound:
        table = bigquery.Table(TABLE_FULL_ID, schema=BIGQUERY_SCHEMA)
        bq_client.create_table(table)
        st.toast(f"BigQueryテーブル '{TABLE_ID}' を作成しました。")

def add_transaction_to_bq(transaction_data: Dict[str, Any]):
    if not bq_client: return False
    if isinstance(transaction_data["transaction_date"], datetime) and transaction_data["transaction_date"].tzinfo is None:
        transaction_data["transaction_date"] = transaction_data["transaction_date"].replace(tzinfo=timezone.utc)
    transaction_data["transaction_date"] = transaction_data["transaction_date"].isoformat()
    errors = bq_client.insert_rows_json(TABLE_FULL_ID, [transaction_data])
    if errors:
        st.error(f"データの追加に失敗しました: {errors}")
        return False
    return True

def delete_transaction_from_bq(transaction: pd.Series) -> bool:
    if not bq_client: return False
    query = f"""
        DELETE FROM `{TABLE_FULL_ID}` WHERE transaction_date = @transaction_date
        AND coin_id = @coin_id AND exchange = @exchange
        AND transaction_type = @transaction_type AND quantity = @quantity
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
    if not bq_client: return pd.DataFrame()
    query = f"SELECT * FROM `{TABLE_FULL_ID}` ORDER BY transaction_date DESC"
    try:
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table()
        return pd.DataFrame()
    if not df.empty:
        df['transaction_date'] = df['transaction_date'].dt.tz_convert('Asia/Tokyo')
        rename_map = {'transaction_date': '取引日', 'coin_name': 'コイン名', 'exchange': '取引所',
                      'transaction_type': '売買種別', 'quantity': '数量', 'price_jpy': '価格(JPY)',
                      'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)'}
        df_display = df.rename(columns=rename_map)
        df_display['コインID'] = df['coin_id']
        return df_display
    return pd.DataFrame()

def reset_bigquery_table():
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
    try:
        data = cg_client.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=20, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'current_price', 'price_change_24h', 'price_change_percentage_24h', 'market_cap'])
        return df.rename(columns={'current_price': 'price_jpy', 'price_change_24h': 'price_change_24h_jpy'})
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_exchange_rate(target_currency: str) -> float:
    if target_currency == 'jpy': return 1.0
    try:
        prices = cg_client.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency}')
        jpy_price = prices['bitcoin']['jpy']
        target_price = prices['bitcoin'][target_currency]
        return target_price / jpy_price if jpy_price > 0 else 1.0
    except Exception as e:
        st.warning(f"為替レートの取得に失敗しました: {e}")
        return 1.0


# --- データ処理 & フォーマット関数 ---
def format_currency(value: float, symbol: str, precision: int = 0) -> str:
    return f"{symbol}{value:,.{precision}f}"

def calculate_portfolio(transactions_df: pd.DataFrame, price_map: Dict, price_change_map: Dict, name_map: Dict) -> (Dict, float, float):
    portfolio, total_asset_jpy, total_change_24h_jpy = {}, 0, 0
    if transactions_df.empty: return portfolio, total_asset_jpy, total_change_24h_jpy
    for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
        buy_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_BUY)]['数量'].sum()
        sell_quantity = group[group['売買種別'].isin(TRANSACTION_TYPES_SELL)]['数量'].sum()
        current_quantity = buy_quantity - sell_quantity
        if current_quantity > 1e-9:
            current_price_jpy = price_map.get(coin_id, 0)
            current_value_jpy = current_quantity * current_price_jpy
            change_24h_per_coin = price_change_map.get(coin_id, 0)
            asset_change_24h_jpy = current_quantity * change_24h_per_coin
            portfolio[(coin_id, exchange)] = {"コイン名": name_map.get(coin_id, coin_id), "取引所": exchange, "保有数量": current_quantity,
                                             "現在価格(JPY)": current_price_jpy, "評価額(JPY)": current_value_jpy, "コインID": coin_id}
            total_asset_jpy += current_value_jpy
            total_change_24h_jpy += asset_change_24h_jpy
    return portfolio, total_asset_jpy, total_change_24h_jpy

def calculate_btc_value(total_asset_jpy: float, price_map: Dict) -> float:
    btc_price_jpy = price_map.get('bitcoin', 0)
    return total_asset_jpy / btc_price_jpy if btc_price_jpy > 0 else 0.0

def calculate_deltas(total_asset_jpy: float, total_change_24h_jpy: float, rate: float, symbol: str, price_map: Dict, price_change_map: Dict) -> Tuple[str, str, str, str]:
    display_total_change = total_change_24h_jpy * rate
    yesterday_asset_jpy = total_asset_jpy - total_change_24h_jpy
    change_pct = (total_change_24h_jpy / yesterday_asset_jpy * 100) if yesterday_asset_jpy > 0 else 0
    delta_display_str = f"{symbol}{display_total_change:,.2f} ({change_pct:+.2f}%)"
    jpy_delta_color = "green" if total_change_24h_jpy >= 0 else "red"
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    delta_btc_str, btc_delta_color = "N/A", "grey"
    btc_price_jpy = price_map.get('bitcoin', 0)
    if btc_price_jpy > 0:
        btc_change_24h_jpy = price_change_map.get('bitcoin', 0)
        btc_price_24h_ago_jpy = btc_price_jpy - btc_change_24h_jpy
        if btc_price_24h_ago_jpy > 0 and yesterday_asset_jpy > 0:
            total_asset_btc_24h_ago = yesterday_asset_jpy / btc_price_24h_ago_jpy
            change_btc = total_asset_btc - total_asset_btc_24h_ago
            change_btc_pct = (change_btc / total_asset_btc_24h_ago * 100) if total_asset_btc_24h_ago > 0 else 0
            delta_btc_str = f"{change_btc:+.8f} BTC ({change_btc_pct:+.2f}%)"
            btc_delta_color = "green" if change_btc >= 0 else "red"
    return delta_display_str, jpy_delta_color, delta_btc_str, btc_delta_color


# --- UI描画関数 ---
RIGHT_ALIGN_STYLE = """
    <style>
        .right-align-table .stDataFrame [data-testid="stDataFrameData-row"] > div {
            text-align: right !important;
            justify-content: flex-end !important;
        }
        .right-align-table .stDataFrame [data-testid="stDataFrameData-row"] > div:first-child {
            text-align: left !important;
            justify-content: flex-start !important;
        }
        .right-align-table .stDataFrame [data-testid="stDataFrameData-row"] > div[data-col-id="1"]:not(:first-child) {
            text-align: left !important;
            justify-content: flex-start !important;
        }
    </style>
"""

# ★★★ 変更点: 引数に jpy_delta_color と btc_delta_color を追加 ★★★
def display_asset_pie_chart(portfolio: Dict, rate: float, symbol: str, total_asset_jpy: float, total_asset_btc: float, jpy_delta_color: str, btc_delta_color: str):
    st.subheader("📊 資産構成")
    if not portfolio:
        st.info("取引履歴を登録すると、ここにグラフが表示されます。")
        return
    pie_data = pd.DataFrame.from_dict(portfolio, orient='index').groupby("コイン名")["評価額(JPY)"].sum().reset_index()
    if pie_data.empty or pie_data["評価額(JPY)"].sum() <= 0:
        st.info("保有資産がありません。")
        return
    pie_data = pie_data.sort_values(by="評価額(JPY)", ascending=False)
    pie_data['評価額_display'] = pie_data['評価額(JPY)'] * rate
    fig = px.pie(pie_data, values='評価額_display', names='コイン名', color='コイン名', hole=0.5, color_discrete_map=COIN_COLORS)
    fig.update_traces(textposition='inside', textinfo='text', texttemplate=f"%{{label}} (%{{percent}})<br>{symbol}%{{value:,.0f}}",
                      textfont_size=12, marker=dict(line=dict(color='#FFFFFF', width=2)), direction='clockwise', rotation=0)
    
    # ★★★ 変更点: アノテーションテキストに色情報を追加 ★★★
    annotation_text = (
    f"<span style='display: block; font-size: 2.0em; color: {jpy_delta_color}; margin-bottom: 8px;'>{symbol}{total_asset_jpy * rate:,.0f}</span>"
    f"<span style='display: block; font-size: 1.5em; color: {btc_delta_color};'>{total_asset_btc:.4f} BTC</span>"
    )
    
    fig.update_layout(uniformtext_minsize=10, uniformtext_mode='hide', showlegend=False,
                      margin=dict(t=30, b=0, l=0, r=0),
                      annotations=[dict(text=annotation_text, x=0.5, y=0.5, font_size=16, showarrow=False)])
    st.plotly_chart(fig, use_container_width=True)

def display_asset_list(portfolio: Dict, currency: str, rate: float, name_map: Dict):
    st.subheader("📋 資産一覧")
    if not portfolio:
        st.info("保有資産はありません。")
        return
    st.markdown(RIGHT_ALIGN_STYLE, unsafe_allow_html=True)
    portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
    portfolio_df['評価額_display'] = portfolio_df['評価額(JPY)'] * rate
    tab_coin, tab_exchange, tab_detail = st.tabs(["コイン別", "取引所別", "詳細"])
    symbol = CURRENCY_SYMBOLS[currency]

    with tab_coin:
        coin_summary = portfolio_df.groupby("コイン名").agg(保有数量=('保有数量', 'sum'), 評価額_display=('評価額_display', 'sum'),
                                                         現在価格_jpy=('現在価格(JPY)', 'first')).sort_values(by='評価額_display', ascending=False).reset_index()
        price_precision = 4 if currency == 'jpy' else 2
        coin_summary['評価額'] = coin_summary['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
        coin_summary['現在価格'] = (coin_summary['現在価格_jpy'] * rate).apply(lambda x: format_currency(x, symbol, price_precision))
        coin_summary['保有数量'] = coin_summary['保有数量'].apply(lambda x: f"{x:,.8f}".rstrip('0').rstrip('.'))
        
        st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
        st.dataframe(coin_summary[['コイン名', '保有数量', '評価額', '現在価格']],
                     column_config={"コイン名": "コイン名", "保有数量": "保有数量", "評価額": f"評価額 ({currency.upper()})", "現在価格": f"現在価格 ({currency.upper()})"},
                     hide_index=True, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_exchange:
        exchange_summary = portfolio_df.groupby("取引所")['評価額_display'].sum().sort_values(ascending=False).reset_index()
        exchange_summary['評価額'] = exchange_summary['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
        
        st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
        st.dataframe(exchange_summary[['取引所', '評価額']],
                     column_config={"取引所": "取引所", "評価額": f"評価額 ({currency.upper()})"},
                     hide_index=True, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_detail:
        df_display = portfolio_df.copy().sort_values(by='評価額_display', ascending=False)
        df_display['現在価格_display'] = df_display['現在価格(JPY)'] * rate
        price_precision = 4 if currency == 'jpy' else 2
        df_display['評価額_formatted'] = df_display['評価額_display'].apply(lambda x: format_currency(x, symbol, 0))
        df_display['現在価格_formatted'] = df_display['現在価格_display'].apply(lambda x: format_currency(x, symbol, price_precision))
        
        if f'before_edit_df_{currency}' not in st.session_state or not st.session_state[f'before_edit_df_{currency}'].equals(df_display):
             st.session_state[f'before_edit_df_{currency}'] = df_display.copy()
        
        column_config = {"コイン名": "コイン名", "取引所": "取引所",
                         "保有数量": st.column_config.NumberColumn("保有数量", format="%.8f"),
                         "評価額_formatted": f"評価額 ({currency.upper()})",
                         "現在価格_formatted": f"現在価格 ({currency.upper()})"}

        st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
        edited_df = st.data_editor(df_display[['コイン名', '取引所', '保有数量', '評価額_formatted', '現在価格_formatted']],
                                   disabled=['コイン名', '取引所', '評価額_formatted', '現在価格_formatted'],
                                   column_config=column_config, use_container_width=True,
                                   key=f"portfolio_editor_{currency}", hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        if not edited_df['保有数量'].equals(st.session_state[f'before_edit_df_{currency}']['保有数量']):
            merged_df = pd.merge(st.session_state[f'before_edit_df_{currency}'], edited_df, on=['コイン名', '取引所'], suffixes=('_before', '_after'))
            for _, row in merged_df.iterrows():
                if not np.isclose(row['保有数量_before'], row['保有数量_after']):
                    quantity_diff = row['保有数量_after'] - row['保有数量_before']
                    coin_name, exchange, coin_id = row['コイン名'], row['取引所'], row['コインID']
                    transaction_type = "調整（増）" if quantity_diff > 0 else "調整（減）"
                    transaction = {"transaction_date": datetime.now(timezone.utc), "coin_id": coin_id, "coin_name": coin_name,
                                   "exchange": exchange, "transaction_type": transaction_type, "quantity": abs(quantity_diff),
                                   "price_jpy": 0, "fee_jpy": 0, "total_jpy": 0}
                    if add_transaction_to_bq(transaction):
                        st.toast(f"{coin_name} ({exchange}) の数量を調整: {quantity_diff:+.8f}", icon="✍️")
            del st.session_state[f'before_edit_df_{currency}']
            st.rerun()

def display_transaction_form(coin_options: Dict, name_map: Dict, currency: str):
    with st.expander("取引履歴の登録", expanded=False):
        with st.form(key=f"transaction_form_{currency}", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            c1, c2, c3 = st.columns(3)
            with c1:
                transaction_date = st.date_input("取引日", datetime.now(), key=f"date_{currency}")
                selected_coin_disp_name = st.selectbox("コイン種別", options=list(coin_options.keys()), key=f"coin_{currency}")
            with c2:
                transaction_type = st.selectbox("売買種別", ["購入", "売却"], key=f"type_{currency}")
                exchange = st.selectbox("取引所", options=EXCHANGES_ORDERED, index=2, key=f"exchange_{currency}")
            with c3:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f", key=f"qty_{currency}")
                price = st.number_input("価格(JPY)", min_value=0.0, format="%.2f", key=f"price_{currency}")
                fee = st.number_input("手数料(JPY)", min_value=0.0, format="%.2f", key=f"fee_{currency}")
            if st.form_submit_button("登録する"):
                coin_id = coin_options[selected_coin_disp_name]
                transaction = {"transaction_date": datetime.combine(transaction_date, datetime.min.time()),
                               "coin_id": coin_id, "coin_name": name_map.get(coin_id, selected_coin_disp_name),
                               "exchange": exchange, "transaction_type": transaction_type, "quantity": quantity,
                               "price_jpy": price, "fee_jpy": fee, "total_jpy": quantity * price}
                if add_transaction_to_bq(transaction):
                    st.success(f"{transaction['coin_name']}の{transaction_type}取引を登録しました。")
                    st.rerun()

def display_transaction_history(transactions_df: pd.DataFrame, currency: str):
    st.subheader("🗒️ 取引履歴")
    if transactions_df.empty:
        st.info("まだ取引履歴がありません。")
        return
    cols = st.columns([3, 2, 2, 2, 2, 1])
    headers = ["取引日時", "コイン名", "取引所", "売買種別", "数量", "操作"]
    for col, header in zip(cols, headers): col.markdown(f"**{header}**")
    for index, row in transactions_df.iterrows():
        unique_key = f"delete_{currency}_{row['取引日'].timestamp()}_{row['コインID']}_{row['数量']}"
        cols = st.columns([3, 2, 2, 2, 2, 1])
        cols[0].text(row['取引日'].strftime('%Y/%m/%d %H:%M:%S'))
        cols[1].text(row['コイン名']); cols[2].text(row['取引所'])
        cols[3].text(row['売買種別']); cols[4].text(f"{row['数量']:.8f}")
        if cols[5].button("削除", key=unique_key):
            if delete_transaction_from_bq(row):
                st.toast(f"取引を削除しました: {row['取引日'].strftime('%Y/%m/%d')}の{row['コイン名']}取引", icon="🗑️")
                st.rerun()

def display_database_management(currency: str):
    st.subheader("⚙️ データベース管理")
    confirm_key = f'confirm_delete_{currency}'
    if confirm_key not in st.session_state:
        st.session_state[confirm_key] = False

    with st.expander("データベースリセット（危険）"):
        st.warning("**警告**: この操作はデータベース上のすべての取引履歴を完全に削除します。この操作は取り消せません。")
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
        else:
            if st.button("すべての取引履歴をリセットする", key=f"reset_button_{currency}"):
                st.session_state[confirm_key] = True
                st.rerun()

def render_watchlist_tab(market_data: pd.DataFrame, currency: str, rate: float):
    st.header("⭐ ウォッチリスト")
    st.subheader(f"時価総額トップ20 ({currency.upper()})")
    
    if 'market_cap' not in market_data.columns:
        st.warning("時価総額データが取得できませんでした。")
        return
        
    st.markdown(RIGHT_ALIGN_STYLE, unsafe_allow_html=True)
    watchlist_df = market_data.copy()
    symbol = CURRENCY_SYMBOLS[currency]
    price_precision = 4 if currency == 'jpy' else 2
    watchlist_df['現在価格_formatted'] = (watchlist_df['price_jpy'] * rate).apply(lambda x: format_currency(x, symbol, price_precision))
    watchlist_df['時価総額_formatted'] = (watchlist_df['market_cap'] * rate).apply(lambda x: format_currency(x, symbol, 0))
    watchlist_df.rename(columns={'name': '銘柄', '現在価格_formatted': '現在価格', '時価総額_formatted': '時価総額',
                                 'price_change_percentage_24h': '24h変動率'}, inplace=True)
    column_config = {
        "銘柄": "銘柄", "現在価格": f"現在価格 ({currency.upper()})",
        "時価総額": f"時価総額 ({currency.upper()})",
        "24h変動率": st.column_config.NumberColumn("24h変動率 (%)", format="%.2f%%")}

    st.markdown('<div class="right-align-table">', unsafe_allow_html=True)
    st.dataframe(
        watchlist_df.sort_values(by='market_cap', ascending=False)[['銘柄', '現在価格', '時価総額', '24h変動率']],
        hide_index=True, use_container_width=True, column_config=column_config)
    st.markdown('</div>', unsafe_allow_html=True)

def render_portfolio_page(transactions_df: pd.DataFrame, market_data: pd.DataFrame, currency: str):
    rate = get_exchange_rate(currency)
    symbol = CURRENCY_SYMBOLS[currency]
    price_map = market_data.set_index('id')['price_jpy'].to_dict()
    price_change_map = market_data.set_index('id')['price_change_24h_jpy'].to_dict()
    name_map = market_data.set_index('id')['name'].to_dict()
    coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in market_data.iterrows()}

    portfolio, total_asset_jpy, total_change_24h_jpy = calculate_portfolio(transactions_df, price_map, price_change_map, name_map)
    total_asset_btc = calculate_btc_value(total_asset_jpy, price_map)
    delta_display_str, jpy_delta_color, delta_btc_str, btc_delta_color = calculate_deltas(
        total_asset_jpy, total_change_24h_jpy, rate, symbol, price_map, price_change_map)

    c1, c2 = st.columns([1, 1.2])
    with c1:
        # ★★★ 変更点: display_asset_pie_chart に jpy_delta_color と btc_delta_color を渡す ★★★
        display_asset_pie_chart(portfolio, rate, symbol, total_asset_jpy, total_asset_btc, jpy_delta_color, btc_delta_color)
        st.markdown(f"""
        <div style="text-align: center; margin-top: 5px; line-height: 1.4;">
            <span style="font-size: 1.0rem; color: {jpy_delta_color};">{delta_display_str}</span>
            <span style="font-size: 1.0rem; color: {btc_delta_color}; margin-left: 12px;">{delta_btc_str}</span>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        display_asset_list(portfolio, currency, rate, name_map)
    
    st.markdown("---")
    display_transaction_form(coin_options, name_map, currency)
    display_transaction_history(transactions_df, currency)
    st.markdown("---")
    display_database_management(currency)


# --- メイン処理 ---
def main():
    if not bq_client: st.stop()
    st.title("🪙 仮想通貨ポートフォリオ管理アプリ")
    
    market_data = get_market_data()
    if market_data.empty:
        st.error("市場データを取得できませんでした。しばらくしてから再読み込みしてください。")
        st.stop()

    init_bigquery_table()
    transactions_df = get_transactions_from_bq()

    tab_pf_jpy, tab_wl_jpy, tab_pf_usd, tab_wl_usd = st.tabs([
        "ポートフォリオ (JPY)", "ウォッチリスト (JPY)", 
        "ポートフォリオ (USD)", "ウォッチリスト (USD)"
    ])

    with tab_pf_jpy:
        render_portfolio_page(transactions_df, market_data, currency='jpy')

    with tab_wl_jpy:
        render_watchlist_tab(market_data, currency='jpy', rate=1.0)
            
    with tab_pf_usd:
        render_portfolio_page(transactions_df, market_data, currency='usd')
            
    with tab_wl_usd:
        usd_rate = get_exchange_rate('usd')
        render_watchlist_tab(market_data, currency='usd', rate=usd_rate)

if __name__ == "__main__":
    main()
