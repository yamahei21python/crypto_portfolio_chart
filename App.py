import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime
import sqlite3
import numpy as np

# --- DB設定 ---
DB_FILE = "portfolio.db"

def get_db_connection():
    """データベース接続を取得する"""
    return sqlite3.connect(DB_FILE)

def init_db():
    """データベースを初期化し、テーブルを作成する"""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_date TEXT NOT NULL,
            coin_id TEXT NOT NULL,
            coin_name TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            price_jpy REAL NOT NULL,
            fee_jpy REAL NOT NULL,
            total_jpy REAL NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def add_transaction_to_db(date, coin_id, coin_name, type, qty, price, fee, total):
    """取引履歴をデータベースに追加する"""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO transactions (transaction_date, coin_id, coin_name, transaction_type, quantity, price_jpy, fee_jpy, total_jpy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (date.isoformat(), coin_id, coin_name, type, qty, price, fee, total))
    conn.commit()
    conn.close()

def get_transactions_from_db():
    """データベースから全ての取引履歴を取得し、DataFrameとして返す"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM transactions", conn)
    finally:
        conn.close()
    
    if not df.empty:
        # ### エラー修正 ###
        # 形式が混在するISO8601形式の日付文字列を正しく解釈するために format='ISO8601' を指定
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='ISO8601', errors='coerce')
        # ### ここまで ###
        
        # NaTになった行（不正な日付データを持つ行）を削除する
        df.dropna(subset=['transaction_date'], inplace=True)

        df = df.rename(columns={
            'transaction_date': '取引日', 'coin_id': 'コインID', 'coin_name': 'コイン名',
            'transaction_type': '売買種別', 'quantity': '数量', 'price_jpy': '価格(JPY)',
            'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)'
        }).drop(columns=['id'])
    else:
        df = pd.DataFrame(columns=[
            "取引日", "コインID", "コイン名", "売買種別", "数量", "価格(JPY)", "手数料(JPY)", "合計(JPY)"
        ])
    return df

# --- DB初期化 ---
init_db()

# --- 初期設定 ---
st.set_page_config(page_title="仮想通貨ポートフォリ管理", page_icon="🪙", layout="wide")

# --- APIクライアントとグローバル設定 ---
cg = CoinGeckoAPI()
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
if 'currency' not in st.session_state:
    st.session_state.currency = 'jpy'

# --- 関数定義 ---
@st.cache_data(ttl=600)
def get_crypto_data():
    try:
        data = cg.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=20, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'current_price'])
        df = df.rename(columns={'current_price': 'price_jpy'})
        return df
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_exchange_rate(target_currency='usd'):
    if target_currency == 'jpy': return 1.0
    try:
        prices = cg.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency}')
        jpy_price = prices['bitcoin']['jpy']
        target_price = prices['bitcoin'][target_currency]
        return target_price / jpy_price if jpy_price > 0 else 1.0
    except Exception as e:
        st.warning(f"為替レートの取得に失敗しました: {e}")
        return 1.0

# --- データ取得と加工 ---
crypto_data_jpy = get_crypto_data()
if crypto_data_jpy.empty:
    st.stop()

coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for index, row in crypto_data_jpy.iterrows()}
price_map_jpy = crypto_data_jpy.set_index('id')['price_jpy'].to_dict()
name_map = crypto_data_jpy.set_index('id')['name'].to_dict()

st.title("🪙 仮想通貨ポートフォリオ管理")

selected_currency = st.radio(
    "表示通貨を選択", options=['jpy', 'usd'], format_func=lambda x: x.upper(),
    horizontal=True, key='currency'
)
st.caption("※取引履歴の入力は常に日本円(JPY)で行ってください。保有資産一覧の数量は直接編集して調整できます。")
exchange_rate = get_exchange_rate(selected_currency)
currency_symbol = CURRENCY_SYMBOLS[selected_currency]

tab1, tab2 = st.tabs(["ポートフォリオ", "ウォッチリスト"])

with tab1:
    transactions_df = get_transactions_from_db()
    portfolio = {}
    total_asset_value_jpy = 0
    
    if not transactions_df.empty:
        for coin_id in transactions_df['コインID'].unique():
            coin_tx = transactions_df[transactions_df['コインID'] == coin_id]
            buy_quantity = coin_tx[coin_tx['売買種別'].isin(['購入', '調整（増）'])]['数量'].sum()
            sell_quantity = coin_tx[coin_tx['売買種別'].isin(['売却', '調整（減）'])]['数量'].sum()
            current_quantity = buy_quantity - sell_quantity
            
            if current_quantity > 1e-8:
                current_price_jpy = price_map_jpy.get(coin_id, 0)
                current_value_jpy = current_quantity * current_price_jpy
                portfolio[coin_id] = {
                    "コイン名": name_map.get(coin_id, coin_id), "保有数量": current_quantity,
                    "現在価格(JPY)": current_price_jpy, "評価額(JPY)": current_value_jpy
                }
                total_asset_value_jpy += current_value_jpy
    
    st.header("📈 ポートフォリオサマリー")
    btc_price_jpy = price_map_jpy.get('bitcoin', 0)
    total_asset_btc = total_asset_value_jpy / btc_price_jpy if btc_price_jpy > 0 else 0
    display_total_asset = total_asset_value_jpy * exchange_rate
    col1, col2 = st.columns(2)
    col1.metric(label=f"保有資産合計 ({selected_currency.upper()})", value=f"{currency_symbol}{display_total_asset:,.2f}")
    col2.metric(label="保有資産合計 (BTC)", value=f"{total_asset_btc:.8f} BTC")
    
    st.markdown("---")
    col1, col2 = st.columns([1, 1.2])

    with col1:
        st.subheader("📊 資産割合")
        if portfolio:
            portfolio_df_jpy = pd.DataFrame.from_dict(portfolio, orient='index')
            display_df = portfolio_df_jpy[portfolio_df_jpy['評価額(JPY)'] > 0]
            if not display_df.empty:
                fig = px.pie(display_df, values='評価額(JPY)', names='コイン名', hole=0.3)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("保有資産がありません。")
        else: st.info("取引履歴を登録すると、ここにグラフが表示されます。")
    
    with col2:
        st.subheader("📋 保有資産一覧")
        if portfolio:
            portfolio_df_jpy = pd.DataFrame.from_dict(portfolio, orient='index')
            portfolio_df_before_edit = portfolio_df_jpy.copy()
            portfolio_df_display = portfolio_df_jpy.copy()
            portfolio_df_display['現在価格'] = portfolio_df_display['現在価格(JPY)'] * exchange_rate
            portfolio_df_display['評価額'] = portfolio_df_display['評価額(JPY)'] * exchange_rate
            
            edited_df = st.data_editor(
                portfolio_df_display[['コイン名', '保有数量', '現在価格', '評価額']],
                disabled=['コイン名', '現在価格', '評価額'],
                column_config={
                    "保有数量": st.column_config.NumberColumn(format="%.8f"),
                    "現在価格": st.column_config.NumberColumn(format=f"{currency_symbol}%,.2f"),
                    "評価額": st.column_config.NumberColumn(format=f"{currency_symbol}%,.0f"),
                }, use_container_width=True, key="portfolio_editor")

            update_triggered = False
            for index, row in edited_df.iterrows():
                original_quantity = portfolio_df_before_edit.loc[index, '保有数量']
                edited_quantity = row['保有数量']
                
                if not np.isclose(original_quantity, edited_quantity):
                    quantity_diff = edited_quantity - original_quantity
                    coin_id, coin_name = index, portfolio_df_before_edit.loc[index, 'コイン名']
                    transaction_type = "調整（増）" if quantity_diff > 0 else "調整（減）"
                    add_transaction_to_db(datetime.now(), coin_id, coin_name, transaction_type, abs(quantity_diff), 0, 0, 0)
                    st.toast(f"{coin_name}の数量を調整しました: {quantity_diff:+.8f}", icon="✍️")
                    update_triggered = True
            
            if update_triggered: st.rerun()
        else: st.info("保有資産はありません。")

    st.markdown("---")
    with st.expander("取引履歴の登録", expanded=False):
        with st.form("transaction_form", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            col1, col2 = st.columns(2)
            with col1:
                transaction_date = st.date_input("取引日", datetime.now())
                selected_coin_name = st.selectbox("コイン種別", options=coin_options.keys())
                transaction_type = st.selectbox("売買種別", ["購入", "売却"])
            with col2:
                quantity = st.number_input("数量", min_value=0.0, format="%.8f")
                price = st.number_input("価格 (1コインあたり, JPY)", min_value=0.0, format="%.2f")
                fee = st.number_input("手数料 (JPY)", min_value=0.0, format="%.2f")
            submitted = st.form_submit_button("登録する")
            if submitted:
                coin_id = coin_options[selected_coin_name]
                coin_name = name_map.get(coin_id, coin_id)
                add_transaction_to_db(transaction_date, coin_id, coin_name, transaction_type, quantity, price, fee, quantity * price)
                st.success(f"{coin_name}の{transaction_type}取引を登録しました。")
                st.rerun()

    st.subheader("🗒️ 取引履歴")
    if not transactions_df.empty:
        display_transactions = transactions_df.sort_values(by="取引日", ascending=False)
        st.dataframe(
            display_transactions, hide_index=True, use_container_width=True,
            column_config={ "取引日": st.column_config.DateColumn("取引日", format="YYYY/MM/DD HH:mm"), "数量": st.column_config.NumberColumn(format="%.6f"), "価格(JPY)": st.column_config.NumberColumn(format="¥%,.2f"), "手数料(JPY)": st.column_config.NumberColumn(format="¥%,.2f"), "合計(JPY)": st.column_config.NumberColumn(format="¥%,.0f"), })
    else: st.info("まだ取引履歴がありません。")

with tab2:
    st.header("⭐ ウォッチリスト")
    st.info("ここに仮想通貨一覧・ランキング機能を実装する予定です。")
    st.subheader(f"現在の仮想通貨価格 ({selected_currency.upper()})")
    watchlist_df = crypto_data_jpy.copy()
    watchlist_df['現在価格'] = watchlist_df['price_jpy'] * exchange_rate
    st.dataframe(
        watchlist_df[['symbol', 'name', '現在価格']],
        hide_index=True, use_container_width=True,
        column_config={ "symbol": "シンボル", "name": "コイン名", "現在価格": st.column_config.NumberColumn(f"現在価格 ({selected_currency.upper()})", format=f"{currency_symbol}%,.2f"), })
