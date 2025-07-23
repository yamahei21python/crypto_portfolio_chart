import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core.exceptions

# --- BigQuery 設定 ---
try:
    creds_dict = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=creds, project=creds.project_id)
except (KeyError, FileNotFoundError):
    st.error("BigQueryの認証情報が設定されていません。StreamlitのSecretsを確認してください。")
    st.stop()

PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_ID = "transactions"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def init_bigquery_table():
    schema = [
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
    table = bigquery.Table(TABLE_FULL_ID, schema=schema)
    try: client.get_table(TABLE_FULL_ID)
    except google.api_core.exceptions.NotFound:
        client.create_table(table)
        st.toast(f"BigQueryテーブル '{TABLE_ID}' を作成しました。")

def add_transaction_to_bq(date, coin_id, coin_name, exchange, type, qty, price, fee, total):
    if isinstance(date, datetime) and date.tzinfo is None: date = date.replace(tzinfo=timezone.utc)
    rows_to_insert = [{"transaction_date": date.isoformat(), "coin_id": coin_id, "coin_name": coin_name, "exchange": exchange, "transaction_type": type, "quantity": qty, "price_jpy": price, "fee_jpy": fee, "total_jpy": total}]
    errors = client.insert_rows_json(TABLE_FULL_ID, rows_to_insert)
    if errors: st.error(f"データの追加に失敗しました: {errors}")

def get_transactions_from_bq():
    query = f"SELECT * FROM `{TABLE_FULL_ID}` ORDER BY transaction_date DESC"
    try:
        df = client.query(query).to_dataframe(create_bqstorage_client=False)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table()
        return pd.DataFrame()
    if not df.empty:
        df = df.rename(columns={'transaction_date': '取引日', 'coin_id': 'コインID', 'coin_name': 'コイン名', 'exchange': '取引所', 'transaction_type': '売買種別', 'quantity': '数量', 'price_jpy': '価格(JPY)', 'fee_jpy': '手数料(JPY)', 'total_jpy': '合計(JPY)'})
        df['取引日'] = df['取引日'].dt.tz_convert('Asia/Tokyo')
    return df

def reset_bigquery_table():
    query = f"TRUNCATE TABLE `{TABLE_FULL_ID}`"
    try:
        client.query(query).result()
        st.success("すべての取引履歴がリセットされました。")
    except Exception as e:
        st.error(f"データベースのリセット中にエラーが発生しました: {e}")

# --- 初期設定 ---
st.set_page_config(page_title="仮想通貨ポートフォリ管理", page_icon="🪙", layout="wide")
cg = CoinGeckoAPI()
CURRENCY_SYMBOLS = {'jpy': '¥', 'usd': '$'}
if 'currency' not in st.session_state: st.session_state.currency = 'jpy'
if 'confirm_delete' not in st.session_state: st.session_state.confirm_delete = False

# --- 関数定義 (API関連) ---
@st.cache_data(ttl=600)
def get_crypto_data():
    try:
        data = cg.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=20, page=1)
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
def get_exchange_rate(target_currency='usd'):
    if target_currency == 'jpy': return 1.0
    try:
        prices = cg.get_price(ids='bitcoin', vs_currencies=f'jpy,{target_currency}')
        jpy_price, target_price = prices['bitcoin']['jpy'], prices['bitcoin'][target_currency]
        return target_price / jpy_price if jpy_price > 0 else 1.0
    except Exception as e:
        st.warning(f"為替レートの取得に失敗しました: {e}")
        return 1.0

# --- アプリ本体 ---
crypto_data_jpy = get_crypto_data()
if crypto_data_jpy.empty: st.stop()
coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for _, row in crypto_data_jpy.iterrows()}
price_map_jpy = crypto_data_jpy.set_index('id')['price_jpy'].to_dict()
price_change_24h_map_jpy = crypto_data_jpy.set_index('id')['price_change_24h_jpy'].to_dict()
name_map = crypto_data_jpy.set_index('id')['name'].to_dict()


st.title("🪙 仮想通貨ポートフォリオ管理アプリ") 
selected_currency = st.radio("表示通貨を選択", options=['jpy', 'usd'], format_func=lambda x: x.upper(), horizontal=True, key='currency')
exchange_rate = get_exchange_rate(selected_currency)
currency_symbol = CURRENCY_SYMBOLS[selected_currency]
init_bigquery_table()

tab1, tab2 = st.tabs(["ポートフォリオ", "ウォッチリスト"])
with tab1:
    transactions_df = get_transactions_from_bq()
    portfolio, total_asset_value_jpy, total_change_24h_jpy = {}, 0, 0

    if not transactions_df.empty:
        for (coin_id, exchange), group in transactions_df.groupby(['コインID', '取引所']):
            buy_quantity = group[group['売買種別'].isin(['購入', '調整（増）'])]['数量'].sum()
            sell_quantity = group[group['売買種別'].isin(['売却', '調整（減）'])]['数量'].sum()
            current_quantity = buy_quantity - sell_quantity

            if current_quantity > 1e-8:
                current_price_jpy = price_map_jpy.get(coin_id, 0)
                current_value_jpy = current_quantity * current_price_jpy
                
                change_24h_for_coin_jpy = price_change_24h_map_jpy.get(coin_id, 0)
                asset_change_24h_jpy = current_quantity * change_24h_for_coin_jpy
                
                portfolio[(coin_id, exchange)] = {"コイン名": name_map.get(coin_id, coin_id), "取引所": exchange, "保有数量": current_quantity, "現在価格(JPY)": current_price_jpy, "評価額(JPY)": current_value_jpy}
                total_asset_value_jpy += current_value_jpy
                total_change_24h_jpy += asset_change_24h_jpy
    
    st.header("📈 ポートフォリオサマリー")
    
    display_total_asset = total_asset_value_jpy * exchange_rate
    display_total_change_24h = total_change_24h_jpy * exchange_rate
    yesterday_asset_value_jpy = total_asset_value_jpy - total_change_24h_jpy
    total_change_percentage_24h = (total_change_24h_jpy / yesterday_asset_value_jpy * 100) if yesterday_asset_value_jpy > 0 else 0
    delta_display_str = f"{currency_symbol}{display_total_change_24h:,.2f} ({total_change_percentage_24h:+.2f}%)"

    btc_price_jpy = price_map_jpy.get('bitcoin', 0)
    total_asset_btc = total_asset_value_jpy / btc_price_jpy if btc_price_jpy > 0 else 0
    btc_price_change_24h_jpy = price_change_24h_map_jpy.get('bitcoin', 0)
    btc_price_jpy_24h_ago = btc_price_jpy - btc_price_change_24h_jpy
    total_asset_btc_24h_ago = (yesterday_asset_value_jpy / btc_price_jpy_24h_ago) if btc_price_jpy_24h_ago > 0 and yesterday_asset_value_jpy > 0 else 0
    total_change_24h_btc = total_asset_btc - total_asset_btc_24h_ago
    total_change_percentage_24h_btc = (total_change_24h_btc / total_asset_btc_24h_ago * 100) if total_asset_btc_24h_ago > 0 else 0
    delta_btc_str = f"{total_change_24h_btc:+.8f} BTC ({total_change_percentage_24h_btc:+.2f}%)"

    col1, col2 = st.columns(2)
    col1.metric(label=f"保有資産合計 ({selected_currency.upper()})", value=f"{currency_symbol}{display_total_asset:,.2f}", delta=delta_display_str)
    col2.metric(label="保有資産合計 (BTC)", value=f"{total_asset_btc:.8f} BTC", delta=delta_btc_str)
    
    st.markdown("---")
    col1, col2 = st.columns([1, 1.2])

    # ===【円グラフ修正箇所】===
    with col1:
        st.subheader("📊 資産割合 (コイン別)")
        if portfolio:
            # コイン名でグループ化し、評価額(JPY)を合計
            pie_data = pd.DataFrame.from_dict(portfolio, orient='index').groupby("コイン名")["評価額(JPY)"].sum().reset_index()
            
            # 資産がある場合のみグラフを描画
            if not pie_data.empty and pie_data["評価額(JPY)"].sum() > 0:
                
                # 表示通貨建ての評価額を計算
                pie_data['評価額_display'] = pie_data['評価額(JPY)'] * exchange_rate
                
                # 円グラフを作成。valuesには表示通貨建ての評価額を指定
                fig = px.pie(
                    pie_data, 
                    values='評価額_display',
                    names='コイン名', 
                    hole=0.3
                )
                
                # texttemplateを使って表示内容をカスタマイズ
                # {label}: コイン名, {percent}: 割合, {value:,.0f}: カンマ区切り整数にフォーマットした評価額
                fig.update_traces(
                    textposition='inside',
                    texttemplate=f'%{{label}}<br>%{{percent}}<br>{currency_symbol}%{{value:,.0f}}'
                )

                # ラベルが重なり合わないように調整し、凡例を非表示にする
                fig.update_layout(
                    uniformtext_minsize=12, 
                    uniformtext_mode='hide',
                    showlegend=False
                )

                st.plotly_chart(fig, use_container_width=True)
            else: 
                st.info("保有資産がありません。")
        else: 
            st.info("取引履歴を登録すると、ここにグラフが表示されます。")
    
    with col2:
        st.subheader("📋 保有資産一覧")
        if portfolio:
            portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index').reset_index(drop=True)
            portfolio_df_before_edit = portfolio_df.copy()
            portfolio_df_display = portfolio_df.copy()
            portfolio_df_display['現在価格'] = portfolio_df_display['現在価格(JPY)'] * exchange_rate
            portfolio_df_display['評価額'] = portfolio_df_display['評価額(JPY)'] * exchange_rate
            portfolio_df_display = portfolio_df_display.sort_values(by='評価額', ascending=False)
            
            asset_list_config = {
                "コイン名": "コイン名", "取引所": "取引所",
                "保有数量": st.column_config.NumberColumn(format="%.8f"),
                "現在価格": st.column_config.NumberColumn(f"現在価格 ({selected_currency.upper()})", format="%,.2f"),
                "評価額": st.column_config.NumberColumn(f"評価額 ({selected_currency.upper()})", format="%,.0f"),
            }

            edited_df = st.data_editor(
                portfolio_df_display[['コイン名', '取引所', '保有数量', '現在価格', '評価額']], 
                disabled=['コイン名', '取引所', '現在価格', '評価額'], 
                column_config=asset_list_config, 
                use_container_width=True, key="portfolio_editor", hide_index=True
            )
            
            update_triggered = False
            if not edited_df.equals(portfolio_df_display[['コイン名', '取引所', '保有数量', '現在価格', '評価額']]):
                merged_df = pd.merge(portfolio_df_before_edit, edited_df, on=['コイン名', '取引所'], suffixes=('_before', '_after'))
                for _, row in merged_df.iterrows():
                    if not np.isclose(row['保有数量_before'], row['保有数量_after']):
                        quantity_diff = row['保有数量_after'] - row['保有数量_before']
                        coin_name, exchange = row['コイン名'], row['取引所']
                        coin_id = next((cid for cid, name in name_map.items() if name == coin_name), None)
                        if coin_id:
                            transaction_type = "調整（増）" if quantity_diff > 0 else "調整（減）"
                            add_transaction_to_bq(datetime.now(timezone.utc), coin_id, coin_name, exchange, transaction_type, abs(quantity_diff), 0, 0, 0)
                            st.toast(f"{coin_name} ({exchange}) の数量を調整: {quantity_diff:+.8f}", icon="✍️")
                            update_triggered = True
            if update_triggered: st.rerun()
        else: st.info("保有資産はありません。")

    st.markdown("---")
    with st.expander("取引履歴の登録", expanded=False):
        with st.form("transaction_form", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            col1, col2, col3 = st.columns(3)
            with col1:
                transaction_date = st.date_input("取引日", datetime.now())
                selected_coin_name = st.selectbox("コイン種別", options=coin_options.keys())
            with col2:
                transaction_type = st.selectbox("売買種別", ["購入", "売却"])
                exchange = st.text_input("取引所", "Binance")
            with col3:
                quantity = st.number_input("数量", 0.0, format="%.8f")
                price = st.number_input("価格(JPY)", 0.0, format="%.2f")
                fee = st.number_input("手数料(JPY)", 0.0, format="%.2f")
            if st.form_submit_button("登録する"):
                coin_id = coin_options[selected_coin_name]
                coin_name = name_map.get(coin_id, selected_coin_name)
                dt_transaction_date = datetime.combine(transaction_date, datetime.min.time())
                add_transaction_to_bq(dt_transaction_date, coin_id, coin_name, exchange, transaction_type, quantity, price, fee, quantity * price)
                st.success(f"{coin_name}の{transaction_type}取引を登録しました。")
                st.rerun()

    st.subheader("🗒️ 取引履歴")
    if not transactions_df.empty:
        history_config = {
            "取引日": st.column_config.DatetimeColumn(format="YYYY/MM/DD HH:mm"), 
            "数量": st.column_config.NumberColumn(format="%.6f"), 
            "価格(JPY)": st.column_config.NumberColumn(format="%,.2f")
        }
        st.dataframe(
            transactions_df[['取引日', 'コイン名', '取引所', '売買種別', '数量', '価格(JPY)']],
            hide_index=True, use_container_width=True,
            column_config=history_config)
    else: st.info("まだ取引履歴がありません。")

    st.markdown("---")
    st.subheader("⚙️ データベース管理")
    with st.expander("データベースリセット（危険）"):
        st.warning("**警告**: この操作はデータベース上のすべての取引履歴を完全に削除します。この操作は取り消せません。")
        if st.session_state.get('confirm_delete', False):
            st.error("本当によろしいですか？最終確認です。")
            col1, col2 = st.columns(2)
            if col1.button("はい、すべてのデータを削除します", type="primary"):
                reset_bigquery_table()
                st.session_state.confirm_delete = False
                st.rerun()
            if col2.button("いいえ、キャンセルします"):
                st.session_state.confirm_delete = False
                st.rerun()
        else:
            if st.button("すべての取引履歴をリセットする", type="primary"):
                st.session_state.confirm_delete = True
                st.rerun()

with tab2:
    st.header("⭐ ウォッチリスト")
    st.info("ここに仮想通貨一覧・ランキング機能を実装する予定です。")
    st.subheader(f"現在の仮想通貨価格 ({selected_currency.upper()})")
    watchlist_df = crypto_data_jpy.copy()
    watchlist_df['現在価格'] = watchlist_df['price_jpy'] * exchange_rate
    
    watchlist_config = {
        "symbol": "シンボル", "name": "コイン名",
        "現在価格": st.column_config.NumberColumn(f"現在価格 ({selected_currency.upper()})", format="%,.2f"),
        "price_change_percentage_24h": st.column_config.NumberColumn("24h変動率 (%)", format="%.2f")
    }
    st.dataframe(
        watchlist_df.sort_values(by='price_jpy', ascending=False)[['symbol', 'name', '現在価格', 'price_change_percentage_24h']],
        hide_index=True, use_container_width=True,
        column_config=watchlist_config
    )
