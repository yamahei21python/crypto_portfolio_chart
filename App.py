# -- coding: utf-8 --
"""
仮想通貨ポートフォリオ管理Streamlitアプリケーション (アカウント機能付き)

このアプリケーションは、ユーザーの仮想通貨取引履歴を記録・管理し、
現在の資産状況をリアルタイムで可視化するためのツールです。

主な機能:
- ★アカウント作成、ログイン、ログアウト機能
- ★ユーザーごとのポートフォリオ、ウォッチリスト管理
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
import re
import bcrypt # ★パスワードハッシュ化のために追加

# === 2. 定数・グローバル設定 ===
# --- BigQuery関連 ---
PROJECT_ID = "cyptodb"
DATASET_ID = "coinalyze_data"
TABLE_USERS = "users" # ★ユーザーテーブルを追加
TABLE_TRANSACTIONS = "transactions"
TABLE_WATCHLIST = "watchlist"
TABLE_USERS_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_USERS}"
TABLE_TRANSACTIONS_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_TRANSACTIONS}"
TABLE_WATCHLIST_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_WATCHLIST}"

# ★★★ スキーマ定義の変更 ★★★
BIGQUERY_SCHEMA_USERS = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("password_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]
BIGQUERY_SCHEMA_TRANSACTIONS = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"), # ★user_idカラムを追加
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
# ★★★ ここまで ★★★

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
BLACK_THEME_CSS = "..." # (変更なしのため省略)

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

# === ★ 4. 認証関連関数 (新規追加) ★ ===
def hash_password(password: str) -> bytes:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def verify_password(plain_password: str, hashed_password: bytes) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def get_user_from_bq(user_id: str) -> Any | None:
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
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    errors = bq_client.insert_rows_json(TABLE_USERS_FULL_ID, [user_data])
    return not errors

# === 5. BigQuery 操作関数 (ユーザーID対応) ===
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
    transaction_data["user_id"] = user_id # ★ユーザーIDを追加
    transaction_data["transaction_date"] = datetime.now(timezone.utc).isoformat()
    errors = bq_client.insert_rows_json(TABLE_TRANSACTIONS_FULL_ID, [transaction_data])
    return not errors

def delete_transaction_from_bq(user_id: str, transaction: pd.Series) -> bool:
    if not bq_client: return False
    query = f"""
    DELETE FROM `{TABLE_TRANSACTIONS_FULL_ID}`
    WHERE user_id = @user_id AND transaction_date = @transaction_date AND coin_id = @coin_id
    AND exchange = @exchange AND transaction_type = @transaction_type AND quantity = @quantity
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id), # ★ユーザーIDを追加
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
        
def get_transactions_from_bq(user_id: str) -> pd.DataFrame:
    if not bq_client: return pd.DataFrame()
    query = f"""
        SELECT * FROM `{TABLE_TRANSACTIONS_FULL_ID}`
        WHERE user_id = @user_id
        ORDER BY transaction_date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=False)
        if df.empty: return pd.DataFrame()
        # user_idカラムは表示しない
        df = df.drop(columns=['user_id'], errors='ignore')
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_convert('Asia/Tokyo')
        return df.rename(columns=COLUMN_NAME_MAP_JA)
    except google.api_core.exceptions.NotFound:
        init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
        return pd.DataFrame()

# --- ウォッチリスト用 BigQuery 操作関数 (変更なし、user_id引数は元から存在) ---
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
    
    # ユーザー固有のデータのみ削除
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
# (このセクションの関数は変更なし)
# get_full_market_data, get_exchange_rate, calculate_portfolio, ...

# === 7. UIコンポーネント & ヘルパー関数 (ユーザーID対応) ===
# (format_price, display_summary_card などの表示系ヘルパーは変更なし)

def display_add_transaction_form(user_id: str, market_data: pd.DataFrame, currency: str):
    with st.expander("新しい取引履歴を追加", expanded=False):
        coin_options = {row['id']: f"{row['name']} ({row['symbol'].upper()})" for _, row in market_data.iterrows()}
        name_map = market_data.set_index('id')['name'].to_dict()
        with st.form(key=f"transaction_form_{currency}", clear_on_submit=True):
            # (フォームの中身は変更なし)
            ...
            
            if st.form_submit_button("この内容で登録する"):
                transaction = {
                    "transaction_date": datetime.combine(date, datetime.min.time()),
                    "coin_id": coin_disp, "coin_name": name_map.get(coin_disp, coin_disp),
                    "exchange": exchange, "transaction_type": trans_type, "quantity": quantity,
                    "price_jpy": price, "fee_jpy": fee, "total_jpy": quantity * price
                }
                # ★user_idを渡すように変更
                if add_transaction_to_bq(user_id, transaction):
                    st.success(f"{transaction['coin_name']}の{trans_type}履歴を登録しました。")
                    st.rerun()

def display_transaction_history(user_id: str, transactions_df: pd.DataFrame, currency: str):
    st.subheader("🗒️ 登録履歴一覧")
    if transactions_df.empty:
        st.info("まだ登録履歴がありません。")
        return
    
    for index, row in transactions_df.iterrows():
        unique_key = f"{currency}_{index}"
        with st.container(border=True):
            # (表示部分は変更なし)
            ...
            with cols[1]:
                if st.button("削除 🗑️", key=f"del_{unique_key}", use_container_width=True, help="この履歴を削除します"):
                    # ★user_idを渡すように変更
                    if delete_transaction_from_bq(user_id, row):
                        st.toast(f"履歴を削除しました: {row['登録日'].strftime('%Y/%m/%d')}の{row['コイン名']}", icon="🗑️")
                        st.rerun()

# === 8. ページ描画関数 (ユーザーID対応) ===
def render_portfolio_page(user_id: str, jpy_market_data: pd.DataFrame, currency: str, rate: float):
    # ★user_idを使ってデータを取得
    transactions_df = get_transactions_from_bq(user_id)
    
    portfolio, total_asset_jpy, total_change_jpy = calculate_portfolio(transactions_df, jpy_market_data)
    # (以降の描画ロジックは変更なし、ただし下位関数に user_id を渡す)
    ...
    with tab_history:
        display_transaction_history(user_id, transactions_df, currency)
        display_add_transaction_form(user_id, jpy_market_data, currency)

def render_custom_watchlist(user_id: str, market_data: pd.DataFrame, currency: str, rate: float):
    # ★引数で受け取った user_id を使用
    watchlist_db = get_watchlist_from_bq(user_id)
    
    if not watchlist_db.empty:
        # ... (描画部分は変更なし)
    else:
        st.info("カスタムウォッチリストは空です。下の編集エリアから銘柄を追加してください。")
    
    st.divider()
    with st.container(border=True):
        # ... (編集フォーム部分は変更なし)
        
        if st.button("この内容でウォッチリストを保存"):
            # ★引数で受け取った user_id を使用
            update_watchlist_in_bq(user_id, selected_coins)
            st.toast("ウォッチリストを更新しました。")
            st.cache_data.clear()
            st.rerun()

def render_watchlist_page(user_id: str, jpy_market_data: pd.DataFrame):
    # ... (通貨切り替え部分は変更なし)
    with tab_custom:
        # ★user_id を渡す
        render_custom_watchlist(user_id, jpy_market_data, vs_currency, rate)
    
# === 9. 認証画面描画関数 (新規追加) ===
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
                user_data = get_user_from_bq(user_id)
                if user_data and verify_password(password, user_data['password_hash'].encode('utf-8')):
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
                    # エラーメッセージはcreate_user_in_bq内で表示

# === 10. メイン処理 (認証フローを組み込み) ===
def main():
    st.markdown(BLACK_THEME_CSS, unsafe_allow_html=True)
    
    # --- セッションステートの初期化 ---
    st.session_state.setdefault('authenticated', False)
    st.session_state.setdefault('user_id', None)
    st.session_state.setdefault('balance_hidden', False)
    st.session_state.setdefault('currency', 'jpy')
    st.session_state.setdefault('watchlist_currency', 'jpy')
    
    if not bq_client: st.stop()
    
    # --- 認証チェック ---
    if not st.session_state.authenticated:
        init_bigquery_table(TABLE_USERS_FULL_ID, BIGQUERY_SCHEMA_USERS) # ユーザーテーブルの初期化
        render_auth_page()
        st.stop() # ログインするまでここで停止

    # --- ログイン後のメインアプリケーション ---
    user_id = st.session_state.user_id

    # サイドバーにログアウト機能を追加
    with st.sidebar:
        st.success(f"{user_id} でログイン中")
        if st.button("ログアウト", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.user_id = None
            st.toast("ログアウトしました。")
            st.rerun()
        st.divider()
        st.write("表示設定")

    jpy_market_data = get_full_market_data(currency='jpy')
    if jpy_market_data.empty:
        st.error("市場データを取得できませんでした。"); st.stop()
    
    # ログイン後にテーブルを初期化
    init_bigquery_table(TABLE_TRANSACTIONS_FULL_ID, BIGQUERY_SCHEMA_TRANSACTIONS)
    init_bigquery_table(TABLE_WATCHLIST_FULL_ID, BIGQUERY_SCHEMA_WATCHLIST)

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
