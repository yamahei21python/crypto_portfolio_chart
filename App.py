import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime
import sqlite3 # ### DB変更点 ###: sqlite3をインポート

# --- DB設定 --- ### DB変更点 ###
DB_FILE = "portfolio.db"

def get_db_connection():
    """データベース接続を取得する"""
    return sqlite3.connect(DB_FILE)

def init_db():
    """データベースを初期化し、テーブルを作成する"""
    conn = get_db_connection()
    c = conn.cursor()
    # 取引履歴を保存するテーブルを作成
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
    # データを挿入
    c.execute('''
        INSERT INTO transactions (transaction_date, coin_id, coin_name, transaction_type, quantity, price_jpy, fee_jpy, total_jpy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (date.isoformat(), coin_id, coin_name, type, qty, price, fee, total))
    conn.commit()
    conn.close()

def get_transactions_from_db():
    """データベースから全ての取引履歴を取得し、DataFrameとして返す"""
    conn = get_db_connection()
    # SQLクエリでデータを読み込み、Pandas DataFrameに変換
    df = pd.read_sql_query("SELECT * FROM transactions", conn)
    conn.close()
    
    # データ型の変換と列名の調整
    if not df.empty:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df = df.rename(columns={
            'transaction_date': '取引日',
            'coin_id': 'コインID',
            'coin_name': 'コイン名',
            'transaction_type': '売買種別',
            'quantity': '数量',
            'price_jpy': '価格(JPY)',
            'fee_jpy': '手数料(JPY)',
            'total_jpy': '合計(JPY)'
        }).drop(columns=['id']) # id列は表示に不要なため削除
    else:
        # 空の場合でも列構成を合わせておく
        df = pd.DataFrame(columns=[
            "取引日", "コインID", "コイン名", "売買種別", "数量", "価格(JPY)", "手数料(JPY)", "合計(JPY)"
        ])
    return df

# --- DB初期化 --- ### DB変更点 ###
init_db()


# --- 初期設定 ---
st.set_page_config(
    page_title="仮想通貨ポートフォリ管理",
    page_icon="🪙",
    layout="wide"
)

# --- APIクライアントの初期化 ---
cg = CoinGeckoAPI()

# --- 関数定義 ---
@st.cache_data(ttl=600)
def get_crypto_data():
    try:
        data = cg.get_coins_markets(vs_currency='jpy', order='market_cap_desc', per_page=20, page=1)
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'current_price'])
        return df
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame()

# --- データ取得 ---
crypto_data = get_crypto_data()
if crypto_data.empty:
    st.stop()

coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for index, row in crypto_data.iterrows()}
price_map = crypto_data.set_index('id')['current_price'].to_dict()
name_map = crypto_data.set_index('id')['name'].to_dict()

# --- アプリケーションのタイトル ---
st.title("🪙 仮想通貨ポートフォリオ管理")
st.caption("CoinGecko APIを利用して、時価総額上位20の仮想通貨に対応しています。")

# --- タブUIの作成 ---
tab1, tab2 = st.tabs(["ポートフォリオ", "ウォッチリスト"])

# --- ポートフォリオタブ ---
with tab1:
    # ### DB変更点 ###: セッションステートからではなく、DBから直接データを読み込む
    transactions_df = get_transactions_from_db()

    # --- ポートフォリオ計算 (この部分のロジックは変更なし) ---
    portfolio = {}
    total_investment = 0
    total_asset_value = 0
    
    if not transactions_df.empty:
        for coin_id in transactions_df['コインID'].unique():
            coin_tx = transactions_df[transactions_df['コインID'] == coin_id]
            
            buy_quantity = coin_tx[coin_tx['売買種別'] == '購入']['数量'].sum()
            sell_quantity = coin_tx[coin_tx['売買種別'] == '売却']['数量'].sum()
            
            buy_cost = (coin_tx[coin_tx['売買種別'] == '購入']['数量'] * coin_tx[coin_tx['売買種別'] == '購入']['価格(JPY)']).sum()
            sell_proceeds = (coin_tx[coin_tx['売買種別'] == '売却']['数量'] * coin_tx[coin_tx['売買種別'] == '売却']['価格(JPY)']).sum()

            current_quantity = buy_quantity - sell_quantity
            
            if current_quantity > 0:
                current_price = price_map.get(coin_id, 0)
                current_value = current_quantity * current_price
                
                portfolio[coin_id] = {
                    "コイン名": name_map.get(coin_id, coin_id),
                    "保有数量": current_quantity,
                    "現在価格": current_price,
                    "評価額": current_value
                }
                
                investment_cost = buy_cost - sell_proceeds
                total_investment += investment_cost
                total_asset_value += current_value

    profit_loss = total_asset_value - total_investment

    # --- サマリー表示 ---
    st.header("📈 ポートフォリオサマリー")
    col1, col2, col3 = st.columns(3)
    col1.metric("保有資産合計", f"¥{total_asset_value:,.0f}")
    col2.metric("評価損益", f"¥{profit_loss:,.0f}", delta=f"{profit_loss:,.0f} JPY")
    if total_investment > 0:
        col3.metric("損益率", f"{(profit_loss / total_investment) * 100:.2f}%")
    else:
        col3.metric("損益率", "N/A")

    st.markdown("---")

    # --- 保有資産の内訳 ---
    col1, col2 = st.columns([1, 1.2])

    with col1:
        st.subheader("📊 資産割合")
        if portfolio:
            portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index')
            display_df = portfolio_df[portfolio_df['評価額'] > 0]
            if not display_df.empty:
                fig = px.pie(display_df, values='評価額', names='コイン名', title='各コインの資産割合', hole=0.3)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("保有資産がありません。")
        else:
            st.info("取引履歴を登録すると、ここにグラフが表示されます。")
    
    with col2:
        st.subheader("📋 保有資産一覧")
        if portfolio:
            portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index')
            st.dataframe(
                portfolio_df,
                column_config={"保有数量": st.column_config.NumberColumn(format="%.6f"),"現在価格": st.column_config.NumberColumn(format="¥%,.2f"),"評価額": st.column_config.NumberColumn(format="¥%,.0f"),},
                use_container_width=True
            )
        else:
            st.info("保有資産はありません。")

    st.markdown("---")

    # --- 取引履歴の登録フォーム ---
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
                fee = st.number_input("手数料 (JPY)", min_value=0.0, format="%.2f", help="取引にかかった手数料を入力します。")
            submitted = st.form_submit_button("登録する")

            if submitted:
                # ### DB変更点 ###: セッションステートではなくDBにデータを保存する
                coin_id = coin_options[selected_coin_name]
                coin_name = name_map.get(coin_id, coin_id)
                total_amount = quantity * price
                
                # データベースに関数を使ってデータを追加
                add_transaction_to_db(
                    transaction_date, 
                    coin_id, 
                    coin_name, 
                    transaction_type, 
                    quantity, 
                    price, 
                    fee, 
                    total_amount
                )
                
                st.success(f"{coin_name}の{transaction_type}取引を登録しました。")
                # データを再読み込みして画面を更新するために st.rerun() を実行
                st.rerun()

    # --- 取引履歴の一覧表示 ---
    st.subheader("🗒️ 取引履歴")
    if not transactions_df.empty:
        display_transactions = transactions_df.sort_values(by="取引日", ascending=False)
        st.dataframe(
            display_transactions,
            hide_index=True,
            use_container_width=True,
            column_config={"取引日": st.column_config.DateColumn("取引日", format="YYYY/MM/DD"),"数量": st.column_config.NumberColumn(format="%.6f"),"価格(JPY)": st.column_config.NumberColumn(format="¥%,.2f"),"手数料(JPY)": st.column_config.NumberColumn(format="¥%,.2f"),"合計(JPY)": st.column_config.NumberColumn(format="¥%,.0f"),}
        )
    else:
        st.info("まだ取引履歴がありません。")

# --- ウォッチリストタブ ---
with tab2:
    st.header("⭐ ウォッチリスト")
    st.info("ここに仮想通貨一覧・ランキング機能を実装する予定です。")
    st.subheader("現在の仮想通貨価格（時価総額トップ20）")
    st.dataframe(
        crypto_data.drop(columns=['id']),
        hide_index=True,
        use_container_width=True,
        column_config={"symbol": "シンボル","name": "コイン名","current_price": st.column_config.NumberColumn("現在価格 (JPY)", format="¥%,.2f"),}
    )
