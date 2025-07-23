import streamlit as st
import pandas as pd
import plotly.express as px
from pycoingecko import CoinGeckoAPI
from datetime import datetime

# --- 初期設定 ---
st.set_page_config(
    page_title="仮想通貨ポートフォリ管理",
    page_icon="🪙",
    layout="wide"
)

# --- APIクライアントの初期化 ---
cg = CoinGeckoAPI()

# --- 関数定義 ---
@st.cache_data(ttl=600)  # 10分間キャッシュを保持
def get_crypto_data():
    """CoinGecko APIから時価総額上位20の仮想通貨データを取得する"""
    try:
        # vs_currency='jpy'で日本円建ての価格を取得
        data = cg.get_coins_markets(
            vs_currency='jpy',
            order='market_cap_desc',
            per_page=20,
            page=1
        )
        # 必要な情報だけを抽出したDataFrameを作成
        df = pd.DataFrame(data, columns=['id', 'symbol', 'name', 'current_price'])
        return df
    except Exception as e:
        st.error(f"価格データの取得に失敗しました: {e}")
        return pd.DataFrame() # エラー時は空のDataFrameを返す

# --- セッションステートの初期化 ---
if 'transactions' not in st.session_state:
    # 取引履歴を保存するためのDataFrameを初期化
    st.session_state.transactions = pd.DataFrame(columns=[
        "取引日", "コインID", "コイン名", "売買種別", "数量", "価格(JPY)", "手数料(JPY)", "合計(JPY)"
    ])

# --- データ取得 ---
crypto_data = get_crypto_data()

# 価格データが取得できなかった場合は処理を中断
if crypto_data.empty:
    st.stop()

# コイン名とID、価格のマッピングを作成
# フォームでの選択用に「コイン名 (シンボル)」のリストを作成
coin_options = {f"{row['name']} ({row['symbol'].upper()})": row['id'] for index, row in crypto_data.iterrows()}
# IDをキーにした価格辞書
price_map = crypto_data.set_index('id')['current_price'].to_dict()
# IDをキーにしたコイン名辞書
name_map = crypto_data.set_index('id')['name'].to_dict()


# --- アプリケーションのタイトル ---
st.title("🪙 仮想通貨ポートフォリオ管理")
st.caption("CoinGecko APIを利用して、時価総額上位20の仮想通貨に対応しています。")

# --- タブUIの作成 ---
tab1, tab2 = st.tabs(["ポートフォリオ", "ウォッチリスト"])


# --- ポートフォリオタブ ---
with tab1:
    # --- ポートフォリオ計算 ---
    transactions_df = st.session_state.transactions
    portfolio = {}
    total_investment = 0
    total_asset_value = 0
    
    if not transactions_df.empty:
        # コインごとの保有数量と平均取得単価を計算
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
                
                # ポートフォリオに追加
                portfolio[coin_id] = {
                    "コイン名": name_map.get(coin_id, coin_id),
                    "保有数量": current_quantity,
                    "現在価格": current_price,
                    "評価額": current_value
                }
                
                # 各コインの投資額（購入額 - 売却額）を計算
                investment_cost = buy_cost - sell_proceeds
                total_investment += investment_cost
                total_asset_value += current_value

    # 評価損益の計算
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
    col1, col2 = st.columns([1, 1.2]) # カラムの幅を調整

    with col1:
        st.subheader("📊 資産割合")
        if portfolio:
            portfolio_df = pd.DataFrame.from_dict(portfolio, orient='index')
            # 評価額が0より大きい資産のみを円グラフに表示
            display_df = portfolio_df[portfolio_df['評価額'] > 0]
            if not display_df.empty:
                fig = px.pie(
                    display_df, 
                    values='評価額', 
                    names='コイン名', 
                    title='各コインの資産割合',
                    hole=0.3
                )
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
                column_config={
                    "保有数量": st.column_config.NumberColumn(format="%.6f"),
                    "現在価格": st.column_config.NumberColumn(format="¥%,.2f"),
                    "評価額": st.column_config.NumberColumn(format="¥%,.0f"),
                },
                use_container_width=True
            )
        else:
            st.info("保有資産はありません。")

    st.markdown("---")

    # --- 取引履歴の登録フォーム ---
    with st.expander("取引履歴の登録", expanded=False):
        with st.form("transaction_form", clear_on_submit=True):
            st.subheader("新しい取引を登録")
            
            # 入力フォーム
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
                # フォームの入力値を処理
                coin_id = coin_options[selected_coin_name]
                coin_name = name_map.get(coin_id, coin_id)
                total_amount = quantity * price
                
                new_transaction = pd.DataFrame([{
                    "取引日": pd.to_datetime(transaction_date),
                    "コインID": coin_id,
                    "コイン名": coin_name,
                    "売買種別": transaction_type,
                    "数量": quantity,
                    "価格(JPY)": price,
                    "手数料(JPY)": fee,
                    "合計(JPY)": total_amount
                }])

                # セッションステートのDataFrameに追加
                st.session_state.transactions = pd.concat(
                    [st.session_state.transactions, new_transaction],
                    ignore_index=True
                )
                st.success(f"{coin_name}の{transaction_type}取引を登録しました。")

    # --- 取引履歴の一覧表示 ---
    st.subheader("🗒️ 取引履歴")
    if not st.session_state.transactions.empty:
        # 日付の降順で表示
        display_transactions = st.session_state.transactions.sort_values(by="取引日", ascending=False)
        st.dataframe(
            display_transactions,
            hide_index=True,
            use_container_width=True,
            column_config={
                "取引日": st.column_config.DateColumn("取引日", format="YYYY/MM/DD"),
                "数量": st.column_config.NumberColumn(format="%.6f"),
                "価格(JPY)": st.column_config.NumberColumn(format="¥%,.2f"),
                "手数料(JPY)": st.column_config.NumberColumn(format="¥%,.2f"),
                "合計(JPY)": st.column_config.NumberColumn(format="¥%,.0f"),
            }
        )
    else:
        st.info("まだ取引履歴がありません。")

# --- ウォッチリストタブ ---
with tab2:
    st.header("⭐ ウォッチリスト")
    # ここに仮想通貨一覧・ランキング機能を実装
    st.info("ここに仮想通貨一覧・ランキング機能を実装する予定です。")
    
    st.subheader("現在の仮想通貨価格（時価総額トップ20）")
    st.dataframe(
        crypto_data.drop(columns=['id']), # ID列は非表示
        hide_index=True,
        use_container_width=True,
        column_config={
            "symbol": "シンボル",
            "name": "コイン名",
            "current_price": st.column_config.NumberColumn("現在価格 (JPY)", format="¥%,.2f"),
        }
    )
