import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DB_PATH = "data/futures_spreads.db"


def visualize_future_spreads_interactive(db_path, near_future_filter=None):
    """
    Строит интерактивный график спредов между фьючерсами на одну акцию.

    :param db_path: Путь к SQLite базе данных
    :param near_future_filter: Опциональный фильтр по акции (например 'VTBR-9.25')
    """
    with sqlite3.connect(db_path) as conn:
        query = "SELECT * FROM future_spreads ORDER BY 'trade_time'"
        if near_future_filter:
            query += f" WHERE near_future = '{near_future_filter}'"
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("Нет данных для отображения.")
        return

    # Добавляем колонку для легенды (near + far)
    df['pair'] = df['near_future'] + ' → ' + df['far_future']
    
    # Получаем список уникальных пар
    unique_pairs = df['pair'].unique()
    
    # Создаём подграфики — один график на каждую пару
    fig = make_subplots(
        rows=len(unique_pairs),
        cols=1,
        shared_xaxes=True,
        subplot_titles=[f"{pair}" for pair in unique_pairs],
        # vertical_spacing=0.05
    )

    for idx, pair in enumerate(unique_pairs):
        pair_df = df[df['pair'] == pair]
    
        fig.add_trace(
            go.Scatter(
                x=pair_df['trade_time'],
                y=pair_df['spread_bid_y'],
                mode='lines+markers',
                name=pair,
                hovertext=pair_df[['spread_bid_y', 'spread_offer_y']].apply(
                    lambda row: f"spread_bid_y: {row[0]}%, spread_offer_y: {row[1]}", axis=1
                )
            ),
            row=idx + 1,
            col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=pair_df['trade_time'],
                y=pair_df['spread_offer_y'],
                mode='lines+markers',
                name=pair,
                hovertext=pair_df[['spread_bid_y', 'spread_offer_y']].apply(
                    lambda row: f"spread_bid_y: {row[0]}%, spread_offer_y: {row[1]}", axis=1
                )
            ),
            row=idx + 1,
            col=1
        )

    fig.update_layout(
        title_text=f"Спреды между фьючерсами ({near_future_filter or 'Все акции'})",
        height=400 * len(unique_pairs),  # Высота зависит от количества графиков
        showlegend=False,
        template="plotly_white"
    )
    
    fig.update_yaxes(title_text="Годовая доходность (%)")
    fig.update_xaxes(title_text="Дата")

    fig.write_html("./data/graph_futures.html")
    fig.show()
    
    
if __name__ == '__main__':
    visualize_future_spreads_interactive(DB_PATH)