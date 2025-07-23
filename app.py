import dash
from dash import html, dcc, dash_table
from dash.dependencies import Input, Output
import sqlite3
import pandas as pd
import plotly.graph_objects as go

# Путь до базы данных
DB_PATH = "data/futures_spreads.db"

# Список дат экспираций у фьючерсов
LIST_EXPIRATIONS = ['9.25', '12.25', '3.26', '6.26']  # или None для пустого выбора

# === Подключение к БД и загрузка данных ===

def get_unique_expirations():
    """Получаем уникальные экспирации из spreads (например, 6.25, 9.25)"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT DISTINCT name_future FROM spreads", conn)
    conn.close()

    expirations = df['name_future'].str.extract(r'-(\d+\.\d+)')[0].dropna().unique()
    return sorted(expirations)


def load_data(expiration_list=None):
    """Загружает данные из таблицы spreads с фильтром по экспирации"""
    conn = sqlite3.connect(DB_PATH)

    query = "SELECT trade_time, name_future, kerry_buy_spread_y, kerry_sell_spread_y FROM spreads WHERE 1=1"
    params = []

    if expiration_list:
        placeholders = []
        for exp in expiration_list:
            placeholders.append(f"name_future LIKE ?")
            params.append(f"%-{exp}")
        query += " AND (" + " OR ".join(placeholders) + ")"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if not df.empty:
        df['trade_time'] = pd.to_datetime(df['trade_time'], format='%d.%m.%Y %H:%M:%S')

    return df.sort_values('trade_time')


def get_unique_future_expirations():
    """Получаем уникальные экспирации из future_spreads"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT DISTINCT far_future FROM future_spreads", conn)
    conn.close()

    expirations = df['far_future'].str.extract(r'-(\d+\.\d+)')[0].dropna().unique()
    return sorted(expirations)


def get_all_futures():
    """Получаем все уникальные значения name_future из таблицы spreads"""
    conn = sqlite3.connect("data/futures_spreads.db")
    df = pd.read_sql_query("SELECT DISTINCT name_future FROM spreads ORDER BY name_future", conn)
    conn.close()
    return df["name_future"].dropna().tolist()


def load_future_spreads(expiration_list=None):
    """Загружает данные из future_spreads с фильтром по экспирации"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM future_spreads WHERE 1=1"
    params = []

    if expiration_list:
        placeholders = []
        for exp in expiration_list:
            placeholders.append(f"far_future LIKE ?")
            params.append(f"%-{exp}")
        query += " AND (" + " OR ".join(placeholders) + ")"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if not df.empty:
        df['trade_time'] = pd.to_datetime(df['trade_time'], format='%d.%m.%Y %H:%M:%S')

    return df.sort_values('trade_time')


# === Визуализация графиков и таблиц для spreads ===

def create_spread_graphs(df, sort_by='kerry_buy_spread_y'):
    graphs = []
    latest_df = df.drop_duplicates('name_future', keep='last').sort_values(sort_by, ascending=False)

    # Список фьючерсов в нужном порядке
    sorted_futures = latest_df['name_future'].tolist()

    for future_name in sorted_futures:
        group = df[df['name_future'] == future_name]
        row = latest_df[latest_df['name_future'] == future_name].iloc[0]
        buy = row['kerry_buy_spread_y']
        sell = row['kerry_sell_spread_y']
    
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=group['trade_time'],
            y=group['kerry_buy_spread_y'],
            mode='lines+markers',
            name='Buy Spread',
            line=dict(color='green'),
            hovertemplate="Дата: %{x}<br>Buy спред: %{y:.2f}%<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=group['trade_time'],
            y=group['kerry_sell_spread_y'],
            mode='lines+markers',
            name='Sell Spread',
            line=dict(color='red'),
            hovertemplate="Дата: %{x}<br>Sell спред: %{y:.2f}%<extra></extra>"
        ))
    
        fig.update_layout(
            title_text=f"{future_name} | Buy: {buy:.2f}% | Sell: {sell:.2f}%",
            height=300,
            showlegend=True,
            template="plotly_white",
            margin=dict(l=10, r=10, t=40, b=20)
        )
    
        fig.update_yaxes(title_text="% годовых")
        fig.update_xaxes(title_text="Дата")
    
        graphs.append(html.Div([
            dcc.Graph(figure=fig)
        ]))

    return graphs


def create_current_spreads_table(df_last):
    current_df = df_last[['name_future', 'kerry_buy_spread_y', 'kerry_sell_spread_y', 'trade_time']]
    current_df['trade_time'] = current_df['trade_time'].dt.strftime('%d.%m.%Y')
    current_df = current_df.rename(columns={
        'name_future': 'Фьючерс',
        'kerry_buy_spread_y': 'Buy Spread (%)',
        'kerry_sell_spread_y': 'Sell Spread (%)',
        'trade_time': 'Обновлено'
    }).round(2)

    table = dash_table.DataTable(
        data=current_df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in current_df.columns],
        sort_action='native',
        sort_by=[{'column_id': 'Buy Spread (%)', 'direction': 'desc'}],
        style_table={'overflowX': 'auto'},
        style_cell={'minWidth': '100px', 'width': '150px', 'maxWidth': '300px', 'textAlign': 'center'},
        page_size=10
    )

    return html.Div([
        html.H3("Текущие спреды между акцией и фьючерсом"),
        table
    ])


# === Визуализация графиков и таблиц для future_spreads ===

def create_future_spread_graphs(df, sort_by):
    graphs = []
    sorted_df = df.sort_values(by=sort_by, ascending=False)
    
    for _, row in sorted_df.iterrows():
        near = row['near_future']
        far = row['far_future']
    
        # Выбираем все записи этой пары из полного датафрейма
        pair_df = df[(df['near_future'] == near) & (df['far_future'] == far)]
        
        # Последние актуальыне данные для подписи в заголовке
        buy = pair_df['spread_bid_y'].values[-1]
        sell = pair_df['spread_offer_y'].values[-1]
    
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pair_df['trade_time'],
            y=pair_df['spread_bid_y'],
            mode='lines+markers',
            name='spread',
            line=dict(color='green'),
            hovertemplate="Дата: %{x}<br>Buy: %{y:.2f}%<extra></extra>"
        ))
        
        fig.add_trace(go.Scatter(
            x=pair_df['trade_time'],
            y=pair_df['spread_offer_y'],
            mode='lines+markers',
            name='spread',
            line=dict(color='red'),
            hovertemplate="Дата: %{x}<br>Sell:  %{y:.2f}%<extra></extra>"
        ))
    
        fig.update_layout(
            title_text=f"{near} - {far} |  Buy: {buy:.2f}% | Sell: {sell:.2f}%",
            height=300,
            showlegend=True,
            template="plotly_white",
            margin=dict(l=50, r=50, t=40, b=20),
        )
    
        fig.update_yaxes(title_text="% годовых")
        fig.update_xaxes(title_text="Дата")
    
        graphs.append(html.Div([dcc.Graph(figure=fig)]))

    return graphs


def create_current_future_spreads_table(df_last):
    current_df = df_last[['near_future', 'far_future', 'spread_bid_y', 'spread_offer_y', 'trade_time']].copy(deep=True)
    current_df['trade_time'] = current_df['trade_time'].dt.strftime('%d.%m.%Y')
    current_df = current_df.rename(columns={
        'spread_bid_y': 'Buy Spread (%)',
        'spread_offer_y': 'Sell Spread (%)',
        'trade_time': 'Обновлено'
    }).round(2)

    table = dash_table.DataTable(
        data=current_df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in current_df.columns],
        sort_action='native',
        sort_by=[{'column_id': 'Buy Spread (%)', 'direction': 'desc'}],
        style_table={'overflowX': 'auto'},
        style_cell={'minWidth': '100px', 'width': '150px', 'maxWidth': '300px', 'textAlign': 'center'},
        page_size=10
    )

    return html.Div([
        html.H3("Текущие спреды между фьючерсами"),
        table
    ])


# === Основной интерфейс Dash ===

app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    html.H2("Мониторинг спредов", style={"textAlign": "center"}),

    dcc.Tabs(id='tabs', value='tab-spreads', children=[
        dcc.Tab(label='Спред между фьючерсом и акцией', value='tab-spreads'),
        dcc.Tab(label='Спред между фьючерсами', value='tab-future-spreads')
    ]),

    html.Div(id='content')
])


# === Callback для переключения вкладок ===

@app.callback(
    Output('content', 'children'),
    Input('tabs', 'value')
)
def render_content(tab):
    if tab == 'tab-spreads':
        return html.Div([
            html.H3("Спреды между акцией и фьючерсом", className="header-title"),

            html.Div([
                html.Label("Фьючерс", className="input-label"),
                dcc.Dropdown(
                    id='dropdown-future',
                    options=[{'label': f, 'value': f} for f in get_all_futures()],
                    value=None,  # По умолчанию — все фьючерсы
                    multi=True,
                    placeholder="Все фьючерсы",
                    style={'width': '100%', 'maxWidth': '160px'}
                ),
                
                html.Label("Экспирация", className="input-label"),
                dcc.Dropdown(
                    id='dropdown-expiration',
                    options=[{'label': f"{exp}", 'value': exp} for exp in get_unique_expirations()],
                    value=LIST_EXPIRATIONS,
                    multi=True,
                    style={'width': '100%', 'maxWidth': '300px'}
                ),

                html.Label("Сортировать по", className="input-label"),
                dcc.Dropdown(
                    id='dropdown-sort-by',
                    options=[
                        {'label': 'Лучший % спроса (Продать спред)', 'value': 'kerry_buy_spread_y'},
                        {'label': 'Лучший % предложения (Купить спред)', 'value': 'kerry_sell_spread_y'}
                    ],
                    value='kerry_buy_spread_y',
                    clearable=False,
                    style={'width': '100%', 'maxWidth': '300px'}
                ),
                
                html.Label("Мин. Buy Spread (%)", className="input-label"),
                dcc.Input(
                    id='input-min-buy-spread',
                    type='number',
                    value=0.0,
                    step=0.1,
                ),
                html.Label("Макс. Buy Spread (%)", className="input-label"),
                dcc.Input(
                    id='input-max-buy-spread',
                    type='number',
                    value=100.0,
                    step=0.1,
                )
            ], style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px', 'margin-bottom': '20px'}),

            html.Div(id='table-container'),
            html.Div(id='graphs-container')
        ])

    elif tab == 'tab-future-spreads':
        return html.Div([
            html.H3("Спреды между фьючерсами", className="header-title"),

            html.Div([
                html.Label("Экспирация", className="input-label"),
                dcc.Dropdown(
                    id='dropdown-expiration-futures',
                    options=[{'label': f"{exp}", 'value': exp} for exp in get_unique_future_expirations()],
                    value=LIST_EXPIRATIONS,
                    multi=True,
                    style={'width': '100%', 'maxWidth': '300px'}
                ),

                html.Label("Сортировка"),
                dcc.Dropdown(
                    id='dropdown-sort-by',
                    options=[
                        {'label': 'Лучший % спроса (Продать спред)', 'value': 'spread_bid_y'},
                        {'label': 'Лучший % предложения (Купить спред)', 'value': 'spread_offer_y'}
                    ],
                    value='spread_bid_y',
                    clearable=False,
                    style={'width': '100%', 'maxWidth': '300px'}
                )
            ], style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px', 'margin-bottom': '20px'}),

            html.Div(id='future-table-container'),
            html.Div(id='future-graphs-container')
        ])

    return html.Div("Неизвестная вкладка")


# === Callback для первой вкладки (spreads) ===

@app.callback(
    [Output('graphs-container', 'children'),
     Output('table-container', 'children')],
    [Input('dropdown-future', 'value'),
     Input('dropdown-expiration', 'value'),
     Input('dropdown-sort-by', 'value'),
     Input('input-min-buy-spread', 'value'),
     Input('input-max-buy-spread', 'value')]
)
def update_spreads(selected_futures, expiration_list, sort_by, min_buy_spread, max_buy_spread):
    df_full = load_data(expiration_list)

    if df_full.empty:
        return [
            html.Div("Нет данных для отображения графиков", style={"textAlign": "center"}),
            html.Div("Нет данных для отображения таблицы", style={"textAlign": "center"})
        ]
    
    # Фильтр по конкретным фьючерсам (если выбраны)
    if selected_futures:
        df_full = df_full[df_full['name_future'].isin(selected_futures)]
    
    # Фильтр по диапазону kerry_buy_spread_y
    min_buy = min_buy_spread or 0.0
    max_buy = max_buy_spread or 100.0
    df_filtered = df_full[
        (df_full['kerry_buy_spread_y'] >= min_buy) &
        (df_full['kerry_buy_spread_y'] <= max_buy)
    ]

    if df_filtered.empty:
        return [
            html.Div("Нет данных, удовлетворяющих фильтру", style={"textAlign": "center"}),
            html.Div("Нет данных, удовлетворяющих фильтру", style={"textAlign": "center"})
        ]

    # Получаем самые свежие значения
    df_last = df_filtered.sort_values('trade_time').drop_duplicates('name_future', keep='last')

    # Графики
    graphs = create_spread_graphs(df_filtered, sort_by)

    # Таблица
    table = create_current_spreads_table(df_last)

    return graphs, table


# === Callback для второй вкладки (future_spreads) ===

@app.callback(
    [Output('future-graphs-container', 'children'),
     Output('future-table-container', 'children')],
    [Input('dropdown-expiration-futures', 'value'),
     Input('dropdown-sort-by', 'value')]
)
def update_future_spreads(expiration_list, sort_by):
    df_full = load_future_spreads(expiration_list)

    if df_full.empty:
        return [
            html.Div("Нет данных для графиков", style={"textAlign": "center"}),
            html.Div("Нет данных для таблицы", style={"textAlign": "center"})
        ]

    # Самые свежие записи
    df_last = df_full.sort_values('trade_time').drop_duplicates(['near_future', 'far_future'], keep='last')

    # Графики
    graphs = create_future_spread_graphs(df_full, sort_by)

    # Таблица
    table = create_current_future_spreads_table(df_last)

    return graphs, table


# === Запуск сервера ===

if __name__ == '__main__':
    app.run(debug=True)