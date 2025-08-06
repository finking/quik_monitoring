from dash import html, dcc, dash_table, Dash
from dash.dependencies import Input, Output, State
import logging
import sqlite3
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger('app.py')
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                    datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                    level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                    handlers=[logging.FileHandler('app_logs.log', encoding='utf-8'),
                              logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль

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

def create_spread_graphs(df_full, futures_on_page):
    """Создаем графики только для указанных фьючерсов"""
    graphs = []

    # Получаем последние значения только для фьючерсов на текущей странице
    df_last_page = df_full[
        df_full['name_future'].isin(futures_on_page)
    ].sort_values('trade_time').drop_duplicates('name_future', keep='last')

    # Сортируем в том же порядке, что и в таблице
    df_last_page = df_last_page.set_index('name_future').reindex(futures_on_page).reset_index()

    for _, row in df_last_page.iterrows():
        future_name = row['name_future']
        # Получаем все данные для этого фьючерса
        group = df_full[df_full['name_future'] == future_name]
        buy = row['kerry_buy_spread_y']
        sell = row['kerry_sell_spread_y']
    
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=group['trade_time'],
            y=group['kerry_buy_spread_y'],
            mode='lines+markers',
            name='Спрос',
            line=dict(color='green'),
            hovertemplate="Дата: %{x}<br>Продать спред: %{y:.2f}%<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=group['trade_time'],
            y=group['kerry_sell_spread_y'],
            mode='lines+markers',
            name='Предложение',
            line=dict(color='red'),
            hovertemplate="Дата: %{x}<br>Купить спред: %{y:.2f}%<extra></extra>"
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
        'kerry_buy_spread_y': 'Спрос (%)',
        'kerry_sell_spread_y': 'Предложение (%)',
        'trade_time': 'Обновлено'
    }).round(2)

    table = dash_table.DataTable(
        id='spreads-data-table',  # ID для отслеживания страниц
        data=current_df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in current_df.columns],
        sort_action='native',
        style_table={'overflowX': 'auto'},
        style_cell={'minWidth': '100px', 'width': '150px', 'maxWidth': '300px', 'textAlign': 'center'},
        page_size=10,  # Показываем по 10 записей на странице
        page_current=0,  # Начинаем с первой страницы
        page_action='native'  # Включаем пагинацию
    )

    return html.Div([
        html.H3("Текущие спреды между акцией и фьючерсом"),
        table
    ])


# === Вспомогательная функция для Future Spreads ===
def get_sorted_future_data(df_full, sort_by='spread_bid_y'):
    """
    Получает отсортированные последние значения из данных future spreads
    """
    if df_full.empty:
        return df_full

    # Самые свежие записи
    df_last = df_full.sort_values('trade_time').drop_duplicates(['near_future', 'far_future'], keep='last')
    
    df_last_sorted = df_last.sort_values(by=sort_by, ascending=False)
    
    return df_last_sorted


# === Визуализация графиков и таблиц для future_spreads ===

def create_future_spread_graphs(df_full, df_page):
    """
    Создаем графики для фьючерсов на текущей странице таблицы.
    df_full: полный отфильтрованный DataFrame
    df_page: DataFrame с фьючерсами для текущей страницы (из таблицы)
    """
    graphs = []
    
    # Убедимся, что df_page это DataFrame, а не Series
    if df_page.empty:
        return html.Div("Нет данных для отображения на этой странице", style={"textAlign": "center"})
    
    for _, row in df_page.iterrows():
        near = row['near_future']
        far = row['far_future']
    
        # Выбираем все записи этой пары из полного датафрейма
        pair_df = df_full[(df_full['near_future'] == near) & (df_full['far_future'] == far)]
        
        if pair_df.empty:
            continue
            
        # Последние актуальные данные для подписи в заголовке
        # Используем последнюю запись из pair_df для подписи
        last_row = pair_df.iloc[-1]
        buy = last_row.get('spread_bid_y', 0)
        sell = last_row.get('spread_offer_y', 0)
    
        fig = go.Figure()
        # Добавляем линии для spread_bid_y и spread_offer_y
        fig.add_trace(go.Scatter(
            x=pair_df['trade_time'],
            y=pair_df['spread_bid_y'],
            mode='lines+markers',
            name='Спрос',
            line=dict(color='green'),
            hovertemplate="Дата: %{x}<br>Продать спред: %{y:.2f}%<extra></extra>"
        ))
        
        fig.add_trace(go.Scatter(
            x=pair_df['trade_time'],
            y=pair_df['spread_offer_y'],
            mode='lines+markers',
            name='Предложение',
            line=dict(color='red'),
            hovertemplate="Дата: %{x}<br>Купить спред:  %{y:.2f}%<extra></extra>"
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
        
    if not graphs:
        return html.Div("Нет графиков для отображения", style={"textAlign": "center"})

    return graphs


def create_current_future_spreads_table(df_last_sorted):
    """
    Создает таблицу future spreads с пагинацией.
    """
    if df_last_sorted.empty:
        return html.Div("Нет данных для отображения таблицы", style={"textAlign": "center"})
    
    current_df = df_last_sorted[['near_future', 'far_future', 'spread_bid_y', 'spread_offer_y', 'trade_time']].copy(deep=True)
    current_df['trade_time'] = current_df['trade_time'].dt.strftime('%d.%m.%Y')
    current_df = current_df.rename(columns={
        'near_future': 'Ближний фьючерс',
        'far_future': 'Дальний фьючерс',
        'spread_bid_y': 'Спрос (%)',
        'spread_offer_y': 'Предложение (%)',
        'trade_time': 'Обновлено'
    }).round(2)

    table = dash_table.DataTable(
        id='future-spreads-data-table',  # Уникальный ID для таблицы future spreads
        data=current_df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in current_df.columns],
        sort_action='native',
        style_table={'overflowX': 'auto'},
        style_cell={'minWidth': '100px', 'width': '150px', 'maxWidth': '300px', 'textAlign': 'center'},
        page_size=10,  # Показываем по 10 записей на странице
        page_current=0,  # Начинаем с первой страницы
        page_action='native'  # Включаем пагинацию
    )

    return html.Div([
        html.H3("Текущие спреды между фьючерсами"),
        table
    ])


# === Основной интерфейс Dash ===

app = Dash(__name__, suppress_callback_exceptions=True)

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
                    style={'width': '100%', 'maxWidth': '310px', 'whiteSpace': 'nowrap'}
                ),
                
                html.Label("Мин. Спрос (%)", className="input-label"),
                dcc.Input(
                    id='input-min-buy-spread',
                    type='number',
                    value=0.0,
                    step=0.1,
                ),
                html.Label("Макс. Спрос (%)", className="input-label"),
                dcc.Input(
                    id='input-max-buy-spread',
                    type='number',
                    value=100.0,
                    step=0.1,
                )
            ], style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px', 'margin-bottom': '20px'}),
    
            # Хранилище для кэширования данных
            dcc.Store(id='stored-filtered-data'),
            dcc.Store(id='stored-sorted-data'),
            
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
                )
            ], style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px', 'margin-bottom': '20px'}),

            # Хранилище для кэширования данных future spreads
            dcc.Store(id='stored-future-filtered-data'),
            dcc.Store(id='stored-future-sorted-data'),
            
            html.Div(id='future-table-container'),
            html.Div(id='future-graphs-container')
        ])

    return html.Div("Неизвестная вкладка")


# === Callback'и для первой вкладки (spreads) ===
# --- Первый Callback: Обновление Таблицы ---
@app.callback(
    [Output('table-container', 'children'),
     Output('stored-filtered-data', 'data'),  # Кэшируем отфильтрованные данные
     Output('stored-sorted-data', 'data')],  # Кэшируем отсортированные данные
    [Input('dropdown-future', 'value'),
     Input('dropdown-expiration', 'value'),
     Input('dropdown-sort-by', 'value'),
     Input('input-min-buy-spread', 'value'),
     Input('input-max-buy-spread', 'value')]
)
def update_table(selected_futures,
                 expiration_list,
                 sort_by,
                 min_buy_spread,
                 max_buy_spread):
    logger.debug("Update_table called")
    
    # --- Начало логики фильтрации ---
    df_full = load_data(expiration_list)

    if df_full.empty:
        empty_result = html.Div("Нет данных для отображения таблицы", style={"textAlign": "center"})
        return empty_result, None, None
    
    # Фильтр по конкретным фьючерсам (если выбраны)
    if selected_futures:
        df_full = df_full[df_full['name_future'].isin(selected_futures)]

        # Сначала получаем последние значения для каждого фьючерса
    df_last = df_full.sort_values('trade_time').drop_duplicates('name_future', keep='last')

    # Затем применяем фильтр по диапазону kerry_buy_spread_y к ПОСЛЕДНИМ значениям
    try:
        min_val = float(min_buy_spread) if min_buy_spread not in (None, '') else -float('inf')
    except (ValueError, TypeError):
        min_val = -float('inf')

    try:
        max_val = float(max_buy_spread) if max_buy_spread not in (None, '') else float('inf')
    except (ValueError, TypeError):
        max_val = float('inf')

    # Фильтруем последние значения
    df_last_filtered = df_last[
        (df_last['kerry_buy_spread_y'] >= min_val) &
        (df_last['kerry_buy_spread_y'] <= max_val)
        ]

    # Получаем список фьючерсов, которые прошли фильтр по последним значениям
    valid_futures = df_last_filtered['name_future'].tolist()

    # Фильтруем полный датафрейм, оставляя только данные по этим фьючерсам
    if valid_futures:
        df_filtered = df_full[df_full['name_future'].isin(valid_futures)]
    else:
        df_filtered = pd.DataFrame()  # Пустой датафрейм если нет подходящих фьючерсов

    if df_filtered.empty:
        empty_result = html.Div("Нет данных, удовлетворяющих фильтру", style={"textAlign": "center"})
        return empty_result, None, None
    # --- Конец логики фильтрации ---
    
    # --- Получение последних значений и сортировка для таблицы ---
    df_last = df_filtered.sort_values('trade_time').drop_duplicates('name_future', keep='last')
    df_last_sorted = df_last.sort_values(by=sort_by, ascending=False)
    # --- Конец получения и сортировки ---
    
    # --- Создание таблицы (всегда передаем полные отсортированные данные) ---
    table = create_current_spreads_table(df_last_sorted)
    # --- Конец создания таблицы ---
    logger.debug(f"Table component created and returned by {sort_by}")
    
    # Конвертируем DataFrame в JSON для хранения
    filtered_json = df_filtered.to_json(date_format='iso', orient='split')
    sorted_json = df_last_sorted.to_json(date_format='iso', orient='split')

    return table, filtered_json, sorted_json


# --- Второй Callback: Обновление Графиков ---
@app.callback(
    Output('graphs-container', 'children'),
    [Input('spreads-data-table', 'page_current'),
     Input('spreads-data-table', 'page_size')],
    [State('stored-filtered-data', 'data'),   # Получаем закэшированные данные
     State('stored-sorted-data', 'data')]
)
def update_graphs(page_current, page_size, filtered_json, sorted_json):
    logger.debug(f"Update_graphs called with page_current={page_current}, page_size={page_size}")

    # Проверяем, есть ли закэшированные данные
    if filtered_json is None or sorted_json is None:
        return html.Div("Нет данных для графиков", style={"textAlign": "center"})

    # Восстанавливаем DataFrame из JSON
    try:
        df_filtered = pd.read_json(filtered_json, orient='split')
        df_last_sorted = pd.read_json(sorted_json, orient='split')
    except Exception as e:
        logger.error(f"Failed to restore data from JSON: {e}")
        return html.Div("Ошибка при загрузке данных", style={"textAlign": "center"})
    
    # Обработка значений по умолчанию для пагинации
    page_current = page_current if page_current is not None else 0
    page_size = page_size if page_size is not None else 10
    
    # Определяем фьючерсы для текущей страницы
    start_idx = page_current * page_size
    end_idx = start_idx + page_size
    df_page = df_last_sorted.iloc[start_idx:end_idx]
    futures_on_page = df_page['name_future'].tolist()
    logger.debug(f"Futures for graphs on page {page_current}: {futures_on_page}")

    # Создаем графики
    if not futures_on_page:
        return html.Div("Нет данных для отображения на этой странице", style={"textAlign": "center"})

    graphs = create_spread_graphs(df_filtered, futures_on_page)
    logger.debug(f"Graphs created and returned")
    return graphs
    # --- Конец создания графиков ---

# === Callback'и для второй вкладки (future_spreads) ===

# --- Первый Callback: Обновление Таблицы Future Spreads ---
@app.callback(
    [Output('future-table-container', 'children'),
     Output('stored-future-filtered-data', 'data'),  # Кэшируем отфильтрованные данные
     Output('stored-future-sorted-data', 'data')],
    [Input('dropdown-expiration-futures', 'value'),
     Input('dropdown-sort-by', 'value')]
)
def update_future_table(expiration_list, sort_by):
    logger.debug(f"Update_future_table called with exp={expiration_list}, sort={sort_by}")

    # Загружаем данные
    df_full = load_future_spreads(expiration_list)

    if df_full.empty:
        empty_result = html.Div("Нет данных для отображения таблицы Future Spreads", style={"textAlign": "center"})
        return empty_result, None, None
    
    # Применяем фильтры (в данном случае только по экспирации, которая уже в load_future_spreads)
    # Если понадобятся дополнительные фильтры, добавить их здесь
    df_filtered = df_full.copy() # В данном случае df_full уже отфильтрован по expiration

    if df_filtered.empty:
        empty_result = html.Div("Нет данных, удовлетворяющих фильтру Future Spreads", style={"textAlign": "center"})
        return empty_result, None, None
    
    # Получаем отсортированные последние значения
    df_last_sorted = get_sorted_future_data(df_filtered, sort_by)
    logger.debug(f"Got {len(df_last_sorted)} unique future pairs after sorting")

    # Создаем таблицу
    table = create_current_future_spreads_table(df_last_sorted)
    logger.debug("Future table component created and returned")

    # Конвертируем DataFrame в JSON для хранения
    try:
        filtered_json = df_filtered.to_json(date_format='iso', orient='split')
        sorted_json = df_last_sorted.to_json(date_format='iso', orient='split')
    except Exception as e:
        logger.error(f"Failed to serialize data to JSON: {e}")
        return html.Div("Ошибка при сериализации данных", style={"textAlign": "center"}), None, None

    return table, filtered_json, sorted_json


# --- Второй Callback: Обновление Графиков Future Spreads ---
@app.callback(
    Output('future-graphs-container', 'children'),
    [Input('future-spreads-data-table', 'page_current'),  # Input от таблицы future spreads
     Input('future-spreads-data-table', 'page_size')],
    [State('stored-future-filtered-data', 'data'),   # Получаем закэшированные данные
     State('stored-future-sorted-data', 'data')]
)
def update_future_graphs(page_current, page_size, filtered_json, sorted_json):
    logger.debug(f"Update_future_graphs called with page_current={page_current}, page_size={page_size}")

    # Проверяем, есть ли закэшированные данные
    if filtered_json is None or sorted_json is None:
        return html.Div("Нет данных для графиков Future Spreads. Обновите фильтры.", style={"textAlign": "center"})

    # Восстанавливаем DataFrame из JSON
    try:
        df_filtered = pd.read_json(filtered_json, orient='split')
        df_last_sorted = pd.read_json(sorted_json, orient='split')
    except Exception as e:
        logger.error(f"[ERROR] Failed to restore future data from JSON: {e}")
        return html.Div("Ошибка при загрузке данных для графиков", style={"textAlign": "center"})

    # Обработка значений по умолчанию для пагинации
    page_current = page_current if page_current is not None else 0
    page_size = page_size if page_size is not None else 10

    # Определяем фьючерсы для текущей страницы
    start_idx = page_current * page_size
    end_idx = start_idx + page_size
    df_page = df_last_sorted.iloc[start_idx:end_idx]
    logger.debug(f"Futures for future graphs on page {page_current}: {len(df_page)} pairs")

    # Создаем графики
    if df_page.empty:
        return html.Div("Нет данных для отображения на этой странице", style={"textAlign": "center"})

    graphs = create_future_spread_graphs(df_filtered, df_page)
    logger.debug(f"Future graphs created and returned")
    return graphs


# === Запуск сервера ===

if __name__ == '__main__':
    app.run(debug=True)
