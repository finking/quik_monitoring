import dash
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from dash import dash_table, dcc, html, Input, Output
from datetime import datetime


# Список дат экспираций у фьючерсов
LIST_EXPIRATIONS = ['9.25', '12.25', '3.26', '6.26']  # или None для пустого выбора


# Загрузка данных из БД
def load_data(expiration_list=None, start_date=None, end_date=None):
    conn = sqlite3.connect("data/futures_spreads.db")
    query = "SELECT trade_time, name_future, kerry_buy_spread_y, kerry_sell_spread_y FROM spreads WHERE 1=1"
    params = []

    if expiration_list:
        placeholders = []
        for exp in expiration_list:
            placeholders.append(f"name_future LIKE ?")
            params.append(f"%-{exp}")

        query += " AND (" + " OR ".join(placeholders) + ")"

    if start_date:
        parsed_start = datetime.fromisoformat(start_date)
        formatted_start = parsed_start.strftime("%d.%m.%Y %H:%M:%S")
        query += " AND trade_time >= ?"
        params.append(formatted_start)

    if end_date:
        parsed_end = datetime.fromisoformat(end_date)
        formatted_end = parsed_end.strftime("%d.%m.%Y %H:%M:%S")
        query += " AND trade_time <= ?"
        params.append(formatted_end)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if not df.empty:
        # Преобразуем дату
        df['trade_time'] = pd.to_datetime(df['trade_time'], format='%d.%m.%Y %H:%M:%S')

    return df


def get_unique_expirations():
    conn = sqlite3.connect("data/futures_spreads.db")
    df = pd.read_sql_query("SELECT DISTINCT name_future FROM spreads", conn)
    conn.close()

    # Извлекаем только часть после дефиса: "GAZR-6.25" → "6.25"
    expirations = df['name_future'].str.extract(r'-(\d+\.\d+)')[0].dropna().unique()
    return sorted(expirations)


# Инициализация приложения
app = dash.Dash(__name__, external_stylesheets=[])
app.title = "Спреды между фьючерсом и акцией"

app.layout = html.Div([
    # Заголовок
    html.Div([
        html.H2("Анализ спреда фьючерс-акция", className="header-title"),
        html.P("Мониторинг спредов", className="header-subtitle")
    ], className="header"),

    # Фильтры
    html.Div([
        html.Label("Экспирация", className="input-label"),
        dcc.Dropdown(
            id='dropdown-expiration',
            options=[{'label': f"{exp}", 'value': exp} for exp in get_unique_expirations()],
            value=LIST_EXPIRATIONS,
            multi=True,
            className="custom-dropdown"
        ),
        html.Label("Сортировать по", className="input-label"),
        dcc.Dropdown(
            id='dropdown-sort-by',
            options=[
                {'label': 'Лучший % спроса (Продать спред)', 'value': 'buy'},
                {'label': 'Лучший % предложения (Купить спред)', 'value': 'sell'}
            ],
            value='buy',
            clearable=False,
            className="custom-dropdown"
        ),
        html.Label("Начальная дата", className="input-label"),
        dcc.DatePickerSingle(
            id='date-picker-start',
            date=datetime(2025, 1, 1),
            display_format='DD-MM-YYYY',
        ),
        html.Label("Конечная дата", className="input-label"),
        dcc.DatePickerSingle(
            id='date-picker-end',
            date=datetime.today(),
            display_format='DD-MM-YYYY',
        ),

    ], className="filters-container"),

    # Таблица и графики
    html.Div(id='graphs-container', className="graphs-container")

], className="main-container")


@app.callback(
    Output('graphs-container', 'children'),
    Input('dropdown-expiration', 'value'),
    Input('dropdown-sort-by', 'value'),
    Input('date-picker-start', 'date'),
    Input('date-picker-end', 'date')
)
def update_graph(expiration_list, sort_by, start_date, end_date):
    if not expiration_list:
        return []

    full_df = load_data(expiration_list, start_date, end_date)

    if full_df.empty:
        return [html.Div("Нет данных для отображения.", style={"textAlign": "center"})]

    # Получаем самые свежие значения для сортировки
    latest_df = full_df.drop_duplicates(subset=['name_future'], keep='last')

    # Определяем столбец для сортировки
    sort_column = 'kerry_buy_spread_y' if sort_by == 'buy' else 'kerry_sell_spread_y'
    latest_df = latest_df.sort_values(by=sort_column, ascending=False)

    # Список фьючерсов в нужном порядке
    sorted_futures = latest_df['name_future'].tolist()

    # Графики
    graphs = []
    for future_name in sorted_futures:
        group = full_df[full_df['name_future'] == future_name]
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

    # Таблица с текущими значениями
    table = create_current_table(latest_df, sort_by)

    return [table] + graphs


def create_current_table(df, sort_by):
    # Определяем, какой столбец использовать для сортировки
    if sort_by == 'buy':
        sort_column = 'Buy Spread (%)'
    else:
        sort_column = 'Sell Spread (%)'

    # Подготавливаем таблицу
    current_df = df[['name_future', 'kerry_buy_spread_y', 'kerry_sell_spread_y', 'trade_time']]
    current_df['trade_time'] = current_df['trade_time'].dt.strftime('%d.%m.%Y %H:%M:%S')
    current_df = current_df.round(2)
    current_df.columns = ['Фьючерс', 'Buy Spread (%)', 'Sell Spread (%)', 'Последнее обновление']

    # Сортируем по выбранному спреду
    current_df = current_df.sort_values(by=sort_column, ascending=False)

    table = dash_table.DataTable(
        data=current_df.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in current_df.columns],
        sort_action='native',
        sort_by=[{'column_id': sort_column, 'direction': 'desc'}],
        style_table={'overflowX': 'auto'},
        style_cell={
            'minWidth': '100px',
            'width': '150px',
            'maxWidth': '300px',
            'padding': '10px',
            'text-align': 'center'
        },
        style_header={
            'backgroundColor': '#f0f0f0',
            'fontWeight': 'bold',
            'color': 'black'
        },
        page_size=10
    )

    return html.Div([
        html.H3("Текущие значения спредов", className="section-title"),
        table
    ], className="table-container")


if __name__ == '__main__':
    app.run(debug=True)