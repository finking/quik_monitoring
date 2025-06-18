import sqlite3
import pandas as pd
import logging
import plotly.graph_objs as go


def visualize_kerry_year_interactive(shortname="GAZR-9.25"):
    conn = sqlite3.connect("data/futures_spreads.db")
    df = pd.read_sql_query("SELECT trade_time, name_future, kerry_buy_spread_y, kerry_sell_spread_y "
                           "FROM spreads WHERE name_future = ?", conn,
                           params=[shortname])
    conn.close()

    if df.empty:
        logging.info(f"Нет данных для {shortname}")
        return

    logging.debug(df.head())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
            x=df["trade_time"],
            y=df["kerry_sell_spread_y"],
            name='offer',
            line=dict(color='red'),
    ))
    fig.add_trace(go.Scatter(
        x=df["trade_time"],
        y=df["kerry_buy_spread_y"],
        name='bid',
        line=dict(color='green'),
    ))

    # Обновляем макет
    fig.update_layout(
        title_text=f"Динамика спреда на фьючерс {shortname}",
        legend_orientation="h",  # Расположение Легенды внизу
        legend=dict(x=.5, xanchor="center"), # Центровка расположения Легенды
        margin=dict(l=0, r=0, t=30, b=0),  # Отступы на экране до графика
        xaxis_title="Дата", # Подпись оси X
        yaxis_title="% годовых", # Подпись оси Y
        hovermode="x", # Сравнение значений на графике
        template="plotly_white" # Фон
    )
    # Шаблон для подписей на графике при навидении
    fig.update_traces(hoverinfo="all", hovertemplate="Дата: %{x}<br>Спред: %{y} %")

    # Запись в файл и отображение
    fig.write_html("./data/graph.html")
    fig.show()


if __name__ == '__main__':
    logger = logging.getLogger('spread.py')  # Будем вести лог
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('logs.log', encoding='utf-8'),
                                  logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    visualize_kerry_year_interactive()