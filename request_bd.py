import sqlite3
import logging

logger = logging.getLogger('request.py')
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                    datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                    level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                    handlers=[logging.FileHandler('logs.log', encoding='utf-8'),
                              logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль

conn = sqlite3.connect('data/futures_spreads.db')
cursor = conn.cursor()

# SQL-запроса получения Топ-5 спредов по kerry_sell_spread_y
cursor.execute(
    '''
        SELECT trade_time, name_share, name_future, kerry_buy_spread_y, kerry_sell_spread_y
        FROM spreads
        WHERE (name_share, trade_time) IN (
            SELECT name_share, MAX(trade_time)
            FROM spreads
            GROUP BY name_share
        )
        ORDER BY kerry_sell_spread_y DESC
        LIMIT 5;
    '''
)

# Заголовки из запроса
headers = [description[0] for description in cursor.description]

# Полученные данные
rows = cursor.fetchall()


if not rows:
    logging.info("Нет записей")

for row in rows:
    logging.info(row)

conn.close()
