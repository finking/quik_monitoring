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

# SQL-запроса получения Топ-5 спредов между акцией и фьючерсом по kerry_sell_spread_y
cursor.execute(
    '''
        SELECT trade_time, name_share, name_future, kerry_buy_spread_y, kerry_sell_spread_y
        FROM spreads
        WHERE (name_share, trade_time) IN (
            SELECT name_share, MAX(trade_time)
            FROM spreads
            GROUP BY name_share
        )
        ORDER BY kerry_buy_spread_y DESC
        LIMIT 5;
    '''
)

# Заголовки из запроса
headers = [description[0] for description in cursor.description]

# Полученные данные
rows = cursor.fetchall()

if rows:
    logging.info("Вывод спреда между акцией и фьючерсом.")
    logging.info(headers)
    for row in rows:
        logging.info(row)
else:
    logging.info("Нет записей")

# SQL-запроса получения Топ-5 спредов фьючерсами по spread
cursor.execute(
    '''
        SELECT trade_time, near_future, far_future, spread_bid_y, spread_offer_y
        FROM future_spreads
        WHERE (far_future, trade_time) IN (
            SELECT far_future, MAX(trade_time)
            FROM future_spreads
            GROUP BY far_future
        )
        ORDER BY spread_bid_y DESC
        LIMIT 5;
    '''
)

# Заголовки из запроса
headers = [description[0] for description in cursor.description]

# Полученные данные
rows = cursor.fetchall()

if rows:
    logging.info("Вывод спреда между фьючерсами.")
    logging.info(headers)
    for row in rows:
        logging.info(row)
else:
    logging.info("Нет записей")

conn.close()
