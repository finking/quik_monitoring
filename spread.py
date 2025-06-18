import logging
import os
import csv
import sqlite3
from datetime import datetime  # Дата и время
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QUIK#

FILE_PATH = "data/stocks_futures.csv"
DB_PATH = "data/futures_spreads.db"


# функция для создания бд
def init_db(db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS spreads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_time TEXT,
            name_share TEXT,
            bid_share REAL,
            offer_share REAL,
            name_future TEXT,
            bid_future REAL,
            offer_future REAL,
            lot_size_future REAL,
            exp_days INTEGER,
            kerry_buy_spread_y REAL,
            kerry_sell_spread_y REAL
        )
        ''')
        conn.commit()


# Функция для сохранения данных в БД
def save_to_db(cursor, data):

    cursor.execute('''
    INSERT INTO spreads (
        trade_time, name_share, bid_share, offer_share,
        name_future, bid_future, offer_future,
        lot_size_future, exp_days, kerry_buy_spread_y, kerry_sell_spread_y
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()

# Функция получения Топ-5 по доходности продажи спреда
def get_top_by_kerry_sell(db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Получаем самые свежие записи для каждой акции
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

        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        latest_per_share = [dict(zip(headers, row)) for row in rows]

        return latest_per_share


# Чтение файла настроек
def read_stock_futures_csv(file_path, has_header=True):
    """
    Читает CSV-файл с акциями и фьючерсами.
    Возвращает список словарей: [{акция1: [фьючерсы]}, ...]
    """

    if not os.path.exists(file_path):
        logging.error(f"Файл {file_path} не найден.")
        return None

    result = []

    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=";")

            if has_header:
                header = next(reader)  # Пропустить заголовок
                logging.info(f"Заголовок CSV: {header}")

            for row_num, row in enumerate(reader, start=1):
                if not row:
                    logging.warning(f"Пустая строка №{row_num}. Пропущена.")
                    continue

                stock = row[0]
                futures = [f for f in row[1:] if f.strip()] # Пропуск пустых значений

                if not stock:
                    logging.warning(f"Строка №{row_num}: отсутствует название акции. Пропущена.")
                    continue

                result.append({stock: futures})
                logging.info(f"Обработана строка №{row_num}: {stock} -> {futures}")

        logging.info(f"Успешно обработано {len(result)} записей.")
        return result

    except Exception as e:
        logging.error(f"Ошибка при чтении файла: {e}", exc_info=True)
        return None


# Получение данных по инструменту
def get_info(dataname):
    try:
        class_code, sec_code = qp_provider.dataname_to_class_sec_codes(dataname)  # Код режима торгов и тикер
        si = qp_provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        logger.debug(f'Ответ от сервера: {si}')
        logger.info(
            f'Информация о тикере {si["class_code"]}.{si["sec_code"]} ({si["short_name"]}, {si["exp_date"]}):')  # Короткое наименование инструмента
        logger.info(f'- Валюта: {si["face_unit"]}')
        lot_size = si['lot_size']  # Лот
        logger.info(f'- Лот: {lot_size}')
        last_price = float(
            qp_provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value'])  # Последняя цена сделки
        logger.info(f'- Последняя цена сделки: {last_price}')

        bid = float(qp_provider.get_param_ex(class_code, sec_code, "bid")['data']['param_value'])
        offer = float(qp_provider.get_param_ex(class_code, sec_code, "offer")['data']['param_value'])
        logger.info(f'- Лучшая цена спроса: {bid}')
        logger.info(f'- Лучшая цена предложения: {offer}')
        # logger.info(f'- Спред: {offer - bid}')
        return si["short_name"], lot_size, si["exp_date"], bid, offer

    except Exception as e:
        logging.error(f"Не удалось получить информацию по {dataname}. Ошибка: {e}")
        return None


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('spread.py')  # Будем вести лог
    qp_provider = QuikPy()  # Подключение к локальному запущенному терминалу QUIK

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('logs.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=qp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    # Проверяем, существует ли файл базы данных. Если нет, то создаем
    if not os.path.exists(DB_PATH):
        try:
            init_db(DB_PATH)
            logger.info(f'База данных создана в {DB_PATH}')
        except Exception as e:
            logger.error(f'Не удалось создать базу данных в {DB_PATH}. Ошибка: {e}')

    # Формат короткого имени для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>. Пример: SiU4, RIU4
    list_datanames = read_stock_futures_csv(FILE_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for datanames in list_datanames:
            for share, futures in datanames.items():
                # получение данных для акции
                info_share = get_info(share)
                if not info_share:
                    break
                name_share, _, _, bid_share, offer_share = info_share
                for future in futures:
                    # получение данных для фчс
                    info_future = get_info(future)
                    if not info_future:
                        break
                    name_future, lot_size_future, exp_date, bid_future, offer_future = info_future
                    # Конвертация даты экспирации из формата "20250620" в дату.
                    converted_date = datetime.strptime(str(exp_date), "%Y%m%d")
                    exp_days = (converted_date - datetime.now()).days + 1
                    logger.info(f"Кол-во дней до экспирации: {exp_days}")

                    # Продажа спреда
                    diff_buy_spread = bid_future - offer_share * lot_size_future
                    kerry_buy_spread = round((diff_buy_spread / (offer_share * lot_size_future)) * 100, 2)
                    kerry_buy_spread_y = round(diff_buy_spread / (offer_share * lot_size_future) / exp_days * 365 * 100, 2)

                    logger.info(f"Разница между покупкой акции {name_share} и продажей фьючерса {name_future} составляет {diff_buy_spread}")
                    logger.info(f"Керри продажи спреда межуду {name_share} и фьючерса {name_future} составляет {kerry_buy_spread }")
                    logger.info(f"Годовой Керри продажи спреда межуду {name_share} и фьючерса {name_future} составляет {kerry_buy_spread_y}")

                    # Покупка спреда
                    diff_sell_spread = offer_future - bid_share * lot_size_future
                    kerry_sell_spread = round((diff_sell_spread / (bid_share * lot_size_future)) * 100, 2)
                    kerry_sell_spread_y = round(diff_sell_spread / (bid_share * lot_size_future) / exp_days * 365 * 100, 2)

                    logger.info(f"Разница между продажей акции {name_share} и покупкой фьючерса {name_future} составляет {diff_sell_spread}")
                    logger.info(f"Керри покупки спреда между {name_share} и фьючерса {name_future} составляет {kerry_sell_spread }")
                    logger.info(f"Годовой Керри покупки спреда между {name_share} и фьючерса {name_future} составляет {kerry_sell_spread_y}")

                    # Сохранение в БД
                    data_to_save = (
                        datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
                        name_share,
                        bid_share,
                        offer_share,
                        name_future,
                        bid_future,
                        offer_future,
                        lot_size_future,
                        exp_days,
                        kerry_buy_spread_y,
                        kerry_sell_spread_y
                    )

                    # Запись в БД
                    save_to_db(cursor, data_to_save)

    qp_provider.close_connection_and_thread()  # Перед выходом закрываем соединение для запросов и поток обработки функций обратного вызова
