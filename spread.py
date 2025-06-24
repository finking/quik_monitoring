import logging
import os
import csv
import sqlite3
from datetime import datetime  # Дата и время
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QUIK#

FILE_PATH = "data/stocks_futures.csv"
DB_PATH = "data/futures_spreads.db"

DAYS_YEAR = 365 # дней в году

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

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS future_spreads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_time TEXT,
            near_future TEXT,
            far_future TEXT,
            spread_bid REAL,
            spread_offer REAL,
            spread_bid_y REAL,
            spread_offer_y REAL,
            far_exp_days INTEGER
        )
        ''')
        conn.commit()


# Функция для сохранения данных в БД
def save_to_db(cursor, table_name, data):
    if table_name == 'spreads':
        cursor.execute('''
        INSERT INTO spreads (
            trade_time, name_share, bid_share, offer_share,
            name_future, bid_future, offer_future,
            lot_size_future, exp_days, kerry_buy_spread_y, kerry_sell_spread_y
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
    elif table_name == 'future_spreads':
        cursor.execute('''
        INSERT INTO future_spreads (
            trade_time, near_future, far_future, spread_bid, 
            spread_offer, spread_bid_y, spread_offer_y, far_exp_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
    else:
        raise ValueError(f"Неизвестная таблица: {table_name}")


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
    # if not os.path.exists(DB_PATH):
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

                # Список для данных по фчс
                futures_data = []

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

                    # Добавляем в список для дальнейшего использования
                    futures_data.append({
                        'name_future': name_future,
                        'exp_days': exp_days,
                        'bid_future': bid_future,
                        'offer_future': offer_future,
                        'bid_share': bid_share * lot_size_future,
                        'offer_share': offer_share * lot_size_future,
                    })

                    # Сохранение в таблицу spreads
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
                    save_to_db(cursor, 'spreads', data_to_save)

                # Обрабатываем пары фьючерсов
                if len(futures_data) >= 2:
                    sorted_futures = sorted(futures_data, key=lambda x: x['exp_days'])

                    # Перебираем все возможные пары: ближний vs дальний
                    for i in range(len(sorted_futures)):
                        for j in range(i + 1, len(sorted_futures)):
                            near = sorted_futures[i]
                            far = sorted_futures[j]

                            # Расчет спроса для спреда (по какой "цене" продать КС)
                            spread_bid = far['bid_future'] - near['offer_future']
                            # Пересчет в годовую доходность по формуле:
                            # Доходность годовых = (спред / предложение акции с учетом лота) / кол-во дней до эксп дальнего фчс * кол-во дней * 100%
                            spread_bid_y = (spread_bid / far['offer_share']) / far['exp_days'] * DAYS_YEAR * 100

                            # Расчет предложения для спреда (по какой "цене" купить КС)
                            spread_offer = far['offer_future'] - near['bid_future']
                            # Доходность годовых = (спред / спрос акции с учетом лота) / кол-во дней до эксп дальнего фчс * кол-во дней * 100%
                            spread_offer_y = (spread_offer / far['bid_share']) / far['exp_days'] * DAYS_YEAR * 100

                            future_spread_data = (
                                datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
                                near['name_future'],
                                far['name_future'],
                                spread_bid,
                                spread_offer,
                                round(spread_bid_y, 2),
                                round(spread_offer_y, 2),
                                far['exp_days']
                            )

                            save_to_db(cursor, 'future_spreads', future_spread_data)

        conn.commit()

    qp_provider.close_connection_and_thread()  # Перед выходом закрываем соединение для запросов и поток обработки функций обратного вызова
