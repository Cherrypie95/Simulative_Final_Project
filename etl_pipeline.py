import requests
import psycopg2
import logging
from datetime import datetime, timedelta
import json
import configparser
import os
from typing import List, Dict

dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname,"config.ini"))

# Параметры для подключения к API
api_url = config['API']['API_URL']
params = {
        "date": datetime.strftime(datetime.now()- timedelta(days=1), "%Y-%m-%d")
        }

# Параметры для подключения к БД
db_name = config['Database']['DBNAME']
db_user = config['Database']['DBUSER']
db_password = config['Database']['DBPASSWORD']
db_host = config['Database']['DBHOST']
db_port = config['Database']['DBPORT']

# Шаблон для генерации логов в консоли
logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO
)


# Класс для подкючения к базе данных PostgreSQL
class PGConnection:
    """_summary_

    Returns:
        _type_: _description_
    """
    @staticmethod
    def get_pg_connection():
        conn = psycopg2.connect(
                        dbname = db_name,
                        user = db_user,
                        password = db_password,
                        host = db_host,
                        port = db_port
                        )
        return conn
    


class ApiResponseReader:
    """Класс для подключения к API и выгрузки данных

    Returns:
        List[Dict]: Метод get_response возвращает ответ сервера в виде списка словарей
    """
    # Создаем экземпляр класса с атрибутами url и params
    def __init__(self, url: str, params: Dict):
        self.url = url
        self.params = params
    
    def get_response(self) -> List[Dict]:
        """Функция для выгрузки данных по API

        Returns:
            list: Функция возвращает ответ сервера в виде списка словарей
        """
        try:
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт загрузки данных по API')
            logger.info('=================================')
            response = requests.get(self.url, self.params, timeout=100)
            response.raise_for_status()
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно получены')
            logger.info('=================================')
            response_json = response.json()
            if not response_json:
                logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Сервер вернул пустой список')
                logger.info('=================================')
                raise ValueError('Сервер вернул пустой список')
            if not isinstance(response_json, list):
                logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: API вернул невалидный тип данных')
                logger.info('=================================')
                raise ValueError('Ожидался список, получен ' + type(response_json))
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно преобразованы в JSON\nПолучено {len(response_json)} записей')
            logger.info('=================================')            
            return response_json
        except requests.exceptions.HTTPError as http_err:            
            logger.error(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {response.status_code}\nОписание ошибки: {http_err}')
            logger.error('=================================')                    
        except Exception as err:              
            logger.error(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: {err}')
            logger.error('=================================') 


class ApiResponseProcessing:
    """Класс для обработки данных от API

    Returns:
        List[Dict]: Метод pars_response возвращает список словарей с параметрами покупок
    """

    # Создаем экземпляр класса с атрибутом data
    # В data будем записывать обработанные данные от API
    def __init__(self):
        self.data = [] 
    
    def pars_response(self, response_json: List[Dict]) -> List[Dict]:
        """Функция для обработки ответа сервера и сохранения данных

        Returns:
            List[Dict]: Метод возвращает список словарей с необходимыми параметрами
        """

        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт обработки данных ответа API')
        logger.info('=================================')  
        for row in response_json:
            try:
                client_id = row.get('client_id', None)
                product_id = row.get('product_id', None)
                if client_id and product_id:                  
                    gender = row.get('gender', None)
                    purchase_datetime = row.get('purchase_datetime', None)
                    purchase_time_as_seconds_from_midnight = row.get('purchase_time_as_seconds_from_midnight', None)
                    #product_id = row.get('product_id', None)
                    quantity = row.get('quantity', None)
                    price_per_item = row.get('price_per_item', None)
                    discount_per_item = row.get('discount_per_item', None)
                    total_price = row.get('total_price', None)                      
                    self.data.append((client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price))
            except json.JSONDecodeError as err:
                logger.error(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка парсинга JSON\nИнформация по ошбике: {err}')
                logger.error('=================================')
                continue
            except Exception as err:
                logger.error(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошбике: {err}')
                logger.error('=================================')
        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно обработаны')
        logger.info('=================================')
        return self.data


# 
class ImportData:
    """Класс для загрузки данных в базу данных PostgreSQL
    """

    @staticmethod
    def import_data(purchases_data: List[Dict]) -> None:
        """Функция для загрузки данных в БД

        Args:
            purchases_data (List): Список словарей с необходимыми параметрами
        """
        with open('sql_query.sql', 'r') as f:
            sql_query = f.read()
        try:
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка подключения к базе данных')
            logger.info('=================================')
            conn = PGConnection.get_pg_connection()
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Подключение к базе данных успешно установлено')
            logger.info('=================================')
        except Exception as err:
            logger.error(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {err.pgcode}\nИнформация по ошибке {err}')
            logger.error('=================================')
        else:
            try:
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка загрузки данных в базу данных')
                logger.info('=================================')
                cur = conn.cursor()
                cur.execute(sql_query)
                cur.executemany(
                """INSERT INTO purchases (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price) DO NOTHING""",
                purchases_data)
                conn.commit()
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно загружены в базу данных')
                logger.info('=================================')
            except psycopg2.Error as err:
                pgcode = err.pgcode if hasattr(err, 'pgcode') else 'Unknown'
                logger.error(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {pgcode}\nИнформация по ошибке {err}')
                logger.error('=================================')
            except Exception as err:
                logger.error(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке {err}')
            finally:
                cur.close()
                conn.close()

if __name__ == "__main__":
    # Создаем экземляр класса ApiResponseReader и выгружаем данные
    t = ApiResponseReader(api_url, params)
    response = t.get_response()

    # Создаем экземляр класса ApiResponseProcessing и приводим данные к нужной форме
    r = ApiResponseProcessing()
    r_pars = r.pars_response(response)

    # Загружаем данные в базу данных
    ImportData.import_data(r_pars)
