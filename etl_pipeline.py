import requests
import psycopg2
import logging
from datetime import datetime, timedelta
import json
import configparser
import os


# Параметры для подключения к API
api_url = "http://final-project.simulative.ru/data"
params = {
        "date": datetime.strftime(datetime.now()- timedelta(days=1), "%Y-%m-%d")
        }

dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname,"config.ini"))

db_name = config['Datebase']['DBNAME']
db_user = config['Datebase']['DBUSER']
db_password = config['Datebase']['DBPASSWORD']
db_host = config['Datebase']['DBHOST']
db_port = config['Datebase']['DBPORT']

# Шаблон для генерации логов в консоли
logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO
)


# Класс для подкючения к базе данных PostgreSQL
class PGConnection:
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
    

# Класс для подключения к API и выгрузки данных
class ApiResponseReader:
    
    # Создаем экземпляр класса с атрибутами url и params
    def __init__(self, url, params):
        self.url = url
        self.params = params
    
    def get_response(self):
        """Функция для выгрузки данных по API

        Args:
            url (string): api url для выгрузки статистики по студентам
            params (dict): параметры для доступа к API

        Returns:
            list: Функция возвращает ответ сервера в виде списка словарей
        """
        try:
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт загрузки данных по API')
            logger.info('=================================')
            response = requests.get(self.url, self.params)
            response.raise_for_status()
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно получены')
            logger.info('=================================')
            response_json = response.json()
            if not response_json:
                logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: API вернул пустой список')
                logger.info('=================================')
            if not isinstance(response_json, list):
                logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: API вернул невалидный тип данных')
                logger.info('=================================')
                raise ValueError('Ожидался список, получен ' + type(response_json))
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно преобразованы в JSON\nПолучено {len(response_json)} записей')
            logger.info('=================================')            
            return response_json
        except requests.exceptions.HTTPError as http_err:            
            logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {response.status_code}\nОписание ошибки: {http_err}')
            logger.info('=================================')                    
        except Exception as err:              
            logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: {err}')
            logger.info('=================================') 


# Класс для обработки данных от API
class ApiResponseProcessing:

    # Создаем экземпляр класса с атрибутами data и tasks_stat
    # В data будем записывать обработанные данные от API
    # В task_stat будет записывать статистику решения задач 
    def __init__(self):
        self.data = [] 
    
    def pars_response(self, response_json):
        """Функция для обработки ответа сервера и сохранения данных

        Returns:
            List: Функция возвращает список словарей с необходимыми параметрами
        """

        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт обработки данных ответа API')
        logger.info('=================================')  
        for row in response_json:
            try:
                client_id = row.get('client_id', None)
                if client_id:                    
                    gender = row.get('gender', None)
                    purchase_datetime = row.get('purchase_datetime', None)
                    purchase_time_as_seconds_from_midnight = row.get('purchase_time_as_seconds_from_midnight', None)
                    product_id = row.get('product_id', None)
                    quantity = row.get('quantity', None)
                    price_per_item = row.get('price_per_item', None)
                    discount_per_item = row.get('discount_per_item', None)
                    total_price = row.get('total_price', None)                      
                    self.data.append((client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price))
            except json.JSONDecodeError as err:
                logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка парсинга JSON\nИнформация по ошбике: {err}')
                logger.info('=================================')
                continue
            except Exception as err:
                logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошбике: {err}')
                logger.info('=================================')
        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно обработаны')
        logger.info('=================================')
        return self.data


# Класс для загрузки данных в базу данных PostgreSQL
class ImportData:

    @staticmethod
    def import_data(students_data):
        """Функция для загрузки данных в БД

        Args:
            students_data (List): Список словарей с необходимыми параметрами
        """
        try:
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка подключения к базе данных')
            logger.info('=================================')
            conn = PGConnection.get_pg_connection()
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Подключение к базе данных успешно установлено')
            logger.info('=================================')
        except Exception as err:
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {err.pgcode}\nИнформация по ошибке {err}')
            logger.info('=================================')
        else:
            try:
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка загрузки данных в базу данных')
                logger.info('=================================')
                cur = conn.cursor()
                cur.execute("""create table if not exists purchases (
                        client_id varchar, 
                        gender varchar, 
                        purchase_datetime date, 
                        purchase_time_as_seconds_from_midnight integer, 
                        product_id varchar, 
                        quantity integer, 
                        price_per_item integer, 
                        discount_per_item integer, 
                        total_price integer,
                        CONSTRAINT sales_pkey PRIMARY KEY (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price)
                    )
                """)
                cur.executemany(
                """INSERT INTO purchases (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price) DO NOTHING""",
                students_data)
                conn.commit()
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно загружены в базу данных')
                logger.info('=================================')
            except psycopg2.Error as err:
                pgcode = err.pgcode if hasattr(err, 'pgcode') else 'Unknown'
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {pgcode}\nИнформация по ошибке {err}')
                logger.info('=================================')
            except Exception as err:
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке {err}')
            finally:
                cur.close()
                conn.close()


# Создаем экземляр класса ApiResponseReader и выгружаем данные
t = ApiResponseReader(api_url, params)
response = t.get_response()

# Создаем экземляр класса ApiResponseProcessing и приводим данные к нужной форме
r = ApiResponseProcessing()
r_pars = r.pars_response(response)

# # Загружаем данные в базу данных
ImportData.import_data(r_pars)
