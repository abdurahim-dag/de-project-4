import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator

from realization.logger import logger
from realization.models import OrderOrderObj, RestaurantObj, UserOrderObj
from realization.mongo_to_pg_loader import Loader


CONNECTION_DESTINATION = 'PG_WAREHOUSE_CONNECTION'

VARIABLE_RESTAURANT_DEST_QUERY = 'ORDER_SYSTEM_RESTAURANT_DEST_QUERY'
VARIABLE_USERS_DEST_QUERY = 'ORDER_SYSTEM_USERS_DEST_QUERY'
VARIABLE_ORDERS_DEST_QUERY = 'ORDER_SYSTEM_ORDERS_DEST_QUERY'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}
with DAG(
        'stg-order-system-load',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin order system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='*/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Создадим группы задач ETL для staging слоя.
    origin = {
        'restaurant_load': {
            'model': RestaurantObj, # Модель которой должна соответствовать запись из источника
            'wf_key': 'restaurant_origin_to_stg_workflow', # Значение ключа состояния прогресса
            'variable_dest_query_path': VARIABLE_RESTAURANT_DEST_QUERY, # Значение ключа состояния прогресса
            'collection': 'restaurants', # Название коллекции на источнике
        },
        'users_load': {
            'model': UserOrderObj,
            'wf_key': 'users_order_origin_to_stg_workflow',
            'variable_dest_query_path': VARIABLE_USERS_DEST_QUERY,
            'collection': 'users',
        },
        'orders_load': {
            'model': OrderOrderObj,
            'wf_key': 'orders_order_origin_to_stg_workflow',
            'variable_dest_query_path': VARIABLE_ORDERS_DEST_QUERY,
            'collection': 'orders',
        },
    }
    tasks = []
    for task_name, task_params in origin.items():

        @task(task_id=task_name)
        def load_task(
            variable_dest_query_path,
            model,
            collection,
            wf_key,
        ):
            # создаем экземпляр класса, в котором реализована логика.
            rank_loader = Loader(
                dest_con_id=CONNECTION_DESTINATION, # Куда грузим
                dest_query_path=Variable.get(variable_dest_query_path), # С помощью какого запроса
                log=logger, # Как логгируем
                model=model, # Какая модель должна быть загружена
                collection=collection, # Из какой коллекции выгружаем
                wf_key=wf_key,  # Названия ключа состояния прогресса
                cert_path=Variable.get("MONGO_DB_CERTIFICATE_PATH"), # Mongo сертификат
                db_user = Variable.get("MONGO_DB_USER"), # Mongo имя пользователя
                db_pw = Variable.get("MONGO_DB_PASSWORD"), # Mongo пароль
                rs = Variable.get("MONGO_DB_REPLICA_SET"), # Mongo реплика
                db = Variable.get("MONGO_DB_DATABASE_NAME"), # Mongo БД
                host = Variable.get("MONGO_DB_HOST"), # Mongo хост
            )
            rank_loader.load()  # Вызываем функцию, которая перельет данные.

        tasks.append(load_task(
            task_params['variable_dest_query_path'],
            task_params['model'],
            task_params['collection'],
            task_params['wf_key']
       ))

    start >> tasks >> end
