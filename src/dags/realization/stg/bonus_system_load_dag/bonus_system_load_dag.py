"""Выгрузка из бонусной системы в DWH"""
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from realization.logger import logger
from realization.models import OutboxObj, RankObj, UserObj
from realization.pg_to_pg_loader import Loader


ORIGIN_CONNECTION = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
DESTINATION_CONNECTION = 'PG_WAREHOUSE_CONNECTION'

VARIABLE_RANKS_ORIG_QUERY = 'BONUS_SYSTEM_RANKS_ORIG_QUERY'
VARIABLE_RANKS_DEST_QUERY = 'BONUS_SYSTEM_RANKS_DEST_QUERY'

VARIABLE_USERS_ORIG_QUERY = 'BONUS_SYSTEM_USERS_ORIG_QUERY'
VARIABLE_USERS_DEST_QUERY = 'BONUS_SYSTEM_USERS_DEST_QUERY'

VARIABLE_OUTBOX_ORIG_QUERY = 'BONUS_SYSTEM_OUTBOX_ORIG_QUERY'
VARIABLE_OUTBOX_DEST_QUERY = 'BONUS_SYSTEM_OUTBOX_DEST_QUERY'


args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'stg-bonus-system-load',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin bonus system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='*/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Создадим группы задач ETL для staging слоя.
    origin = {
        'ranks_load': {
            'model': RankObj,
            'wf_key': 'ranks_origin_to_stg_workflow',
            'variable_dest_query_path': VARIABLE_RANKS_DEST_QUERY,
            'variable_origin_query_path': VARIABLE_RANKS_ORIG_QUERY,
        },
        'users_load': {
            'model': UserObj,
            'wf_key': 'users_origin_to_stg_workflow',
            'variable_dest_query_path': VARIABLE_USERS_DEST_QUERY,
            'variable_origin_query_path': VARIABLE_USERS_ORIG_QUERY,
        },
        'outbox_load':{
            'model': OutboxObj,
            'wf_key': 'outbox_origin_to_stg_workflow',
            'variable_dest_query_path': VARIABLE_OUTBOX_DEST_QUERY,
            'variable_origin_query_path': VARIABLE_OUTBOX_ORIG_QUERY,
        }
    }
    tasks = []
    for task_name, task_params in origin.items():

        @task(task_id=task_name)
        def load_task(
            variable_dest_query_path,
            variable_origin_query_path,
            model,
            wf_key,
        ):
            # создаем экземпляр класса, в котором реализована логика.
            rank_loader = Loader(
                dest_con_id=DESTINATION_CONNECTION, # название соединения DWH
                orgin_con_id=ORIGIN_CONNECTION, # название соединения источника
                variable_dest_query_path=variable_dest_query_path, # название переменной, где лежит запрос на загрузку
                variable_origin_query_path=variable_origin_query_path, # название переменной, где лежит запрос на выгрузку
                log=logger, # логгер
                model=model, # Модель которой должна соответствовать запись из источника
                wf_key=wf_key # Названия ключа состояния прогресса
            )
            rank_loader.load()  # Вызываем функцию, которая перельет данные.

        tasks.append(load_task(
            task_params['variable_dest_query_path'],
            task_params['variable_origin_query_path'],
            task_params['model'],
            task_params['wf_key']
       ))

    start >> tasks >> end
