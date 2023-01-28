"""Выгрузка из системы доставки в DWH."""
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from realization.api_to_pg import CourierSTGLoader, DeliverySTGLoader
from realization.logger import logger


CONNECTION_DESTINATION = 'PG_WAREHOUSE_CONNECTION'
CONNECTION_ORIGIN = 'API_DELIVERY'

# В этих переменных лежат пути до файлов sql шаблонов запросов на вставку в слой STG.
VARIABLE_COURIER_DEST_QUERY = 'DELIVERY_SYSTEM_COURIER_DEST_QUERY'
VARIABLE_DELIVERY_DEST_QUERY = 'DELIVERY_SYSTEM_DELIVERY_DEST_QUERY'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'stg-delivery-system-load',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin delivery system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='*/15 * * * *',
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task(task_id='courier_load_task')
    def courier_load():
        loader = CourierSTGLoader(
            src_con_id=CONNECTION_ORIGIN,
            dest_con_id=CONNECTION_DESTINATION,
            query_variable=VARIABLE_COURIER_DEST_QUERY,
            log=logger,
            wf_key='courier_delivery_to_stg_workflow',
        )
        loader.load()

    @task(task_id='delivery_load')
    def delivery_load():
        loader = DeliverySTGLoader(
            src_con_id=CONNECTION_ORIGIN,
            dest_con_id=CONNECTION_DESTINATION,
            query_variable=VARIABLE_DELIVERY_DEST_QUERY,
            log=logger,
            wf_key='delivery_to_stg_workflow',
        )
        loader.load()


    courier_load_task = courier_load()
    delivery_load_task = delivery_load()

    start >> [courier_load_task, delivery_load_task] >> end
