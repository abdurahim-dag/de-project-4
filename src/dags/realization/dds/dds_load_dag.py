import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from realization.dds_loader import Loader
from realization.logger import logger
from realization.models import DDSParams


CONNECTION_DESTINATION = 'PG_WAREHOUSE_CONNECTION'

# Переменные указывающие на пути, для запросов sql.
VARIABLE_DM_USERS_QUERY = 'DM_USERS_LOAD_QUERY'
VARIABLE_DM_RESTAURANTS_QUERY = 'DM_RESTAURANTS_LOAD_QUERY'
VARIABLE_DM_TIMESTAMPS_QUERY = 'DM_TIMESTAMPS_LOAD_QUERY'
VARIABLE_DM_PRODUCTS_QUERY = 'DM_PRODUCTS_LOAD_QUERY'
VARIABLE_DM_ORDERS_QUERY = 'DM_ORDERS_LOAD_QUERY'
VARIABLE_FCT_PRODUCT_SALES_QUERY = 'FCT_PRODUCT_SALES_LOAD_QUERY'
VARIABLE_DM_COURIERS_QUERY = 'DM_COURIERS_LOAD_QUERY'
VARIABLE_DM_DELIVERIES_QUERY = 'DM_DELIVERIES_LOAD_QUERY'
VARIABLE_FCT_ORDER_DELIVERY_QUERY = 'FCT_ORDER_DELIVERY_LOAD_QUERY'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
        'dds-load',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin order system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='*/30 * * * *',  # Задаем расписание выполнения дага - каждый 30 минут.
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Создадим группы задач ETL для dds слоя.
    origin = {
        'first': {
            'dm_users_load': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_users_load_workflow',
                variable_query_path=VARIABLE_DM_USERS_QUERY,
            ),
            'dm_restaurants_load': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_restaurants_load_workflow',
                variable_query_path=VARIABLE_DM_RESTAURANTS_QUERY,
                scd2=True,
            ),
            'dm_timestamps_load': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_timestamps_load_workflow',
                variable_query_path=VARIABLE_DM_TIMESTAMPS_QUERY,
            ),
            'dm_couriers_load':DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_couriers_load_load_workflow',
                variable_query_path=VARIABLE_DM_COURIERS_QUERY,
                start_workflow_key=0
            )
        },
        'second': {
            'dm_products_load': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_products_load_workflow',
                variable_query_path=VARIABLE_DM_PRODUCTS_QUERY,
                scd2=True,
            ),
            'dm_orders': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_orders_load_workflow',
                variable_query_path=VARIABLE_DM_ORDERS_QUERY,
            ),
        },
        'third': {
            'dm_deliveries': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dds_dm_deliveries_load_workflow',
                variable_query_path=VARIABLE_DM_DELIVERIES_QUERY,
            ),
        },
        'last':{
            'fct_product_sales_load': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='fct_product_sales_load_workflow',
                variable_query_path=VARIABLE_FCT_PRODUCT_SALES_QUERY,
            ),
            'fct_order_delivery': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='fct_order_delivery_load_workflow',
                variable_query_path=VARIABLE_FCT_ORDER_DELIVERY_QUERY
            )
        }
    }

    groups = []
    for name, task_params in origin.items():
        tg_id = f'group-{name}'
        with TaskGroup(group_id=tg_id) as tg:
            tasks = []
            for task_name, task_param in task_params.items():
                @task(task_id=task_name)
                def load_task(
                        con_id,
                        wf_key,
                        variable_query_path,
                        start_workflow_key,
                        is_scd2,
                ):
                    # создаем экземпляр класса, в котором реализована логика.
                    loader = Loader(
                        con_id=con_id,
                        query_path=Variable.get(variable_query_path),
                        log=logger,
                        wf_key=wf_key,
                        start_workflow_key=start_workflow_key,
                        is_scd2=is_scd2
                    )

                    loader.load()  # Вызываем функцию, которая перельет данные.
                tasks.append(
                    load_task(
                        con_id=task_param.con_id,
                        wf_key=task_param.wf_key,
                        variable_query_path=task_param.variable_query_path,
                        start_workflow_key=task_param.start_workflow_key,
                        is_scd2=task_param.scd2,
                    )
                )
            [t for t in tasks]
        groups.append(tg)

    start >> groups[0] >> groups[1] >> groups[2] >> groups[3] >> end
