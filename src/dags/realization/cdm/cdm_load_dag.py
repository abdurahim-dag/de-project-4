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
VARIABLE_DM_SETTLEMENT_REPORT_QUERY = 'DM_SETTLEMENT_REPORT_QUERY'
VARIABLE_DM_DELIVERY_REPORT_QUERY = 'DM_DELIVERY_REPORT_QUERY'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}
with DAG(
        'cdm-load',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin order system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='@hourly',
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Создадим группы задач ETL для dds слоя.
    origin = {
        'first': {
            'dm_settlement_report': DDSParams(
                con_id=CONNECTION_DESTINATION, # CONNECTION DWH
                wf_key='dm_settlement_report_load_workflow', # Значение ключа состояния
                variable_query_path=VARIABLE_DM_SETTLEMENT_REPORT_QUERY # Путь до шаблона sql insert запроса
            ),
            'dm_courier_ledger': DDSParams(
                con_id=CONNECTION_DESTINATION,
                wf_key='dm_delivery_report_load_workflow',
                variable_query_path=VARIABLE_DM_DELIVERY_REPORT_QUERY,
                start_workflow_key=0,
            ),
        },
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
                        # Значение по умолчанию состояния
                        start_workflow_key,
                        # Если таблица должна поддерживать SCD 2, сообщим нашей реализации
                        is_scd2
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
                    loader.WORKFLOW_SCHEMA = 'cdm' # Таргет схема.
                    loader.load()  # Вызываем функцию, которая перельет данные.

                # Список из однотипных задач
                tasks.append(
                    load_task(
                        con_id=task_param.con_id,
                        wf_key=task_param.wf_key,
                        variable_query_path=task_param.variable_query_path,
                        start_workflow_key=task_param.start_workflow_key,
                        is_scd2=task_param.scd2
                    )
                )
            [t for t in tasks]
        groups.append(tg)

    start >> groups[0] >> end
