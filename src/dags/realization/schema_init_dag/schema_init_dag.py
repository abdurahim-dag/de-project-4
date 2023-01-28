"""Даг инициализирующий схему DWH."""
import pendulum
from airflow import DAG

from realization.operator import MySimplePGSQLOperator
from realization.logger import logger


CONNECTION_WAREHOUSE = "PG_WAREHOUSE_CONNECTION"
VARIABLE_BASE_SQL = 'INIT_DDL_FILES_PATH'
VARIABLE_DELIVERY_SQL = 'INIT_DELIVERY_DDL_FILES_PATH'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'stg-schema-init',
    catchup=False,
    default_args=args,
    description='Initialize dag for staging',
    is_paused_upon_creation=True,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    schedule_interval='@once',
    tags=['sprint5', 'stg', 'schema', 'ddl', 'example']
) as dag:

    schema_init = MySimplePGSQLOperator(
        task_id='schema_init',
        conn_id=CONNECTION_WAREHOUSE,
        variable_sql_path=VARIABLE_BASE_SQL,
        logger=logger,
        dag=dag
    )

    schema_init_delivery = MySimplePGSQLOperator(
        task_id='schema_init_delivery',
        conn_id=CONNECTION_WAREHOUSE,
        variable_sql_path=VARIABLE_DELIVERY_SQL,
        logger=logger,
        dag=dag
    )

    schema_init >> schema_init_delivery
