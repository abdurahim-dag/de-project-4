from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable


class MySimplePGSQLOperator(BaseOperator):
    """Простой оператор выполнения sql в PG."""
    def __init__(
        self,
        conn_id: str,
        variable_sql_path: str,
        logger,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ddl_path = Variable.get(variable_sql_path)
        self.conn_id = conn_id
        self.logger = logger

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        query = open(self.ddl_path, encoding='utf-8').read()
        hook.run(query)
        for output in hook.conn.notices:
            self.logger.info(output)
