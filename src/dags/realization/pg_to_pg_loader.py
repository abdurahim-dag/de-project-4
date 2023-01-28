from logging import Logger
from typing import Iterable, Mapping, Union, Optional, Type

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from realization.backoff import on_exception
from realization.logger import logger
from realization.models import RankObj, UserObj
from realization.storage import WorkflowStorage


class Loader:
    BATCH_LIMIT = 1000  # Размер пачки выгрузки
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(
        self,
        orgin_con_id: str,
        dest_con_id: str,
        variable_origin_query_path: str,
        variable_dest_query_path: str,
        log: Logger,
        model: Union[Type[RankObj], Type[UserObj]],
        wf_key: str
    ) -> None:
        """ Инициализация стартовых параметров.
        :param orgin_con_id: Имя соединения источника в Airflow.
        :param dest_con_id: Имя соединения DWH в Airflow.
        :param variable_origin_query_path: Название переменной, где лежит запрос на выгрузку.
        :param variable_dest_query_path: Название переменной, где лежит запрос на загрузку.
        :param log: Логгер.
        :param model: Модель которой должна соответствовать запись из источника.
        :param wf_key: Ключ в БД состояния прогресса загрузки.
        """
        self.wf_key = wf_key
        self.orgin_con_id = orgin_con_id
        self.dest_con_id = dest_con_id
        self.dest_query_path = Variable.get(variable_dest_query_path)
        self.origin_query_path = Variable.get(variable_origin_query_path)
        self.log = log
        # Объект, для доступа к сохранению и извлечению из хранилища состояния процесса ETL.
        self.wf_storage = WorkflowStorage(
            conn_id=dest_con_id, # название соединения в Airflow БД состояния
            etl_key=self.wf_key, # Ключ идентификации в БД прогресса
            workflow_settings={ # Значение по умолчанию начального состояния прогресса.
                self.LAST_LOADED_ID_KEY: -1
            },
            schema='stg' # Схема в БД, где хранится состояние
        )
        self.model = model

    # Backoff подключения к DWH.
    @on_exception(
        exception=psycopg2.DatabaseError,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
        logger=logger,
    )
    def _get_conn(
        self,
        con_id: str,
    ) -> psycopg2.extensions.connection:
        """ Подкючение к БД DWH."""
        hook = PostgresHook(postgres_conn_id=con_id)
        conn = hook.get_conn()
        conn.autocommit = False
        return conn

    def _get_origin(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        parameters: Union[Iterable, Mapping, None] = None
    ) -> Optional[tuple]:
        """Извлекает данные из источника.
        :param conn: Соединение PG источника.
        :param query: Тело запроса на извлечение.
        :param parameters: Параметры к запросу.
        :return: Кортеж из выгружаемых строк.
        """
        curs: psycopg2.extensions.cursor
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(query, parameters)
            return curs.fetchall()

    def _insert_dest(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        model: Union[RankObj,UserObj]
    ) -> None:
        """Загружает данные в DWH.
        :param conn: Соединение PG DWH.
        :param query: Шаблон запроса insert на вставку объекта.
        :param model: Объект модели, для вставки в DWH.
        """
        curs: psycopg2.extensions.cursor
        with conn.cursor() as curs:
            # Заполняет запрос insert по данным из модели и запускает на БД.
            curs.execute(query, model.dict())
            if curs.rowcount >= 0:
                self.log.info("Rows affected: %s", curs.rowcount)

    def load(self):
        """Функция загрузки."""
        # Исходный шаблон запроса на извлечение из источника.
        query_orig = open(self.origin_query_path, encoding='utf-8').read()
        # Исходный шаблон запроса на вставку в DWH.
        query_dest = open(self.dest_query_path, encoding='utf-8').read()

        # Получаем соединения к БД-х.
        conn_dest = self._get_conn(self.dest_con_id)
        conn_orig = self._get_conn(self.orgin_con_id)

        try:
            while True:

                # Считываем последний прогресс.
                wf_setting = self.wf_storage.retrieve_state()
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

                # Считываем данные из источника.
                load_queue = self._get_origin(
                    conn_orig,
                    query_orig,
                    parameters={ "threshold": last_loaded, "limit": self.BATCH_LIMIT }
                )

                # Вставляем в DWH.
                for obj in load_queue:
                    self._insert_dest(conn_dest, query_dest, self.model(**obj))

                if len(load_queue) > 0:
                    self.log.info(f"Found {len(load_queue)} models to load.")
                else:
                    break

                conn_dest.commit()

                # Сохраняем прогресс.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t['id'] for t in load_queue])
                self.wf_storage.save_state(wf_setting)
                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
        except Exception as e:
            conn_dest.rollback()
            raise e
        finally:
            conn_dest.close()
            conn_orig.close()

        self.log.info("Quitting.")
