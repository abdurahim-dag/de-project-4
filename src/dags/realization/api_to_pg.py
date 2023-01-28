import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from logging import Logger
from typing import Generator, List, Optional, Union
from typing import Type

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from airflow.models.variable import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from realization.backoff import on_exception
from realization.logger import logger
from realization.models import Courier, Delivery
from realization.storage import DummyStorage, Workflow, WorkflowStorage
from realization.workflow import DateWorkflowMixin, OffsetWorkflowMixin


class BaseLoader(ABC):
    """Основной класс загрузчика из API доставки - реализация загрузки в DWH."""
    BATCH_LIMIT: int = 1 # Размер пачки выгрузки
    Model: Union[Type[Courier], Type[Delivery]] # Таргет модель
    SCHEMA: str # Имя схемы в DWH

    def __init__(
        self,
        src_con_id: str,
        dest_con_id: str,
        query_variable: str,
        log: Logger,
        wf_key: str,
    ) -> None:
        """ Инициализация параметров.
        :param src_con_id: Имя соединения источника в Airflow.
        :param dest_con_id: Имя соединения DWH в Airflow.
        :param query_variable: Имя переменной в Airflow с путем до sql запроса insert в DWH.
        :param log: Логгер.
        :param wf_key: Ключ в БД состояния прогресса загрузки.
        """
        self.wf_key = wf_key
        self.src_con_id = src_con_id
        self.dest_con_id = dest_con_id
        self.query_variable = query_variable
        self.log = log

        # Объект, для доступа к сохранению и извлечению из хранилища состояния процесса ETL.
        self.wf_storage = self.get_workflow_storage()

    @abstractmethod
    def set_last_loaded(self, wf_setting: Workflow, load_queue: List[dict]) -> Workflow:
        """Метод должен устанавливать прогресс - учитывая массив выгруженных данных из источника load_queue."""
        pass

    @abstractmethod
    def get_load_queue(self, wf_setting: Workflow) -> List[dict]:
        """Метод должен возвращать в загрузчик список из моделей(в виде словаря) полученных из источника."""
        pass

    @abstractmethod
    def get_last_loaded(self) -> Workflow:
        """Метод должен возвращать последний прогресс из хранилища."""
        pass

    @abstractmethod
    def get_workflow_settings(self) -> dict:
        """Генерация тела, для загручика, параметров начального состояния(workflow_settings)."""
        pass

    def get_workflow_storage(self) -> WorkflowStorage:
        """Возвращает хранилище состояния прогресса."""
        return WorkflowStorage(
            conn_id=self.dest_con_id, # название соединения в Airflow БД состояния
            etl_key=self.wf_key, # Ключ идентификации в БД прогресса
            workflow_settings=self.get_workflow_settings(), # Значение по умолчанию начального состояния прогресса.
            schema=self.SCHEMA # Схема в БД, где хранится состояние
        )

    # Backoff подключения к DWH.
    @on_exception(
        exception=psycopg2.DatabaseError,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
        logger=logger,
    )
    @contextmanager
    def get_dest_connection(
            self,
    ) -> Generator[psycopg2.extensions.connection, None, None]:
        """ Контекстный менеджер подкючения к БД DWH."""
        # Подключение забираем из Airflow.
        hook = PostgresHook(postgres_conn_id=self.dest_con_id)
        conn = hook.get_conn()

        # Так как у нас скриптовая загрузка состоит из нескольких запросов - insert и select max(update_ts),
        # то откинем все возможные нежданчики.
        conn.autocommit = False
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)

        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def insert_dest(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        load_queue: List[dict],
    ):
        """Загрузка списка объекта(моделей в виде словаря), посредством запроса на вставку в DWH."""
        curs: psycopg2.extensions.cursor
        with conn.cursor() as curs:
            curs.executemany(query, load_queue)
            if curs.rowcount >= 0:
                self.log.info("Rows affected: %s", curs.rowcount)

    def get_query(self) -> str:
        """Извлечение тела запроса из заданного файла."""
        query_dest_path = Variable.get(self.query_variable)
        return open(query_dest_path, encoding='utf-8').read()

    def load(self):
        """Процесс загрузки объектов в слой DWH."""
        with self.get_dest_connection() as conn:

            wf_setting = self.get_last_loaded() # Извлекаем состояние прогресса.

            query = self.get_query() # Извлекаем тело шаблонного запроса на вставку.

            try:
                # Цикл пока не получим пустой список.
                while True:

                    # Извлекаем из источника данные по критериям состояния.
                    load_queue = self.get_load_queue(wf_setting=wf_setting)

                    if load_queue:

                        # models = []
                        # # Составляем список объектов на загрузку из словарей по заданной модели.
                        # # Заодно и валидируем извлеченные данные из источника.
                        # for obj in load_queue:
                        #     models.append(self.Model(**obj))
                        #         data = [m.dict() for m in models]

                        # Вставляем объекты в DWH.
                        self.insert_dest(conn=conn, query=query, load_queue=load_queue)
                        # Устанавливаем текущие параметры состояния выгрузки.
                        wf_setting = self.set_last_loaded(wf_setting=wf_setting, load_queue=load_queue)

                        conn.commit() # Коммитим данные в DWH.

                        self.log.info(f"Loaded models: {len(load_queue)}")
                        self.log.info(f"Load on: {wf_setting.workflow_settings}")

                    if len(load_queue) == 0:
                        break

            except Exception as e:
                raise e

            finally:
                self.log.info(f"Load finished on: {wf_setting.workflow_settings}")
                # Сохраняем прогресс.
                self.wf_storage.save_state(wf_setting)

        self.log.info("Quitting.")


class APIOriginMixin:
    """Миксин добавляющий в загрузчик поддержку источника - API."""

    @staticmethod
    def _get_params(params: Optional[dict] = None) -> dict:
        """Параметры запроса только для ключей значения, которых не None"""
        res = {}
        if params:
            for k, v in params.items():
                if v is not None:
                    res[k] = v
        return res

    def get_load_queue(self, wf_setting: Workflow) -> List[dict]:
        """Возвращает в загрузчик список из моделей(в виде словаря) полученных из API."""
        src_connection = HttpHook(method='GET', http_conn_id=self.src_con_id)
        res = src_connection.run(
            endpoint=self.ENDPOINT, # Например ../restaurants?...
            data=self._get_params(wf_setting.workflow_settings), # Параметры запроса.
        ).json()
        return res


class STGLoader(BaseLoader):
    """Загрузчик, для слоя STG."""
    SCHEMA = 'stg'


class CourierSTGLoader(OffsetWorkflowMixin, APIOriginMixin, STGLoader):
    """Загрузчик из API данных о курьерах, для слоя STG DWH."""
    BATCH_LIMIT = 100
    Model = Courier
    ENDPOINT = 'couriers'

    def get_workflow_storage(self) -> DummyStorage:
        """Сохранение состояния не требуется."""
        return DummyStorage(
            workflow_settings=self.get_workflow_settings(), # Значение по умолчанию начального состояния прогресса.
        )

    def insert_dest(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        load_queue: List[dict],
    ):
        """Загрузка списка курьеров в DWH."""
        load_queue = [{'object_value': json.dumps(lq), 'object_id': lq['_id']} for lq in load_queue]
        super().insert_dest(conn=conn, query=query, load_queue=load_queue)


class DeliverySTGLoader(DateWorkflowMixin, APIOriginMixin, STGLoader):
    """Загрузчик из API данных о доставках, для слоя STG DWH."""
    BATCH_LIMIT = 1000
    Model = Delivery
    ENDPOINT = 'deliveries'

    def insert_dest(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        load_queue: List[dict],
    ):
        """Загрузка списка курьеров в DWH."""
        load_queue = [{'object_value': json.dumps(lq), 'object_id': lq['delivery_id'], 'delivery_ts': lq['delivery_ts']} for lq in load_queue]
        super().insert_dest(conn=conn, query=query, load_queue=load_queue)
