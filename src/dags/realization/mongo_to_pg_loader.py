""""""
import json
from logging import Logger
from typing import Type
from typing import Union

import pendulum
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

from realization.backoff import on_exception
from realization.logger import logger
from realization.models import RestaurantObj
from realization.mongo import MongoConnect, Reader
from realization.storage import WorkflowStorage
from realization.utils import MyEncoder


class Loader:
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 513

    def __init__(
        self,
        dest_con_id: str,
        dest_query_path: str,
        log: Logger,
        model: Union[Type[RestaurantObj], None],
        collection: str,
        wf_key: str,
        cert_path: str,
        db_user: str,
        db_pw: str,
        rs: str,
        db: str,
        host: str,
    ) -> None:
        """ Инициализация стартовых параметров.
        :param dest_con_id: Название соединения DWH.
        :param dest_query_path: Название переменной, где лежит запрос на загрузку.
        :param log: логгер.
        :param model: Модель которой должна соответствовать запись из источника.
        :param collection: Название коллекции Mongo.
        :param wf_key: Значение ключа состояния прогресса.
        :param cert_path: Путь до сертификата для Mongo.
        :param db_user: Mongo имя пользователя.
        :param db_pw: Mongo пароль.
        :param rs: Mongo реплика.
        :param db: Mongo БД.
        :param host: Mongo хост.
        """
        self.wf_key = wf_key
        self.dest_con_id = dest_con_id
        self.dest_query_path = dest_query_path
        self.log = log
        # Объект, для доступа к сохранению и извлечению из хранилища состояния процесса ETL.
        self.wf_storage = WorkflowStorage(
            conn_id=dest_con_id, # название соединения в Airflow БД состояния
            etl_key=self.wf_key, # Ключ идентификации в БД прогресса
            workflow_settings={ # Значение по умолчанию начального состояния прогресса.
                self.LAST_LOADED_KEY: str(pendulum.datetime(2022, 1, 1)),
            },
            schema='stg' # Схема в БД, где хранится состояние
        )
        self.model = model
        self.collection = collection
        self.cert_path = cert_path
        self.db_user = db_user
        self.db_pw = db_pw
        self.rs = rs
        self.db = db
        self.host = host

    # Backoff подключения к DWH.
    @on_exception(
        exception=psycopg2.DatabaseError,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
        logger=logger,
    )
    def _get_pg_conn(
        self,
        con_id: str,
    ) -> psycopg2.extensions.connection:
        hook = PostgresHook(postgres_conn_id=con_id)
        conn = hook.get_conn()
        conn.autocommit = False
        return conn

    def _get_mongo_conn(
        self,
    ) -> MongoConnect:
        return MongoConnect(self.cert_path, self.db_user, self.db_pw, self.host, self.rs, self.db, self.db)

    def _insert_dest(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        model: Union[RestaurantObj, None]
    ):
        curs: psycopg2.extensions.cursor
        with conn.cursor() as curs:
            curs.execute(query, model.dict())
            if curs.rowcount >= 0:
                self.log.info("Rows affected: %s", curs.rowcount)

    def load(self):
        query_dest = open(self.dest_query_path, encoding='utf-8').read()

        pg_conn = self._get_pg_conn(self.dest_con_id)

        mongo_conn = self._get_mongo_conn()
        mongo_reader = Reader(mc=mongo_conn, collection=self.collection)


        try:
            while True:
                wf_setting = self.wf_storage.retrieve_state()
                last_loaded = pendulum.parse(wf_setting.workflow_settings[self.LAST_LOADED_KEY])

                # проблема:
                # Если у нас n документов по одной какой-то дате.
                # Если в Mongo при filter по дате и limit
                # больше чем limit документов, т.е. n > limit.
                # То мы n - limit документов упустим.
                # Поэтому нужен еще один цикл, для перебора всех документов по одной дате при n > len(limit & filter).
                # Или по другому говоря потому, что за один цикл мы можеи не получить все документы.
                # Решаем её еще одни циклом.
                checked_infinity = True
                skip = 0
                limit = self.BATCH_LIMIT
                while True:

                    load_queue = mongo_reader.get(
                        load_threshold=last_loaded,
                        limit=limit,
                        skip=skip
                    )

                    for d in load_queue:
                        obj = {
                            'object_id': str(d['_id']),
                            'update_ts': d['update_ts'],
                            'object_value': json.dumps(d, cls=MyEncoder, sort_keys=True, ensure_ascii=False)}
                        self._insert_dest(pg_conn, query_dest, self.model(**obj))

                    if len(load_queue) > 0:
                        self.log.info(f"Found {len(load_queue)} models to load.")
                        checked_infinity = False
                    else:
                        break

                    skip += limit

                    pg_conn.commit()

                    # Сохраняем прогресс.
                    wf_setting.workflow_settings[self.LAST_LOADED_KEY] = max([d["update_ts"] for d in load_queue])
                    self.wf_storage.save_state(wf_setting)
                    self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_KEY]}")

                if checked_infinity:
                    break
        except Exception as e:
            pg_conn.rollback()
            raise e
        finally:
            pg_conn.close()

        self.log.info("Quitting.")
