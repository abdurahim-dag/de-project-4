import re
from contextlib import contextmanager
from logging import Logger
from typing import Any, Iterable, Mapping, Union
from typing import Generator
from typing import Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

from realization.backoff import on_exception
from realization.logger import logger
from realization.storage import WorkflowStorage


class Loader:
    LAST_LOADED_KEY = 'last_loaded' # Название ключа состояния в хранилище состояний.
    WORKFLOW_SCHEMA = 'dds' # Таргет схема.

    def __init__(
        self,
        con_id: str,
        query_path: str,
        log: Logger,
        wf_key: str,
        start_workflow_key: Any,
        is_scd2: bool = False,
    ) -> None:
        """ Инициализация стартовых параметров.
        :param con_id: Название соединения DWH в Airflow.
        :param query_path: Путь до шаблона sql insert запроса
        :param log: Логгер объект.
        :param wf_key: Значение ключа состояния.
        :param start_workflow_key: Значение по умолчанию стартового состояния.
        :param is_scd2: Необходима поддержка SCD 2.
        """
        self.wf_key = wf_key
        self.con_id = con_id
        self.query_path = query_path
        self.log = log
        self.is_scd2 = is_scd2
        self.start_workflow_key = start_workflow_key

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
    def _connection(self,)  -> Generator[psycopg2.extensions.connection, None, None]:
        """ Контекстный менеджер подкючения к БД DWH.
        :rtype: соединение к БД DWH.
        """
        # Подключение забираем из Airflow.
        hook = PostgresHook(postgres_conn_id=self.con_id)
        conn = hook.get_conn()

        # Так как у нас скриптовая загрузка состоит из двух запросов - insert и select max(update_ts),
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


    def _insert(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        parameters: Union[Iterable, Mapping, None] = None
    ) -> None:
        """Исполняем шаблонный запрос insert с заданными параметрами в БД."""
        curs: psycopg2.extensions.cursor
        with conn.cursor() as curs:
            curs.execute(query, parameters)
            if curs.rowcount >= 0:
                self.log.info("Rows affected: %s", curs.rowcount)

    def _last_loaded(
        self,
        conn: psycopg2.extensions.connection,
        query: str,
        parameters: Union[Iterable, Mapping, None] = None
    ) -> Optional[tuple]:
        """Вычленяет запрос из query на получение последней загруженной(insert) записи и исполняет в БД."""
        curs: psycopg2.extensions.cursor
        with conn.cursor() as curs:
            m = re.search(r"(\/\*)((.|\s)*?)(\*/)", query)
            curs.execute(m[2], parameters)
            return curs.fetchone()[0]

    def load(self):
        """Функция загрузки."""
        # Получаем из хранилища последнее сохраненное состояние.
        wf_storage = WorkflowStorage(
            conn_id=self.con_id, # соединение из Airflow где хранится
            etl_key=self.wf_key, # ключ поиска
            workflow_settings={
                # значение по умолчанию
                self.LAST_LOADED_KEY: self.start_workflow_key,
            },
            schema=self.WORKFLOW_SCHEMA # из какой схемы брать состояние
        )
        query_dest = open(self.query_path, encoding='utf-8').read()

        with self._connection() as conn:

            # В случаи необходимости поддержки scd2, sql нужно выполнить дважды.
            # В первый раз скрипт в случаи on conflict должен будет подставить active_to = EXCLUDED.active_from.
            # Во второй раз уже не будет конфликтов и запись с новой версией добавиться.
            for _ in set([False, self.is_scd2]):
                wf_setting = wf_storage.retrieve_state()

                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_KEY]

                parameters = {
                    self.LAST_LOADED_KEY: last_loaded
                }
                self._insert(conn=conn, query=query_dest, parameters=parameters)

                conn.commit()

                # В query_dest зашит sql, для получения last_loaded.
                last_loaded = self._last_loaded(conn=conn, query=query_dest, parameters=parameters)

                # Сохраняем прогресс.
                if last_loaded:
                    wf_setting.workflow_settings[self.LAST_LOADED_KEY] = last_loaded
                    wf_storage.save_state(wf_setting)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_KEY]}")

        self.log.info("Quitting.")
