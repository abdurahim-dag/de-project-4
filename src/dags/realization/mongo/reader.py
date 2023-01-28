from datetime import datetime
from typing import Dict, List
from pymongo.errors import ConnectionFailure
from bson.codec_options import CodecOptions
from .connection import MongoConnect
from realization.backoff import on_exception
from realization.logger import logger

class Reader:

    def __init__(self, mc: MongoConnect, collection: str) -> None:
        self.dbs = mc.client()
        self.collection = collection

    @on_exception(
        exception=ConnectionFailure,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
        logger=logger,
    )
    def get(self, load_threshold: datetime, limit, skip) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1), ('_id', 1)]

        options = CodecOptions(tz_aware=True)
        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection(self.collection, codec_options=options).find(filter=filter, sort=sort, limit=limit, skip=skip))
        return docs
