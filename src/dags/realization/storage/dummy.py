from .base import BaseStorage
from .model import Workflow


class DummyStorage(BaseStorage):
    """Класс извлечения и сохранения состояния прогресса."""
    def __init__(
        self,
        conn_id: str = '',
        etl_key: str = '',
        workflow_settings: dict = {},
        schema: str = ''
    ):
        self.conn_id = conn_id
        self.etl_key = etl_key
        self.workflow_settings = workflow_settings
        self.schema = schema

    def save_state(self, state: Workflow) -> None:
        pass

    def retrieve_state(self) -> Workflow:
        return Workflow(id=0, workflow_key=self.etl_key, workflow_settings=self.workflow_settings )
