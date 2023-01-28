from typing import List
from abc import ABC
import pendulum

from realization.storage import Workflow, WorkflowStorage


class BaseWorkflowMixin(ABC):
    """Базовый класс, для миксинов прогресса."""

    def get_last_loaded(self) -> Workflow:
        """Метод возвращает последний прогресс из хранилища."""
        wf_setting = self.wf_storage.retrieve_state()
        wf_setting.workflow_settings['limit'] = self.BATCH_LIMIT
        return  wf_setting

    def save_state(self, wf_setting: Workflow) -> None:
        """Логика сохранения прогресса."""
        self.wf_storage.save_state(wf_setting)


class OffsetWorkflowMixin(BaseWorkflowMixin):
    """Миксин реализующий логику по установке параметров прогресса на основе смещения в запросе к API."""

    def get_workflow_settings(self) -> dict:
        """Реализация генерации тела, для загручика, параметров начального состояния(workflow_settings)."""
        return {
            'limit': 1,
            'offset': 0,
        }

    def set_last_loaded(self, wf_setting: Workflow, load_queue: List[dict]) -> Workflow:
        """Метод устанавливает прогресс - увеличив смешение на число элементов в массиве load_queue."""
        wf_setting.workflow_settings['offset'] += len(load_queue)
        return wf_setting


class DateWorkflowMixin(BaseWorkflowMixin):
    """Миксин реализующий логику по установке прогресса на основе даты в запросе к API."""

    def set_last_loaded(self, wf_setting: Workflow, load_queue: List[dict]) -> Workflow:
        """Метод устанавливает прогресс - максимальную дату(delivery_ts) из загружаемых моделей."""
        max_load = max([d["delivery_ts"] for d in load_queue])
        wf_setting.workflow_settings['from'] = pendulum.parse(max_load).strftime('%Y-%m-%d %H:%M:%S')
        return wf_setting

    def get_workflow_settings(self) -> dict:
        """Генерация тела, для загручика, параметров начального состояния(workflow_settings)."""
        return {
            'sort_field': ['date'],
            'limit': 1,
            'offset': 0,
            'sort_direction': 'asc',
            'from': pendulum.datetime(2022, 1, 1).strftime('%Y-%m-%d %H:%M:%S')
        }
