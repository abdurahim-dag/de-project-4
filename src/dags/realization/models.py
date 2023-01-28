from pydantic import BaseModel, validator, Field, root_validator
from pendulum import DateTime, parse, datetime
from decimal import Decimal
from typing import List, Optional, Union
from enum import Enum


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class UserObj(BaseModel):
    id: int
    order_user_id: str


class OutboxObj(BaseModel):
    id: int
    event_ts: DateTime
    event_type: str
    event_value: str


class OrderObj(BaseModel):
    id: int = 0
    object_id: str
    object_value: str
    update_ts: DateTime


class RestaurantObj(OrderObj):
    pass


class UserOrderObj(OrderObj):
    pass


class OrderOrderObj(OrderObj):
    pass


class DeliveryRestaurant(BaseModel):
    restaurant_id: str = Field(alias='_id')
    restaurant_name: str = Field(alias='name')


class Courier(BaseModel):
    courier_id: str = Field(alias='_id')
    courier_name: str = Field(alias='name')


class Delivery(BaseModel):
    order_id: str
    order_ts: DateTime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: DateTime
    rate: int
    sum: Decimal
    tip_sum: Decimal


class DDSParams(BaseModel):
    """Модель параметров etl слоёв CDM и DDS"""
    con_id: str
    wf_key: str
    variable_query_path: str
    scd2: Optional[bool] = None
    start_workflow_key: Union[int, DateTime] = datetime(2022, 1, 1)

    class Config:
        # Проверка всех типов
        smart_union = True