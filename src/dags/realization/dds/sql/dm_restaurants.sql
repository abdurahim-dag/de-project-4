/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_restaurants
where update_ts > %(last_loaded)s
*/
insert into dds.dm_restaurants as r( restaurant_id, restaurant_name, active_from, active_to)
(
select
    object_id as "restaurant_id",
    object_value::json->>'name' as "restaurant_name",
    update_ts as "active_from",
    '2099-12-31'::timestamp as "active_to"
from stg.ordersystem_restaurants
where update_ts > %(last_loaded)s
)
on conflict (restaurant_id, active_to) do update SET
    active_to = EXCLUDED.active_from;
-- И обновлять только с более свежей датой
--where r.active_from < EXCLUDED.active_from;