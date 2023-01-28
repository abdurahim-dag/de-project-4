/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_orders
where update_ts > %(last_loaded)s
*/
insert into dds.dm_orders (id, user_id, restaurant_id, timestamp_id, order_key, order_status)
with orders as (
    select
        id,
        update_ts as "active_from",
        object_value::json->>'_id' as "order_key",
        object_value::json->>'final_status' as "order_status",
        object_value::json->'restaurant'->>'id' as "restaurant_id",
        object_value::json->'user'->>'id' as "user_id"
    from stg.ordersystem_orders
    where update_ts > %(last_loaded)s
)
select distinct on (o.id)
    o.id,
    u.id as "user_id",
    r.id as "restaurant_id",
    t.id as "timestamp_id",
    o.order_key,
    o.order_status
from orders o
join dds.dm_restaurants r on
    r.restaurant_id = o.restaurant_id and
-- по хорошему должно быть так
--     o.active_from >= r.active_from and
--     o.active_from < r.active_to
-- но приходится так(для случаев когда дата заказа меньше, чем дата active_from самой ранней записи в dm_restaurants):
-- Такое возможно при первой загрузке...
    ((r.active_from <= o.active_from and o.active_from < r.active_to) or
    r.active_to = '2099-12-31'::timestamp
    )
join dds.dm_timestamps t on t.id = o.id
join dds.dm_users u on u.user_id = o.user_id
-- Отсееваем возможные дубликаты - когда есть несколько версий restaurant для order, берем первый restaurant_id
-- для случаев когда дата заказа меньше, чем дата active_from самой ранней записи в dm_restaurants
order by o.id, r.restaurant_id asc
on conflict (id) do update SET
    user_id = EXCLUDED.user_id,
    restaurant_id = EXCLUDED.restaurant_id,
    timestamp_id = EXCLUDED.timestamp_id,
    order_key = EXCLUDED.order_key,
    order_status = EXCLUDED.order_status;
