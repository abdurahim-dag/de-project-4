/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_restaurants
where update_ts > %(last_loaded)s
*/
insert into dds.dm_products as products(product_id, restaurant_id, product_name, product_price, active_from, active_to)
(
with product_objs as (
    select
        object_id as "restorant_id",
        json_array_elements(object_value::json->'menu') as "product_obj",
        update_ts as "active_from",
        '2099-12-31'::timestamp as "active_to"
-- Если продукты брать из ordersystem_orders
-- то из-за факта различия значения update_ts и в ordersystem_restaurants и в ordersystem_orders
-- берём продукты из ordersystem_restaurants, тогда с датами почти нет проблем.
    from stg.ordersystem_restaurants
    where update_ts > %(last_loaded)s
)
select
   p.product_obj ->> '_id' as "product_id",
   r.id as "restaurant_id",
   p.product_obj ->> 'name' as "product_name",
   (p.product_obj ->> 'price')::numeric(14,2) as "product_price",
   p.active_from,
   p.active_to
from product_objs p
join dds.dm_restaurants r on
    r.restaurant_id = p.restorant_id and
-- по хорошему должно быть так
--     p.active_from >= r.active_from and
--     p.active_from < r.active_to
-- но приходится так:
    r.active_from = p.active_from
)
on conflict (product_id, active_to) do update SET
    active_to = EXCLUDED.active_from;
-- И обновлять только с более свежей датой
--where products.active_from < EXCLUDED.active_from;