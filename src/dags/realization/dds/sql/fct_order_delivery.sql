/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(delivery_ts)
from dds.dm_deliveries
where delivery_ts > %(last_loaded)s
*/
insert into dds.fct_order_delivery (delivery_id, restaurant_id, courier_id, order_id, order_timestamp_id, rate, order_sum, tip_sum)
with stg_orders as (
    select o.id as "order_id",
       (json_array_elements(o.object_value::json -> 'order_items') ->> 'price')::numeric(14, 2) as "product_price",
       (json_array_elements(o.object_value::json -> 'order_items') ->> 'quantity')::int as "product_quantity",
       o.object_value::json->>'final_status' as "order_status"
    from stg.ordersystem_orders o),
orders_agg as (
    select order_id,
       sum(stg_orders.product_quantity * stg_orders.product_price) as "order_sum"
    from stg_orders
    where stg_orders.order_status = 'CLOSED'
    group by stg_orders.order_id),
orders as (
    select o.id, o.restaurant_id, o.timestamp_id, orders_agg.order_sum
    from dds.dm_orders o
    join orders_agg on orders_agg.order_id = o.id)
select d.id, orders.restaurant_id, d.courier_id, orders.id, orders.timestamp_id, d.rate, orders.order_sum, d.tip_sum
from dds.dm_deliveries d
join orders on orders.id = d.order_id
where d.delivery_ts > %(last_loaded)s
on conflict (order_id) do update set
    delivery_id = EXCLUDED.delivery_id,
    restaurant_id = EXCLUDED.restaurant_id,
    courier_id = EXCLUDED.courier_id,
    order_id = EXCLUDED.order_id,
    order_timestamp_id = EXCLUDED.order_timestamp_id,
    rate = EXCLUDED.rate,
    order_sum = EXCLUDED.order_sum,
    tip_sum = EXCLUDED.tip_sum;