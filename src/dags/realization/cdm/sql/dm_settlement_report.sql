/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(active_from)
from dds.dm_products
where active_from > %(last_loaded)s
*/

insert into cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
with restoran_product as (
    select p.id as "p_id",
           p.product_id as "product_id",
           r.restaurant_id,
           r.restaurant_name,
           r.id as "r_id",
           p.active_from
    from dds.dm_products p
    join dds.dm_restaurants r on r.id = p.restaurant_id and r.active_from=p.active_from
    where p.active_from > %(last_loaded)s
),
restoran_product_sales as (
    select t.date,
           fps.order_id,
           rp.restaurant_id,
           rp.restaurant_name,
           sum(fps.total_sum) as "orders_total_sum",
           sum(fps.bonus_payment) as "orders_bonus_payment_sum",
           sum(fps.bonus_grant) as "orders_bonus_granted_sum",
           sum(fps.total_sum) * 0.25 as "order_processing_fee",
           sum(fps.total_sum) - sum(fps.total_sum) * 0.25 - sum(fps.bonus_payment) as "restaurant_reward_sum"
    from dds.fct_product_sales fps
    join dds.dm_timestamps t on t.id = fps.order_id
    join restoran_product rp on rp.p_id=fps.product_id
    group by fps.order_id, t.date, rp.restaurant_id, rp.restaurant_name
)
    select rps.restaurant_id,
           rps.restaurant_name,
           rps.date as "settlement_date",
           count(rps.order_id) as "orders_count",
           sum(rps.orders_total_sum) as "orders_total_sum",
           sum(rps.orders_bonus_payment_sum) as "orders_bonus_payment_sum",
           sum(rps.orders_bonus_granted_sum) as "orders_bonus_granted_sum",
           sum(rps.order_processing_fee) as "order_processing_fee",
           sum(rps.restaurant_reward_sum) as "restaurant_reward_sum"
    from restoran_product_sales rps
    group by rps.date, rps.restaurant_id, rps.restaurant_name
on conflict (restaurant_id, settlement_date) do update set
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
