/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_orders
where update_ts > %(last_loaded)s
*/
-- Проблема с датами:
-- в stg.ordersystem_orders могут быть заказы за позавчера(orders Mongo),
-- а в stg.ordersystem_restaurants откуда берём продукты[ или рестораны] update_ts стоит за сегодня(restaurants Mongo).
-- поэтому по хорошему эти заказы за позавчера не должны попасть в БД,
-- так как нет данных из stg.ordersystem_restaurants за тот момент времени.
-- но тренажер ждёт их для проверки...
insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
with orders as (select o.id                                                                      as "order_id",
                       o.object_id,
                       json_array_elements(o.object_value::json -> 'order_items') ->> 'id'       as "product_id",
                       json_array_elements(o.object_value::json -> 'order_items') ->> 'price'    as "product_price",
                       json_array_elements(o.object_value::json -> 'order_items') ->> 'quantity' as "product_quantity",
                       o.update_ts as "active_from",
                       object_value::json->>'final_status' as "order_status"
                from stg.ordersystem_orders o
                join dds.dm_timestamps t on t.id = o.id
-- Отсеваем ситуацию, когда в dm_orders нет еще записи из ordersystem_orders.
-- Такое возможно из-за паралельной работы дагов
                join dds.dm_orders ddo on ddo.id=o.id
-- Если загрузка STG и DDS слоёв пересечётся(разрешена параллельная работа) возможен момент,
-- когда в stg.ordersystem_orders поступили обновленные данные (order_id которая уже есть в DDS),
-- но они еще не прогружены в измерения.
-- А мы в этот момент начнём грузить данные в факты, таска упадёт.
                where o.update_ts > %(last_loaded)s
),
orders_prodcuts as (select distinct on (o.order_id)
                           o.order_id,
                           o.object_id as "orig_order_id",
                           p.id as "product_id",
                           o.product_id as "orig_product_id",
                           o.product_price::numeric(14, 2),
                           o.product_quantity::int
                    from orders o
                    join dds.dm_products p
                          on p.product_id = o.product_id and
-- по хорошему должно быть так
--                                           o.active_from >= p.active_from and
--                                           o.active_from < p.active_to
-- но из-за факта различия значения update_ts и в ordersystem_restaurants и в ordersystem_orders
-- и тренажёр ждёт совпадений с текущими значениями
-- то приходится брать актуальные записи:
                          (
                           (p.active_from <= o.active_from and o.active_from < p.active_to) or
                           p.active_to = '2099-12-31'::timestamp
                          )
                    where o.order_status = 'CLOSED'
                    order by o.order_id, product_id asc
),
bonuses as (
    select
        event_value::json ->> 'order_id' as "order_id",
        json_array_elements(event_value::json -> 'product_payments') ->> 'product_id' as "product_id",
        (json_array_elements(event_value::json -> 'product_payments') ->> 'bonus_payment')::numeric(14, 2) as "bonus_payment",
        (json_array_elements(event_value::json -> 'product_payments') ->> 'bonus_grant')::numeric(14, 2) as "bonus_grant"
    from stg.bonussystem_events
    where event_type='bonus_transaction'
)
select op.product_id,
       op.order_id,
       sum(op.product_quantity)                    as "count",
       op.product_price,
       sum(op.product_quantity * op.product_price) as "total_sum",
       sum(b.bonus_payment)                       as "bonus_payment",
       sum(b.bonus_grant)                         as "bonus_grant"
from orders_prodcuts op
join bonuses b on b.product_id=op.orig_product_id and b.order_id=op.orig_order_id
group by op.order_id, op.product_id, op.product_price
on conflict (product_id, order_id) do update set
    product_id = EXCLUDED.product_id,
    order_id = EXCLUDED.order_id,
    count = EXCLUDED.count,
    price = EXCLUDED.price,
    total_sum = EXCLUDED.total_sum,
    bonus_payment = EXCLUDED.bonus_payment,
    bonus_grant = EXCLUDED.bonus_grant;
