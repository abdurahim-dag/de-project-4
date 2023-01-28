/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(delivery_ts)
from stg.delivery_deliveries delivery
where delivery_ts > %(last_loaded)s
*/
insert into dds.dm_deliveries(delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
    select object_id,
           courier.id,
           orders.id,
           object_value->>'address',
           delivery_ts,
           (object_value->>'rate')::int,
           (object_value->>'tip_sum')::numeric(14,2)
     from stg.delivery_deliveries delivery
              join dds.dm_couriers courier on courier.courier_id = delivery.object_value->>'courier_id'
              join dds.dm_orders orders on orders.order_key = delivery.object_value->>'order_id'
     where delivery_ts > %(last_loaded)s
on conflict (order_id) do update set
    delivery_id = EXCLUDED.delivery_id,
    courier_id = EXCLUDED.courier_id,
    address = EXCLUDED.address,
    delivery_ts = EXCLUDED.delivery_ts,
    rate = EXCLUDED.rate,
    tip_sum = EXCLUDED.tip_sum;
