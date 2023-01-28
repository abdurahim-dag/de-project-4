/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(id)
from stg.delivery_couriers
where id > %(last_loaded)s
*/
insert into dds.dm_couriers (id, courier_id, courier_name)
select id, object_id, object_value->>'name'
from stg.delivery_couriers
where id > %(last_loaded)s
on conflict (id) do update set
    courier_id = EXCLUDED.courier_id,
    courier_name = EXCLUDED.courier_name;
