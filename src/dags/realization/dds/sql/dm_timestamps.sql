/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_orders
where update_ts > %(last_loaded)s
*/
insert into dds.dm_timestamps (id, ts, year, month, day, time, date)
with order_date as (
    select
        id,
        object_value::json->>'date' as "ts",
        object_value::json->>'final_status' as "status"
    from stg.ordersystem_orders
    where update_ts > %(last_loaded)s
),
order_ts as (
    select
        id,
        date_trunc('seconds', ts::timestamp) as "ts"
    from order_date
    where status = 'CLOSED' or status = 'CANCELLED'
)
select
    id,
    ts,
    extract(year from ts) as "year",
    extract(month from ts) as "month",
    extract(day from ts) as "day",
    ts::time as "time",
    ts::date as "date"
from order_ts
on conflict (id) do update set
    ts = EXCLUDED.ts,
    year = EXCLUDED.year,
    month = EXCLUDED.month,
    day = EXCLUDED.day,
    time = EXCLUDED.time,
    date = EXCLUDED.date;
