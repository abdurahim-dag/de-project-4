/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(id)
from dds.fct_order_delivery
where id > %(last_loaded)s
*/
insert into cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with order_delivery as (
    select
        f.courier_id,
        t.year "settlement_year",
        t.month "settlement_month",
        count(f.order_id) "orders_count",
        sum(f.order_sum) "orders_total_sum",
        avg(f.rate) "rate_avg",
        sum(f.order_sum) * 0.25 "order_processing_fee",
        sum(f.tip_sum) "courier_tips_sum",
        case when avg(f.rate) < 4 then GREATEST(sum(f.order_sum) * 0.05, 100)
             when 4 <= avg(f.rate) and avg(f.rate) < 4.5 then GREATEST(sum(f.order_sum) * 0.07, 150)
             when 4.5 <= avg(f.rate) and avg(f.rate) < 4.9 then  GREATEST(sum(f.order_sum) * 0.08, 175)
             when avg(f.rate) >= 4.9 then GREATEST(sum(f.order_sum) * 0.1, 200)
        END "courier_order_sum"
    from dds.fct_order_delivery f
    join dds.dm_timestamps t on t.id = f.order_timestamp_id
    where f.id > %(last_loaded)s
    group by f.courier_id, t.year, t.month
)
select c.courier_id,
       c.courier_name,
       d.settlement_year,
       d.settlement_month,
       orders_count,
       orders_total_sum,
       rate_avg,
       order_processing_fee,
       courier_order_sum,
       courier_tips_sum,
       courier_order_sum + courier_tips_sum * 0.95 "courier_reward_sum"
from order_delivery d
join dds.dm_couriers c on c.id = d.courier_id
on conflict (courier_id, settlement_year, settlement_month) do update set
       courier_name = EXCLUDED.courier_name,
       orders_count = EXCLUDED.orders_count,
       orders_total_sum = EXCLUDED.orders_total_sum,
       rate_avg = EXCLUDED.rate_avg,
       order_processing_fee = EXCLUDED.order_processing_fee,
       courier_order_sum = EXCLUDED.courier_order_sum,
       courier_tips_sum = EXCLUDED.courier_tips_sum,
       courier_reward_sum = EXCLUDED.courier_order_sum + EXCLUDED.courier_tips_sum * 0.95;
