with checks as (
    select *
from public_test.dm_settlement_report_actual a
full join public_test.dm_settlement_report_expected e on
    a.id = e.id and
    a.restaurant_id = e.restaurant_id and
    a.restaurant_name = e.restaurant_name and
    a.settlement_year = e.settlement_year and
    a.settlement_month = e.settlement_month and
    a.orders_total_sum = e.orders_total_sum and
    a.orders_bonus_payment_sum = e.orders_bonus_payment_sum and
    a.orders_bonus_granted_sum = e.orders_bonus_granted_sum and
    a.order_processing_fee = e.order_processing_fee and
    a.restaurant_reward_sum = e.restaurant_reward_sum and
    a.orders_count = e.orders_count
where a.id is null or  e.id is null
)
select now() as "test_date_time",
       'test_01' as "test_name", count( * ) = 0
from checks;