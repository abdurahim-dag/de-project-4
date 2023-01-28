/*
-- этот sql заберёт код
-- чтобы задать значение last_loaded
select max(update_ts)
from stg.ordersystem_users
where update_ts > %(last_loaded)s
*/
insert into dds.dm_users (id, user_id, user_name, user_login)
(
select
    id,
    object_id as "user_id",
    object_value::json->>'name' as "user_name",
    object_value::json->>'login' as "user_login"
from stg.ordersystem_users
where update_ts > %(last_loaded)s
)
on conflict (id) do update SET
    user_id = EXCLUDED.user_id,
    user_name = EXCLUDED.user_name,
    user_login = EXCLUDED.user_login;
