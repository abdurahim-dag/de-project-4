INSERT INTO stg.bonussystem_users(id, order_user_id)
VALUES (%(id)s, %(order_user_id)s)
ON CONFLICT (id) DO UPDATE
SET
    order_user_id = EXCLUDED.order_user_id;
