INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
ON CONFLICT (object_id) DO UPDATE
SET
    object_value = EXCLUDED.object_value,
    update_ts = EXCLUDED.update_ts;
