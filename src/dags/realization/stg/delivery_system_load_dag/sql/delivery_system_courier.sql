INSERT INTO stg.delivery_couriers(object_id, object_value)
VALUES (%(object_id)s, %(object_value)s)
ON CONFLICT (object_id) DO UPDATE
 SET object_value = EXCLUDED.object_value;
