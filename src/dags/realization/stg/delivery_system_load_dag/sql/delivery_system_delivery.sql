INSERT INTO stg.delivery_deliveries(object_id, object_value, delivery_ts)
VALUES (%(object_id)s, %(object_value)s, %(delivery_ts)s)
ON CONFLICT (object_id) DO UPDATE
 SET object_value = EXCLUDED.object_value,
     delivery_ts = EXCLUDED.delivery_ts;