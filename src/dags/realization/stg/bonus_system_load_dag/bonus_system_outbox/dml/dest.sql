INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
ON CONFLICT (id) DO UPDATE
SET
    event_ts = EXCLUDED.event_ts,
    event_type = EXCLUDED.event_type,
    event_value = EXCLUDED.event_value;
