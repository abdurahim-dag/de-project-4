INSERT INTO stg.delivery_restaurants(restaurant_id, restaurant_name)
VALUES (%(restaurant_id)s, %(restaurant_name)s)
ON CONFLICT (restaurant_id) DO UPDATE
SET restaurant_name = EXCLUDED.restaurant_name;
