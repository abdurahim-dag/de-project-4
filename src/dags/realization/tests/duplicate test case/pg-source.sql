---------------------------- duplicate test case -------------------------
INSERT INTO public_test.users
(order_user_id)
VALUES('test_case_duplicate_order_01');
INSERT INTO public_test.users
(order_user_id)
VALUES('test_case_duplicate_order_01');

--select * from public_test.users;

INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(0, 'JUNIOR', 0.05000, 0.00000);
INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(1, 'JUNIOR', 0.05000, 0.00000);
INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(2, 'MIDDLE', 0.10000, 5000.00000);
INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(3, 'MIDDLE', 0.10000, 5000.00000);
INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(4, 'SENIOR', 0.15000, 15000.00000);
INSERT INTO public_test.ranks
(id, "name", bonus_percent, min_payment_threshold)
VALUES(5, 'SENIOR', 0.15000, 15000.00000);

--select * from public_test.ranks;

INSERT INTO public_test.bonus_balance
(user_id, balance)
VALUES(1, 5);
INSERT INTO public_test.bonus_balance
(user_id, balance)
VALUES(2, 5);

--select * from public_test.bonus_balance;

INSERT INTO public_test.bonus_transactions
(user_id, order_id, product_id, product_price, product_count, order_ts, order_sum, payment_sum, granted_sum)
VALUES(1, 'test_case_duplicate_order_01', 'test_case_duplicate_product_01', 1, 0, '2022-01-01 00:00:00.000', 100, 0, 10);
INSERT INTO public_test.bonus_transactions
(user_id, order_id, product_id, product_price, product_count, order_ts, order_sum, payment_sum, granted_sum)
VALUES(1, 'test_case_duplicate_order_01', 'test_case_duplicate_product_01', 1, 0, '2022-01-01 00:00:00.000', 100, 0, 10);

--select * from public_test.bonus_transactions;

INSERT INTO public_test.user_ranks
(user_id, rank_id)
VALUES(1, 0);
INSERT INTO public_test.user_ranks
(user_id, rank_id)
VALUES(2, 0);

--select * from public_test.user_ranks;

INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'user_rank', '{"user_id": "1", "rank_id": "0", "rank_name": "JUNIOR", "rank_award": 0.05}');
INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'user_rank', '{"user_id": "1", "rank_id": "0", "rank_name": "JUNIOR", "rank_award": 0.05}');
INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'user_balance', '{"user_id": 1, "balance": 5.0}');
INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'user_balance', '{"user_id": 1, "balance": 5.0}');
INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'bonus_transaction', '{"user_id": 1, "order_id": "test_case_duplicate_order_01", "order_date": "2022-01-01 00:00:00", "product_payments": [{"product_id": "test_case_duplicate_product_01", "product_name": "test_case_duplicate_product_name_01", "price": 100, "quantity": 1, "product_cost": 100, "bonus_payment": 0.0, "bonus_grant": 5}]}');
INSERT INTO public_test.outbox
(event_ts, event_type, event_value)
VALUES('2022-01-01 00:00:00.000', 'bonus_transaction', '{"user_id": 1, "order_id": "test_case_duplicate_order_01", "order_date": "2022-01-01 00:00:00", "product_payments": [{"product_id": "test_case_duplicate_product_01", "product_name": "test_case_duplicate_product_name_01", "price": 100, "quantity": 1, "product_cost": 100, "bonus_payment": 0.0, "bonus_grant": 5}]}');

--select * from public_test.outbox;
---------------------------- duplicate test case -------------------------