DROP table if exists public_test.dm_settlement_report_actual;
CREATE table if not exists public_test.dm_settlement_report_actual (
    id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    restaurant_id int4 NOT NULL,
    restaurant_name varchar NOT NULL,
    settlement_year int2 NOT NULL,
    settlement_month int2 NOT NULL,
    orders_count int4 NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
    orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
    orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
    restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
    CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
    CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
    CONSTRAINT dm_settlement_report_orders_count_check CHECK ((orders_count >= 0)),
    CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
    CONSTRAINT dm_settlement_report_pkey_actual PRIMARY KEY (id),
    CONSTRAINT dm_settlement_report_restaurant_id_settlement_year_settleme_ac UNIQUE (restaurant_id, settlement_year, settlement_month),
    CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
    CONSTRAINT dm_settlement_report_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
    CONSTRAINT dm_settlement_report_settlement_year_check CHECK (((settlement_year >= 2022) AND (settlement_year < 2500)))
);