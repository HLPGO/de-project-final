drop table if exists stv2024021918__STAGING.currencies;

create table if not exists stv2024021918__STAGING.currencies (
    date_update timestamp null,
    currency_code integer,
    currency_code_with integer,
    currency_with_div numeric(5,3)

);

DROP TABLE if exists stv2024021918__STAGING.transactions;

CREATE TABLE stv2024021918__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from integer NULL,
	account_number_to integer NULL,
	currency_code integer NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount integer NULL,
	transaction_dt timestamp NULL
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 2, 1);
