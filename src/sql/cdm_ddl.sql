drop table if exists STV2024021918__DWH.global_metrics;

create table if not exists STV2024021918__DWH.global_metrics (
	date_update date,
	currency_from integer,
	amount_total numeric(15,2),
	cnt_transactions integer,
	avg_transactions_per_account numeric(8,3),
	cnt_accounts_make_transactions integer
);