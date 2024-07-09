INSERT INTO STV2024021918__DWH.global_metrics (date_update, currency_from, amount_total, 
												cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT 
    date_update, 
    currency_from, 
    amount_total, 
    cnt_transactions, 
    avg_transactions_per_account, 
    cnt_accounts_make_transactions
FROM (
       WITH 
t AS (
    SELECT * FROM STV2024021918__STAGING.transactions t
    WHERE status = 'done' 
        AND account_number_from > 0
        AND (RIGHT(transaction_type, 8) = 'incoming' OR RIGHT(transaction_type, 8) = 'outgoing')
),
c AS (
    SELECT * FROM STV2024021918__STAGING.currencies c
    WHERE currency_code = '420'
)
SELECT 
    COALESCE(c.date_update::date, MAX(t.transaction_dt::date)) AS date_update,
    t.currency_code AS currency_from,
    SUM(ABS(t.amount)) * COALESCE(MIN(c.currency_with_div), 1) AS amount_total,
    COUNT(DISTINCT t.operation_id) AS cnt_transactions,
    COUNT(DISTINCT t.operation_id) / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM t
LEFT JOIN c ON t.transaction_dt::date = c.date_update AND t.currency_code = c.currency_code_with
GROUP BY c.date_update, t.currency_code
ORDER BY date_update::date
) AS source
WHERE NOT EXISTS (
    SELECT 1 
    FROM STV2024021918__DWH.global_metrics 
    WHERE STV2024021918__DWH.global_metrics.date_update = source.date_update 
    AND STV2024021918__DWH.global_metrics.currency_from = source.currency_from
);
