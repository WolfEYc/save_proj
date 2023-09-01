SELECT a.last_name, a.first_name, a.account_number, p.purchase_number, a.account_state, p.merchant_state
FROM purchase p
JOIN account a ON a.account_number = p.account_number
WHERE a.account_state <> p.merchant_state