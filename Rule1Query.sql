SELECT a.last_name, a.first_name, p.account_number, p.purchase_number, p.merchant_name, p.purchase_amount
FROM purchase p
JOIN account a ON a.account_number = p.account_number
ORDER BY p.purchase_amount