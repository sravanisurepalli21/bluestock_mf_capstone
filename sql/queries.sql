-- QUERY 1: Top 5 Funds by AUM

SELECT
scheme_name,
fund_house,
aum_crore
FROM fact_performance
ORDER BY aum_crore DESC
LIMIT 5;


-- QUERY 2: Average NAV by Month

SELECT
strftime('%Y-%m', date) AS month,
ROUND(AVG(nav),2) AS average_nav
FROM fact_nav
GROUP BY month
ORDER BY month;


-- QUERY 3: Total Transactions by State

SELECT
state,
COUNT(*) AS total_transactions
FROM fact_transactions
GROUP BY state
ORDER BY total_transactions DESC;


-- QUERY 4: Funds with Expense Ratio Less Than 1%

SELECT
scheme_name,
fund_house,
expense_ratio_pct
FROM fact_performance
WHERE expense_ratio_pct < 1
ORDER BY expense_ratio_pct;


-- QUERY 5: Top 5 Funds by 5 Year Return

SELECT
scheme_name,
return_5yr_pct
FROM fact_performance
ORDER BY return_5yr_pct DESC
LIMIT 5;


-- QUERY 6: Top 10 Funds by Sharpe Ratio

SELECT
scheme_name,
sharpe_ratio
FROM fact_performance
ORDER BY sharpe_ratio DESC
LIMIT 10;


-- QUERY 7: Transaction Type Distribution

SELECT
transaction_type,
COUNT(*) AS total_count
FROM fact_transactions
GROUP BY transaction_type
ORDER BY total_count DESC;


-- QUERY 8: Average 3-Year Return by Category

SELECT
category,
ROUND(AVG(return_3yr_pct),2) AS avg_return
FROM fact_performance
GROUP BY category
ORDER BY avg_return DESC;


-- QUERY 9: Top Fund Houses by Total AUM

SELECT
fund_house,
SUM(aum_crore) AS total_aum
FROM fact_performance
GROUP BY fund_house
ORDER BY total_aum DESC;


-- QUERY 10: Average Transaction Amount by Gender

SELECT
gender,
ROUND(AVG(amount_inr),2) AS avg_transaction_amount
FROM fact_transactions
GROUP BY gender
ORDER BY avg_transaction_amount DESC;
