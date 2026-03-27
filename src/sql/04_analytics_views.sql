-- =============================================
-- СЛОЙ АНАЛИТИЧЕСКИХ ПРЕДСТАВЛЕНИЙ
-- =============================================

CREATE SCHEMA IF NOT EXISTS dwh_views;

-- -------------------------------------------------------
-- VIEW 1: Ежемесячный оборот по клиентам
-- Используется для трендовых отчётов
-- -------------------------------------------------------
CREATE OR REPLACE VIEW dwh_views.v_monthly_client_summary AS
SELECT
    c.client_id_nk,
    c.company_name,
    c.industry,
    c.region,
    d.year_num,
    EXTRACT(MONTH FROM d.actual_date)::INT AS month_num,
    SUM(CASE WHEN tt.type_name = 'INCOME'  THEN f.amount_rub ELSE 0 END) AS total_income,
    SUM(CASE WHEN tt.type_name = 'OUTCOME' THEN f.amount_rub ELSE 0 END) AS total_outcome,
    SUM(CASE WHEN tt.type_name = 'INCOME'  THEN f.amount_rub ELSE 0 END) -
    SUM(CASE WHEN tt.type_name = 'OUTCOME' THEN f.amount_rub ELSE 0 END) AS net_cashflow,
    COUNT(f.fact_id) AS tx_count
FROM dwh.fact_transactions f
JOIN dwh.dim_client          c  ON f.client_sk = c.client_sk
JOIN dwh.dim_date             d  ON f.date_id   = d.date_id
JOIN dwh.dim_transaction_type tt ON f.type_id   = tt.type_id
GROUP BY
    c.client_id_nk, c.company_name, c.industry, c.region,
    d.year_num, EXTRACT(MONTH FROM d.actual_date)::INT;

-- -------------------------------------------------------
-- VIEW 2: Риск-статистика по отраслям
-- Используется для портфельного анализа
-- -------------------------------------------------------
CREATE OR REPLACE VIEW dwh_views.v_industry_risk_summary AS
SELECT
    c.industry,
    COUNT(DISTINCT c.client_sk)                                   AS client_count,
    SUM(p.total_income_all_time)                                  AS total_income,
    SUM(p.total_transactions)                                     AS total_tx,
    SUM(p.has_anomaly_tx)                                         AS anomaly_client_count,
    ROUND(100.0 * SUM(p.has_anomaly_tx) / COUNT(DISTINCT c.client_sk), 1) AS anomaly_rate_pct,
    -- Распределение трендов по отрасли
    COUNT(*) FILTER (WHERE p.income_trend_label = 'GROWTH')  AS clients_growth,
    COUNT(*) FILTER (WHERE p.income_trend_label = 'STABLE')  AS clients_stable,
    COUNT(*) FILTER (WHERE p.income_trend_label = 'DECLINE') AS clients_decline
FROM dwh.dim_client c
JOIN dm_risk.client_financial_profile p ON c.client_sk = p.client_sk
GROUP BY c.industry
ORDER BY anomaly_rate_pct DESC;

-- -------------------------------------------------------
-- VIEW 3: Топ клиентов с аномальными транзакциями
-- Используется риск-офицерами для мониторинга AML
-- -------------------------------------------------------
CREATE OR REPLACE VIEW dwh_views.v_top_anomaly_clients AS
SELECT
    p.client_id_nk,
    p.company_name,
    p.industry,
    p.region,
    p.last_month_income,
    p.avg_income_3m,
    p.income_trend_label,
    p.total_transactions,
    -- Максимальная разовая транзакция клиента
    MAX(f.amount_rub) AS max_single_tx,
    -- Средний чек
    ROUND(AVG(f.amount_rub), 2) AS avg_tx_amount,
    -- Соотношение макс. транзакции к среднему чеку
    ROUND(MAX(f.amount_rub) / NULLIF(AVG(f.amount_rub), 0), 1) AS anomaly_ratio,
    p.transaction_keywords_sample
FROM dm_risk.client_financial_profile p
JOIN dwh.fact_transactions f ON p.client_sk = f.client_sk
WHERE p.has_anomaly_tx = 1
GROUP BY
    p.client_id_nk, p.company_name, p.industry, p.region,
    p.last_month_income, p.avg_income_3m, p.income_trend_label,
    p.total_transactions, p.transaction_keywords_sample
ORDER BY anomaly_ratio DESC;
