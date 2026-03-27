-- =============================================
-- СЛОЙ DATA MARTS (Витрины)
-- =============================================

-- Создаем схему для витрин
CREATE SCHEMA IF NOT EXISTS dm_risk;

-- ВИТРИНА 1: Финансовый профиль клиента
-- оконные функции для расчета трендов
DROP TABLE IF EXISTS dm_risk.client_financial_profile;
CREATE TABLE dm_risk.client_financial_profile AS

WITH monthly_aggregates AS (
    -- Базовая агрегация: считаем обороты помесячно
    SELECT 
        f.client_sk,
        d.year_num,
        EXTRACT(MONTH FROM d.actual_date)::INT AS month_num,
        COALESCE(SUM(CASE WHEN tt.type_name = 'INCOME' THEN f.amount_rub ELSE 0 END), 0) as income_monthly,
        COALESCE(SUM(CASE WHEN tt.type_name = 'OUTCOME' THEN f.amount_rub ELSE 0 END), 0) as outcome_monthly,
        COUNT(f.fact_id) as tx_count_monthly
    FROM dwh.fact_transactions f
    JOIN dwh.dim_date d
        ON f.date_id = d.date_id
    JOIN dwh.dim_transaction_type tt
        ON f.type_id = tt.type_id
    GROUP BY f.client_sk, d.year_num, EXTRACT(MONTH FROM d.actual_date)::INT
),

client_trends AS (
    -- Оконные функции: считаем тренды и скользящие средние
    SELECT 
        client_sk,
        income_monthly,
        -- Скользящее среднее дохода за 3 месяца (сглаживаем колебания)
        AVG(income_monthly) OVER (
            PARTITION BY client_sk 
            ORDER BY year_num, month_num 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg_income_3m,
        
        -- Доход за прошлый месяц (чтобы сравнить с текущим)
        LAG(income_monthly, 1) OVER (
            PARTITION BY client_sk 
            ORDER BY year_num, month_num
        ) as prev_month_income,
        
        year_num,
        month_num
    FROM monthly_aggregates
),

last_month_data AS (
    -- Берем данные только за последний доступный месяц (срез состояния)
    SELECT DISTINCT ON (client_sk) 
        client_sk,
        income_monthly as last_income,
        moving_avg_income_3m,
        prev_month_income,
        CASE 
            WHEN prev_month_income IS NULL OR prev_month_income = 0 THEN 'NEW'
            WHEN income_monthly > prev_month_income THEN 'GROWTH'
            WHEN income_monthly < prev_month_income THEN 'DECLINE'
            ELSE 'STABLE'
        END as income_trend_label
    FROM client_trends
    ORDER BY client_sk, year_num DESC, month_num DESC
)

-- Финальная сборка витрины
SELECT 
    c.client_sk,
    c.client_id_nk,
    c.company_name,
    c.industry,
    c.region,
    c.is_active,
    
    -- Финансовые метрики
    COALESCE(l.last_income, 0) as last_month_income,
    COALESCE(l.moving_avg_income_3m, 0) as avg_income_3m,
    l.income_trend_label,
    
    -- Аддитивные метрики (всё время)
    COALESCE(SUM(ma.income_monthly), 0) as total_income_all_time,
    COALESCE(SUM(ma.tx_count_monthly), 0) as total_transactions,
    
    -- Флаг для аномалий
    -- Если максимальная транзакция клиента в 10 раз больше среднего чека - это аномалия
    CASE 
        WHEN MAX(f.amount_rub) > (AVG(f.amount_rub) * 10) THEN 1 
        ELSE 0 
    END as has_anomaly_tx,

    -- Собираем тексты транзакций для NLP (Prompt Engineering)
    -- В Postgres есть удобная функция string_agg
    string_agg(DISTINCT LEFT(f.description, 50), ' | ') as transaction_keywords_sample

FROM dwh.dim_client c
LEFT JOIN last_month_data l ON c.client_sk = l.client_sk
LEFT JOIN monthly_aggregates ma ON c.client_sk = ma.client_sk
LEFT JOIN dwh.fact_transactions f ON c.client_sk = f.client_sk
GROUP BY 
    c.client_sk, c.client_id_nk, c.company_name, c.industry, c.region, c.is_active,
    l.last_income, l.moving_avg_income_3m, l.income_trend_label;

-- Создаем индекс для быстрого доступа
CREATE INDEX idx_dm_risk_client ON dm_risk.client_financial_profile(client_sk);

-- Комментарий к таблице
COMMENT ON TABLE dm_risk.client_financial_profile IS 'Витрина для скоринга: финансовое состояние и тренды клиентов';