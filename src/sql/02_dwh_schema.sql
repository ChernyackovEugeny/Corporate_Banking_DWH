-- =============================================
-- СЛОЙ DWH (Core Layer - Звезда)
-- Хранилище данных
-- =============================================

-- Измерение: Дата
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id INT PRIMARY KEY,
    actual_date DATE UNIQUE,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    year_num INT,
    quarter_num INT
);

-- Измерение: Клиент
CREATE TABLE IF NOT EXISTS dwh.dim_client (
    client_sk SERIAL PRIMARY KEY,    -- Суррогатный ключ
    client_id_nk INT UNIQUE,         -- Натуральный ключ из OLTP
    company_name VARCHAR(255),
    industry VARCHAR(100),
    region VARCHAR(100),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN
);

-- Измерение: Тип транзакции
CREATE TABLE IF NOT EXISTS dwh.dim_transaction_type (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) -- INCOME / OUTCOME
);

-- Факт: Транзакции (Центр таблицы Звезды)
CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    fact_id BIGSERIAL PRIMARY KEY,
    transaction_id_nk BIGINT UNIQUE, -- ID из источника
    client_sk INT REFERENCES dwh.dim_client(client_sk),
    date_id INT REFERENCES dwh.dim_date(date_id),
    type_id INT REFERENCES dwh.dim_transaction_type(type_id),
    amount_rub DECIMAL(18, 2),       -- Сконвертированная сумма
    description TEXT,                -- Текст для NLP
    etl_processed_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);