
-- ИНИЦИАЛИЗАЦИЯ БД И СХЕМЫ

-- Создаем схемы
DROP SCHEMA IF EXISTS oltp CASCADE;
DROP SCHEMA IF EXISTS dwh CASCADE;
DROP SCHEMA IF EXISTS dm_risk CASCADE;

CREATE SCHEMA IF NOT EXISTS oltp;       -- Операционный слой (как у Java-бэкенда)
CREATE SCHEMA IF NOT EXISTS dwh;        -- Слой хранилища (Integration layer)
CREATE SCHEMA IF NOT EXISTS dm_risk;    -- Слой витрин (Data Marts)


-- =============================================
-- СЛОЙ OLTP (Source System)
-- Имитация реальной банковской системы
-- =============================================

-- Таблица клиентов
CREATE TABLE IF NOT EXISTS oltp.clients (
    client_id SERIAL PRIMARY KEY,
    inn VARCHAR(12) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    registration_date DATE NOT NULL,
    region VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- Таблица счетов
CREATE TABLE IF NOT EXISTS oltp.accounts (
    account_id SERIAL PRIMARY KEY,
    client_id INT REFERENCES oltp.clients(client_id),
    account_number VARCHAR(20) UNIQUE NOT NULL,
    currency CHAR(3) DEFAULT 'RUB',
    open_date DATE NOT NULL,
    current_balance DECIMAL(18, 2) DEFAULT 0.00
);

-- Таблица транзакций
CREATE TABLE IF NOT EXISTS oltp.transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    account_id INT REFERENCES oltp.accounts(account_id),
    transaction_date TIMESTAMP NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    currency CHAR(3) DEFAULT 'RUB',
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('INCOME', 'OUTCOME')),
    description TEXT,                -- Назначение платежа
    counterparty_inn VARCHAR(12)     -- ИНН контрагента
);

-- Таблица заявок на кредит
CREATE TABLE IF NOT EXISTS oltp.credit_applications (
    application_id SERIAL PRIMARY KEY,
    client_id INT REFERENCES oltp.clients(client_id),
    request_date TIMESTAMP NOT NULL,
    requested_amount DECIMAL(18, 2),
    requested_term_months INT,
    status VARCHAR(20) CHECK (status IN ('APPROVED', 'REJECTED', 'IN_REVIEW')),
    manager_comment TEXT
);

-- Индексы для производительности
CREATE INDEX idx_trans_date ON oltp.transactions(transaction_date);
CREATE INDEX idx_trans_account ON oltp.transactions(account_id);


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
    client_id_nk INT,                -- Натуральный ключ из OLTP
    company_name VARCHAR(255),
    industry VARCHAR(100),
    region VARCHAR(100),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение: Тип транзакции
CREATE TABLE IF NOT EXISTS dwh.dim_transaction_type (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) -- INCOME / OUTCOME
);

-- Факт: Транзакции (Центр таблицы Звезды)
CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    fact_id BIGSERIAL PRIMARY KEY,
    transaction_id_nk BIGINT,        -- ID из источника
    client_sk INT REFERENCES dwh.dim_client(client_sk),
    date_id INT REFERENCES dwh.dim_date(date_id),
    type_id INT REFERENCES dwh.dim_transaction_type(type_id),
    amount_rub DECIMAL(18, 2),       -- Сконвертированная сумма
    description TEXT,                -- Текст для NLP
    etl_processed_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- СЛОЙ DATA MARTS (Витрины)
-- Будем создавать их через VIEW позже
-- =============================================


















-- Факт: Транзакции (Центр таблицы Звезды)
CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    fact_id BIGSERIAL PRIMARY KEY,
    transaction_id_nk BIGINT,        -- ID из источника
    client_sk INT REFERENCES dwh.dim_client(client_sk),
    date_id INT REFERENCES dwh.dim_date(date_id),
    type_id INT REFERENCES dwh.dim_transaction_type(type_id),
    amount_rub DECIMAL(18, 2),       -- Сконвертированная сумма
    description TEXT,                -- Текст для NLP
    etl_processed_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);