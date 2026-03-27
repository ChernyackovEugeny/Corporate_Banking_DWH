import pandas as pd
from datetime import datetime
from sqlalchemy import text

from src.utils.get_db_connection import get_connection

def etl_dim_clients(conn):
    """
    ETL для измерения Клиенты (dim_client).
    Забираем ВСЕХ клиентов, сохраняем статус активности.
    """
    print("ETL: Начинаем загрузку измерения клиентов (dim_client)...")

    # 1. Extract: Забираем всех клиентов, включая неактивных
    # Это важно для сохранения исторических данных
    query_oltp = """
        SELECT client_id, company_name, industry, region, is_active
        FROM oltp.clients;
    """
    df_clients = pd.read_sql(text(query_oltp), conn)

    # 2. Transform
    df_clients['load_date'] = datetime.now()

    data_to_insert = df_clients.to_dict(orient='records')

    # 3. Load: Upsert
    # Теперь мы обновляем и статус активности
    insert_query = text("""
        INSERT INTO dwh.dim_client (client_id_nk, company_name, industry, region, is_active, load_date)
        VALUES (:client_id, :company_name, :industry, :region, :is_active, :load_date)
        ON CONFLICT (client_id_nk) DO UPDATE
        SET company_name = EXCLUDED.company_name,
            region = EXCLUDED.region,
            is_active = EXCLUDED.is_active;
    """)

    conn.execute(insert_query, data_to_insert)
    conn.commit()
    print(f"ETL: Загружено/обновлено {len(data_to_insert)} клиентов.")

def etl_fact_transactions(conn):
    """
    ETL для таблицы фактов Транзакции.
    Замена натуральных ключей на суррогатные.
    """

    print("ETL: Начинаем загрузку фактов транзакций...")

    # Чтобы ускорить процесс, выгрузим маппинги ключей
    # 1. Маппинг client_id -> client_sk
    df_map_client = pd.read_sql(text("SELECT client_sk, client_id_nk FROM dwh.dim_client"), conn)
    client_map = dict(zip(df_map_client.client_id_nk, df_map_client.client_sk))

    # 2. Маппинг transaction_type -> type_id
    df_map_type = pd.read_sql(text("SELECT type_id, type_name FROM dwh.dim_transaction_type"), conn)
    type_map = dict(zip(df_map_type.type_name, df_map_type.type_id))

    # 3. Маппинг date -> date_id (для простоты считаем, что dim_date уже заполнен при генерации)
    df_map_date = pd.read_sql(text("SELECT date_id, actual_date FROM dwh.dim_date"), conn)
    # Создаем ключ в формате YYYYMMDD для быстрого поиска
    df_map_date['date_key'] = pd.to_datetime(df_map_date['actual_date']).dt.strftime('%Y%m%d').astype(int)
    date_map = dict(zip(df_map_date.date_key, df_map_date.date_id))
    print(f"  dim_date содержит {len(date_map)} записей. Пример: {list(date_map.items())[:3]}")

    # Извлекаем транзакции из OLTP
    # JOIN с accounts, чтобы получить client_id
    query_oltp = """
        SELECT
            t.transaction_id,
            a.client_id,
            t.transaction_date,
            t.transaction_type,
            t.amount,
            t.description
        FROM oltp.transactions t
        JOIN oltp.accounts a ON t.account_id = a.account_id;
    """

    df_trans = pd.read_sql(text(query_oltp), conn)
    print(f"  Извлечено {len(df_trans)} транзакций из OLTP.")

    # TRANSFORM

    # Генерируем ключ даты (YYYYMMDD)
    df_trans['date_key'] = pd.to_datetime(df_trans['transaction_date']).dt.strftime('%Y%m%d').astype(int)

    # Заменяем ключи (Merge/Mappings)
    df_trans['client_sk'] = df_trans['client_id'].map(client_map)
    df_trans['type_id'] = df_trans['transaction_type'].map(type_map)
    df_trans['date_id'] = df_trans['date_key'].map(date_map)

    # Диагностика маппингов перед фильтрацией
    print(f"  Пример date_key из транзакций: {df_trans['date_key'].head(3).tolist()}")
    print(f"  Маппинг client_sk: {df_trans['client_sk'].notna().sum()} / {len(df_trans)} совпадений")
    print(f"  Маппинг type_id:   {df_trans['type_id'].notna().sum()} / {len(df_trans)} совпадений")
    print(f"  Маппинг date_id:   {df_trans['date_id'].notna().sum()} / {len(df_trans)} совпадений")

    # Если вдруг нет клиента в справочнике
    df_trans = df_trans.dropna(subset=['client_sk', 'date_id', 'type_id'])

    # Подготовка данных к загрузке (векторизованно, без iterrows)
    df_ready = pd.DataFrame({
        'transaction_id_nk': df_trans['transaction_id'],  # NK
        'client_sk':         df_trans['client_sk'].astype(int),
        'date_id':           df_trans['date_id'].astype(int),
        'type_id':           df_trans['type_id'].astype(int),
        'amount_rub':        df_trans['amount'],
        'description':       df_trans['description'],
        'etl_processed_dt':  datetime.now()
    })
    data_to_insert = df_ready.to_dict(orient='records')

    # --- LOAD ---
    insert_query = text("""
        INSERT INTO dwh.fact_transactions
        (transaction_id_nk, client_sk, date_id, type_id, amount_rub, description, etl_processed_dt)
        VALUES (:transaction_id_nk, :client_sk, :date_id, :type_id, :amount_rub, :description, :etl_processed_dt)
        ON CONFLICT (transaction_id_nk) DO NOTHING; -- Пропускаем дубликаты
    """)

    print("  Начинаем загрузку в DWH...")
    # Батчевая загрузка — SQLAlchemy executemany нестабилен на очень больших списках
    BATCH_SIZE = 10000
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        conn.execute(insert_query, data_to_insert[i:i + BATCH_SIZE])
    conn.commit()

    print(f"ETL: Загрузка фактов завершена. Обработано {len(data_to_insert)} строк.")

def main():
    conn = get_connection()
    try:
        etl_dim_clients(conn)
        etl_fact_transactions(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
