import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
import random
import pandas as pd
from datetime import datetime, timedelta

import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL')

fake = Faker('ru_RU')  # Русские фейковые данные

def get_connection():
    return psycopg2.connect(db_url)

def generate_dates(start_date, end_date):
    """Генератор дат для.dim_date"""
    dates = []
    current = start_date
    while current <= end_date:
        date_id = int(current.strftime('%Y%m%d'))
        dates.append((
            date_id,
            current,
            current.strftime('%A'),
            current.strftime('%B'),
            current.year,
            (current.month - 1) // 3 + 1
        ))
        current += timedelta(days=1)
    return dates

def main():
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Начинаем генерацию данных...")

    # 1. Заполняем dwh.dim_date (обычно делается один раз)
    print("1. Генерация календаря (dim_date)...")
    dates = generate_dates(datetime(2023, 1, 1), datetime(2024, 5, 31))
    execute_batch(cursor, "INSERT INTO dwh.dim_date VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", dates)

    # 2. Заполняем dwh.dim_transaction_type
    print("2. Заполнение типов транзакций...")
    cursor.execute("INSERT INTO dwh.dim_transaction_type (type_name) VALUES ('INCOME'), ('OUTCOME') ON CONFLICT DO NOTHING")

    # 3. Генерируем клиентов OLTP
    print("3. Генерация клиентов...")
    industries = ['IT', 'Retail', 'Agriculture', 'Construction', 'Finance', 'Logistics']

    clients_data = []
    for _ in range(100): # 100 клиентов
        inn = str(random.randint(1000000000, 9999999999))
        clients_data.append((
            inn,
            fake.company(),
            random.choice(industries),
            fake.date_between(start_date='-5y', end_date='-1y'),
            fake.city(),
            True
        ))

    execute_batch(cursor, 
        "INSERT INTO oltp.clients (inn, company_name, industry, registration_date, region, is_active) VALUES (%s, %s, %s, %s, %s, %s)", 
        clients_data
    )

    # 4. Генерируем счета OLTP
    print("4. Генерация счетов...")
    cursor.execute("SELECT client_id FROM oltp.clients")
    client_ids = [row[0] for row in cursor.fetchall()]

    accounts_data = []
    acc_counter = 40000000 # Пример номера счета
    for cid in client_ids:
        # У каждого клиента от 1 до 3 счетов
        for _ in range(random.randint(1, 3)):
            accounts_data.append((
                cid,
                f"40702{acc_counter}",
                'RUB',
                fake.date_between(start_date='-3y', end_date='today'),
                random.uniform(10000, 5000000)
            ))
            acc_counter += 1
            
    execute_batch(cursor,
        "INSERT INTO oltp.accounts (client_id, account_number, currency, open_date, current_balance) VALUES (%s, %s, %s, %s, %s)",
        accounts_data
    )

    # 5. Генерируем Транзакции OLTP
    print("5. Генерация транзакций (может занять время)...")
    cursor.execute("SELECT account_id FROM oltp.accounts")
    account_ids = [row[0] for row in cursor.fetchall()]

    # Шаблоны текстов для NLP
    income_desc = [
        "Оплата по договору №{}", "Поступление средств от контрагента", "Зачисление выручки", 
        "Оплата за услуги связи", "Возврат средств"
    ]
    outcome_desc = [
        "Оплата налогов", "Зарплата сотрудникам", "Оплата аренды", "Покупка оборудования",
        "Перевод средств контрагенту", "Хозрасходы", "Оплата консалтинговых услуг"
    ]

    transactions_data = []
    batch_size = 50000
    total_tx = 200000 # Генерируем 200к транзакций для аналитики

    for i in range(total_tx):
        acc_id = random.choice(account_ids)
        tx_type = random.choice(['INCOME', 'OUTCOME'])
        amount = round(random.uniform(1000, 500000), 2)
        
        if tx_type == 'INCOME':
            desc = random.choice(income_desc).format(random.randint(100, 999))
        else:
            desc = random.choice(outcome_desc)
            
        tx_date = fake.date_time_between(start_date='-6m', end_date='now')
        
        # Создаем аномалию (для DS задачи)
        # С вероятностью 1% сделаем очень крупную транзакцию
        if random.random() < 0.01:
            amount = round(random.uniform(5000000, 10000000), 2)
            desc = "Срочный перевод по договору цессии" # Подозрительный текст
        
        transactions_data.append((
            acc_id,
            tx_date,
            amount,
            'RUB',
            tx_type,
            desc,
            str(random.randint(1000000000, 9999999999))
        ))

        if len(transactions_data) >= batch_size:
            execute_batch(cursor,
                "INSERT INTO oltp.transactions (account_id, transaction_date, amount, currency, transaction_type, description, counterparty_inn) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                transactions_data
            )
            print(f"   Загружено {i+1}/{total_tx} транзакций...")
            transactions_data = []
    
    # Финальная загрузка
    if transactions_data:
        execute_batch(cursor,
            "INSERT INTO oltp.transactions (account_id, transaction_date, amount, currency, transaction_type, description, counterparty_inn) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            transactions_data
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("Генерация данных завершена успешно!")

if __name__ == "__main__":
    main()