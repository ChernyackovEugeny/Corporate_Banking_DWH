from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import text

from src.utils.get_db_connection import get_connection

fake = Faker('ru_RU')  # Русские фейковые данные

def generate_dates(start_date, end_date):
    """Генератор дат для dim_date"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append({
            'date_id':      int(current.strftime('%Y%m%d')),
            'actual_date':  current.date(),
            'day_name':     current.strftime('%A'),
            'month_name':   current.strftime('%B'),
            'year_num':     current.year,
            'quarter_num':  (current.month - 1) // 3 + 1
        })
        current += timedelta(days=1)
    return dates

def main():
    conn = get_connection()

    print("Начинаем генерацию данных...")

    # 1. Заполняем dwh.dim_date (обычно делается один раз)
    print("1. Генерация календаря (dim_date)...")
    dates = generate_dates(datetime(2020, 1, 1), datetime(2030, 12, 31))
    conn.execute(
        text("INSERT INTO dwh.dim_date VALUES (:date_id, :actual_date, :day_name, :month_name, :year_num, :quarter_num) ON CONFLICT DO NOTHING"),
        dates
    )

    # 2. Заполняем dwh.dim_transaction_type
    print("2. Заполнение типов транзакций...")
    conn.execute(text("INSERT INTO dwh.dim_transaction_type (type_name) VALUES ('INCOME'), ('OUTCOME') ON CONFLICT DO NOTHING"))

    # 3. Генерируем клиентов OLTP
    print("3. Генерация клиентов...")
    industries = ['IT', 'Retail', 'Agriculture', 'Construction', 'Finance', 'Logistics']

    clients_data = []
    for _ in range(100):  # 100 клиентов
        clients_data.append({
            'inn':               str(random.randint(1000000000, 9999999999)),
            'company_name':      fake.company(),
            'industry':          random.choice(industries),
            'registration_date': fake.date_between(start_date='-5y', end_date='-1y'),
            'region':            fake.city(),
            'is_active':         True
        })

    conn.execute(
        text("INSERT INTO oltp.clients (inn, company_name, industry, registration_date, region, is_active) VALUES (:inn, :company_name, :industry, :registration_date, :region, :is_active)"),
        clients_data
    )

    # 4. Генерируем счета OLTP
    print("4. Генерация счетов...")
    client_ids = [row[0] for row in conn.execute(text("SELECT client_id FROM oltp.clients")).fetchall()]

    accounts_data = []
    acc_counter = 40000000  # Пример номера счета
    for cid in client_ids:
        # У каждого клиента от 1 до 3 счетов
        for _ in range(random.randint(1, 3)):
            accounts_data.append({
                'client_id':       cid,
                'account_number':  f"40702{acc_counter}",
                'currency':        'RUB',
                'open_date':       fake.date_between(start_date='-3y', end_date='today'),
                'current_balance': round(random.uniform(10000, 5000000), 2)
            })
            acc_counter += 1

    conn.execute(
        text("INSERT INTO oltp.accounts (client_id, account_number, currency, open_date, current_balance) VALUES (:client_id, :account_number, :currency, :open_date, :current_balance)"),
        accounts_data
    )

    # 5. Генерируем транзакции OLTP
    print("5. Генерация транзакций (может занять время)...")
    account_ids = [row[0] for row in conn.execute(text("SELECT account_id FROM oltp.accounts")).fetchall()]

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
    total_tx = 200000  # Генерируем 200к транзакций для аналитики

    for i in range(total_tx):
        tx_type = random.choice(['INCOME', 'OUTCOME'])
        amount  = round(random.uniform(1000, 500000), 2)

        if tx_type == 'INCOME':
            desc = random.choice(income_desc).format(random.randint(100, 999))
        else:
            desc = random.choice(outcome_desc)

        tx_date = fake.date_time_between(start_date='-6m', end_date='now')

        # Создаем аномалию (для DS задачи)
        # С вероятностью 1% сделаем очень крупную транзакцию
        if random.random() < 0.01:
            amount = round(random.uniform(5000000, 10000000), 2)
            desc   = "Срочный перевод по договору цессии"  # Подозрительный текст

        transactions_data.append({
            'account_id':       random.choice(account_ids),
            'transaction_date': tx_date,
            'amount':           amount,
            'currency':         'RUB',
            'transaction_type': tx_type,
            'description':      desc,
            'counterparty_inn': str(random.randint(1000000000, 9999999999))
        })

        if len(transactions_data) >= batch_size:
            conn.execute(
                text("INSERT INTO oltp.transactions (account_id, transaction_date, amount, currency, transaction_type, description, counterparty_inn) VALUES (:account_id, :transaction_date, :amount, :currency, :transaction_type, :description, :counterparty_inn)"),
                transactions_data
            )
            print(f"   Загружено {i+1}/{total_tx} транзакций...")
            transactions_data = []

    # Финальная загрузка
    if transactions_data:
        conn.execute(
            text("INSERT INTO oltp.transactions (account_id, transaction_date, amount, currency, transaction_type, description, counterparty_inn) VALUES (:account_id, :transaction_date, :amount, :currency, :transaction_type, :description, :counterparty_inn)"),
            transactions_data
        )

    conn.commit()
    conn.close()
    print("Генерация данных завершена успешно!")

if __name__ == "__main__":
    main()
