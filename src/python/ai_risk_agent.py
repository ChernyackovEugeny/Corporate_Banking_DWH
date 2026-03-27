import os
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

from src.utils.get_db_connection import get_connection

load_dotenv()


def load_mart_data(conn):
    """Загружаем данные из витрины в Pandas DataFrame"""
    query = "SELECT * FROM dm_risk.client_financial_profile"
    print("Загрузка данных из витрины...")
    df = pd.read_sql(query, conn)
    print(f"Загружено {len(df)} строк.")
    return df

def preprocess_data(df):
    """
    Предобработка данных для ML модели.
    """
    # Обработка пропусков
    # Заполняем нулями, где не было транзакций
    df['last_month_income'] = df['last_month_income'].fillna(0)
    df['avg_income_3m'] = df['avg_income_3m'].fillna(0)
    df['transaction_keywords_sample'] = df['transaction_keywords_sample'].fillna('Нет данных')

    # создание признаков
    # Переводим категориальный признак тренда в числовой
    trend_map = {
        'GROWTH': 1,
        'STABLE': 0,
        'DECLINE': -1,
        'NEW': -2 # Новички - это риск
    }
    df['trend_score'] = df['income_trend_label'].map(trend_map)

    # Создание таргета
    # считаем клиента проблемным, если у него спад дохода И аномалии
    df['is_risky_client'] = (df['has_anomaly_tx'] == 1) & (df['trend_score'] < 0)

    return df

def simple_analytics(df):
    """Простая аналитика и вывод статистики"""
    print("\n--- Аналитика по портфелю ---")

    # Группировка по отраслям (Vacancy: 'Проверка гипотез')
    industry_stats = df.groupby('industry').agg({
        'total_income_all_time': 'sum',
        'client_id_nk': 'count',
        'is_risky_client': 'sum'
    }).rename(columns={
        'client_id_nk': 'client_count',
        'is_risky_client': 'risky_count'
    })

    print("Статистика по отраслям:")
    print(industry_stats.sort_values('total_income_all_time', ascending=False))

def analyze_risk(row):
    """Отправляем промпт в DeepSeek и получаем оценку риска."""
    client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com"
    )

    prompt = f"""
        Ты — старший риск-аналитик крупного банка.
        Твоя задача: проанализировать профиля клиентов и выявить признаки отмывания денег (AML).

        Данные клиента:
        - Компания: {row['company_name']}
        - Отрасль: {row['industry']}
        - Статус: {row['is_active']}
        - Тренд доходов: {row['income_trend_label']}
        - Флаг аномалий: {row['has_anomaly_tx']} (1 - есть аномалия)
        - Примеры назначений платежей: {row['transaction_keywords_sample']}

        Вопрос: Стоит ли запросить у клиента дополнительные документы? Дай краткий ответ (Да/Нет) и причину.
        """

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "Ты риск-аналитик корпоративного банка. Отвечай кратко и по делу."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3  # Низкая температура для стабильных аналитических ответов
    )

    return response.choices[0].message.content

def generate_llm_prompts(df):
    """
    Генерация промптов для AI-агента и получение реальных ответов от LLM.

    Мы моделируем ситуацию, когда AI-агент должен проанализировать текст транзакций
    и выдать рекомендацию.
    """
    print("\n--- Генерация промптов для LLM (Агент Аналитик) ---")

    # Выбираем только подозрительных клиентов для демонстрации
    risky_clients = df[df['has_anomaly_tx'] == 1].head(3)

    prompts = []
    for index, row in risky_clients.iterrows():
        print(f"\n[Client ID {row['client_id_nk']}] Отправка запроса в DeepSeek...")
        result = analyze_risk(row)
        prompts.append(result)
        print(f"[Client ID {row['client_id_nk']}] Ответ получен.")

    return prompts

def main():
    conn = get_connection()

    # 1. Load
    df = load_mart_data(conn)

    if df.empty:
        print("Витрина пуста. Проверьте ETL.")
        return

    # 2. Preprocessing
    df = preprocess_data(df)

    # 3. Analytics
    simple_analytics(df)

    # 4. Prompt Engineering + реальный LLM запрос
    prompts = generate_llm_prompts(df)

    # Выведем один пример промпта для демонстрации
    if prompts:
        print("\n--- Пример ответа LLM ---")
        print(prompts[0])

    conn.close()

if __name__ == "__main__":
    main()
