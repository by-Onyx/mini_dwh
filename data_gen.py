import json
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Any
import psycopg2
from psycopg2 import sql, extras
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()


class DataGenerator:
    def __init__(self, conn_params: Dict[str, str] = None):
        # Параметры подключения к PostgreSQL
        self.conn_params = conn_params or {
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': os.getenv('PG_PORT', '5432'),
            'database': os.getenv('PG_DATABASE', 'data_warehouse'),
            'user': os.getenv('PG_USER', 'data_user'),
            'password': os.getenv('PG_PASSWORD', 'data_pass')
        }

        self.conn = None
        self.cursor = None

        # Хранилища данных для связности
        self.users_data = []
        self.accounts_data = []
        self.payment_cards_data = []
        self.card_fingerprints_data = []
        self.transactions_data = []
        self.transaction_logs_data = []
        self.transaction_retry_data = []

        # Константы для генерации
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY']
        self.card_types = ['Visa', 'MasterCard', 'American Express', 'Discover', 'UnionPay']
        self.statuses = ['pending', 'success', 'failed', 'cancelled']
        self.transaction_types = ['authorize', 'capture', 'refund', 'void']
        self.payment_methods = ['card', 'cash']
        self.log_steps = ['authorization', 'validation', 'processing', 'settlement', 'notification']
        self.retry_statuses = ['pending', 'processing', 'completed', 'failed']

        # Имена для генерации
        self.first_names = ['Иван', 'Петр', 'Сергей', 'Анна', 'Мария', 'Елена', 'Алексей', 'Дмитрий', 'Ольга',
                            'Татьяна']
        self.last_names = ['Иванов', 'Петров', 'Сидоров', 'Смирнов', 'Кузнецов', 'Попова', 'Соколова', 'Лебедева']
        self.domains = ['gmail.com', 'yandex.ru', 'mail.ru', 'example.com', 'company.com']

    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            print("✓ Подключение к PostgreSQL установлено")
        except Exception as e:
            print(f"✗ Ошибка подключения: {e}")
            raise

    def disconnect(self):
        """Закрытие соединения"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Соединение закрыто")

    def truncate_tables(self):
        """Очистка таблиц перед вставкой (опционально)"""
        tables = [
            'transaction_retry_queue',
            'transaction_pipeline_log',
            'card_fingerprints',
            'transactions',
            'payment_cards',
            'accounts',
            'users'
        ]

        try:
            for table in tables:
                self.cursor.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table)))
            self.conn.commit()
            print("✓ Таблицы очищены")
        except Exception as e:
            print(f"⚠ Предупреждение при очистке: {e}")
            self.conn.rollback()

    def random_date(self, start_date: datetime, end_date: datetime) -> datetime:
        """Генерация случайной даты"""
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randrange(days_between)
        random_seconds = random.randrange(86400)
        return start_date + timedelta(days=random_days, seconds=random_seconds)

    def random_email(self, user_id: int) -> str:
        """Генерация email"""
        name = random.choice(self.first_names).lower()
        domain = random.choice(self.domains)
        return f"{name}.{user_id}@{domain}"

    def random_full_name(self) -> str:
        """Генерация полного имени"""
        return f"{random.choice(self.first_names)} {random.choice(self.last_names)}"

    def generate_users(self, count: int = 100) -> List[Dict]:
        """Генерация пользователей"""
        users = []
        for i in range(1, count + 1):
            user = {
                "user_id": i,
                "email": self.random_email(i),
                "full_name": self.random_full_name(),
                "created_at": self.random_date(
                    datetime(2020, 1, 1),
                    datetime(2025, 12, 31)
                ),
                "is_blocked": random.random() < 0.05
            }
            users.append(user)
        self.users_data = users
        return users

    def generate_accounts(self, count: int = 100) -> List[Dict]:
        """Генерация счетов"""
        accounts = []
        account_id = 1
        for user_id in range(1, count + 1):
            num_accounts = random.randint(1, 3)
            for _ in range(num_accounts):
                account = {
                    "account_id": account_id,
                    "user_id": user_id,
                    "currency": random.choice(self.currencies),
                    "balance": round(random.uniform(-1000, 50000), 2),
                    "version": random.randint(1, 10),
                    "updated_at": self.random_date(
                        datetime(2020, 1, 1),
                        datetime(2025, 12, 31)
                    )
                }
                accounts.append(account)
                account_id += 1
        self.accounts_data = accounts
        return accounts

    def generate_payment_cards(self, count: int = 100) -> List[Dict]:
        """Генерация платежных карт"""
        cards = []
        card_id = 1
        for user_id in range(1, count + 1):
            num_cards = random.randint(0, 2)
            for _ in range(num_cards):
                card = {
                    "card_id": card_id,
                    "user_id": user_id,
                    "card_last4": f"{random.randint(1000, 9999)}",
                    "card_type": random.choice(self.card_types),
                    "expiry_month": random.randint(1, 12),
                    "expiry_year": random.randint(2025, 2030),
                    "created_at": self.random_date(
                        datetime(2020, 1, 1),
                        datetime(2025, 12, 31)
                    )
                }
                cards.append(card)
                card_id += 1
        self.payment_cards_data = cards
        return cards

    def generate_card_fingerprints(self) -> List[Dict]:
        """Генерация fingerprint'ов карт"""
        fingerprints = []
        for card in self.payment_cards_data:
            fingerprint = {
                "fingerprint_id": len(fingerprints) + 1,
                "card_id": card["card_id"],
                "device_fp": f"fp_sha256_{''.join(random.choices(string.hexdigits.lower(), k=32))}",
                "risk_score": round(random.uniform(0, 100), 2)
            }
            fingerprints.append(fingerprint)
        self.card_fingerprints_data = fingerprints
        return fingerprints

    def generate_transactions(self, count: int = 100) -> List[Dict]:
        """Генерация транзакций"""
        transactions = []

        user_ids = [user["user_id"] for user in self.users_data]
        account_ids = [acc["account_id"] for acc in self.accounts_data]
        card_ids = [card["card_id"] for card in self.payment_cards_data] if self.payment_cards_data else [None]

        for i in range(1, count + 1):
            payment_method = random.choice(self.payment_methods)
            status = random.choices(
                self.statuses,
                weights=[0.7, 0.2, 0.05, 0.05],
                k=1
            )[0]
            transaction_type = random.choices(
                self.transaction_types,
                weights=[0.5, 0.3, 0.15, 0.05],
                k=1
            )[0]

            created_at = self.random_date(
                datetime(2024, 1, 1),
                datetime(2026, 4, 11)
            )

            processed_at = created_at + timedelta(seconds=random.randint(1, 300)) if status == 'success' else None
            updated_at = processed_at if processed_at else created_at

            transaction = {
                "transaction_id": i,
                "user_id": random.choice(user_ids),
                "account_id": random.choice(account_ids),
                "amount": round(random.uniform(0.01, 10000), 2),
                "currency": random.choice(self.currencies),
                "payment_method": payment_method,
                "card_id": random.choice(card_ids) if payment_method == 'card' and card_ids else None,
                "cash_terminal_id": f"TERM{random.randint(1000, 9999)}" if payment_method == 'cash' else None,
                "status": status,
                "type": transaction_type,
                "retry_count": random.randint(0, 3) if status == 'failed' else 0,
                "last_error": random.choice([
                    "Insufficient funds",
                    "Card declined",
                    "Network timeout",
                    "Invalid amount",
                    None
                ]) if status == 'failed' else None,
                "external_txn_id": f"txn_{''.join(random.choices(string.ascii_lowercase + string.digits, k=16))}",
                "created_at": created_at,
                "processed_at": processed_at,
                "updated_at": updated_at
            }
            transactions.append(transaction)

        self.transactions_data = transactions
        return transactions

    def generate_transaction_logs(self) -> List[Dict]:
        """Генерация логов транзакций"""
        logs = []
        log_id = 1

        for transaction in self.transactions_data:
            num_steps = random.randint(1, 5)
            for step_num in range(num_steps):
                log = {
                    "log_id": log_id,
                    "transaction_id": transaction["transaction_id"],
                    "step_name": random.choice(self.log_steps),
                    "status": random.choice(['completed', 'failed', 'pending']),
                    "payload": json.dumps({
                        "request_id": f"req_{''.join(random.choices(string.ascii_lowercase + string.digits, k=10))}",
                        "gateway_response": random.choice(['approved', 'declined', 'error']),
                        "processing_time_ms": random.randint(10, 5000),
                        "attempt": step_num + 1
                    }),
                    "created_at": self.random_date(
                        datetime(2024, 1, 1),
                        datetime(2026, 4, 11)
                    )
                }
                logs.append(log)
                log_id += 1

        self.transaction_logs_data = logs
        return logs

    def generate_transaction_retry_queue(self) -> List[Dict]:
        """Генерация очереди повторных попыток"""
        retry_queue = []
        retry_id = 1

        failed_transactions = [t for t in self.transactions_data if t['status'] == 'failed']

        for transaction in failed_transactions:
            retry = {
                "retry_id": retry_id,
                "transaction_id": transaction["transaction_id"],
                "scheduled_at": self.random_date(
                    datetime(2024, 1, 1),
                    datetime(2026, 4, 11)
                ),
                "attempt_number": transaction['retry_count'] + 1,
                "status": random.choice(self.retry_statuses)
            }
            retry_queue.append(retry)
            retry_id += 1

        self.transaction_retry_data = retry_queue
        return retry_queue

    def insert_users(self):
        """Вставка пользователей"""
        if not self.users_data:
            return

        insert_query = """
            INSERT INTO users (user_id, email, full_name, created_at, is_blocked)
            VALUES %s
            ON CONFLICT (user_id) DO NOTHING
        """

        data = [(u["user_id"], u["email"], u["full_name"], u["created_at"], u["is_blocked"])
                for u in self.users_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено пользователей: {len(data)}")

    def insert_accounts(self):
        """Вставка счетов"""
        if not self.accounts_data:
            return

        insert_query = """
            INSERT INTO accounts (account_id, user_id, currency, balance, version, updated_at)
            VALUES %s
            ON CONFLICT (account_id) DO NOTHING
        """

        data = [(a["account_id"], a["user_id"], a["currency"], a["balance"],
                 a["version"], a["updated_at"]) for a in self.accounts_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено счетов: {len(data)}")

    def insert_payment_cards(self):
        """Вставка платежных карт"""
        if not self.payment_cards_data:
            return

        insert_query = """
            INSERT INTO payment_cards (card_id, user_id, card_last4, card_type, expiry_month, expiry_year, created_at)
            VALUES %s
            ON CONFLICT (card_id) DO NOTHING
        """

        data = [(c["card_id"], c["user_id"], c["card_last4"], c["card_type"],
                 c["expiry_month"], c["expiry_year"], c["created_at"]) for c in self.payment_cards_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено карт: {len(data)}")

    def insert_card_fingerprints(self):
        """Вставка fingerprint'ов"""
        if not self.card_fingerprints_data:
            return

        insert_query = """
            INSERT INTO card_fingerprints (fingerprint_id, card_id, device_fp, risk_score)
            VALUES %s
            ON CONFLICT (fingerprint_id) DO NOTHING
        """

        data = [(f["fingerprint_id"], f["card_id"], f["device_fp"], f["risk_score"])
                for f in self.card_fingerprints_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено fingerprint'ов: {len(data)}")

    def insert_transactions(self):
        """Вставка транзакций"""
        if not self.transactions_data:
            return

        insert_query = """
            INSERT INTO transactions (
                transaction_id, user_id, account_id, amount, currency, 
                payment_method, card_id, cash_terminal_id, status, type,
                retry_count, last_error, external_txn_id, created_at, 
                processed_at, updated_at
            )
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
        """

        data = [(
            t["transaction_id"], t["user_id"], t["account_id"], t["amount"], t["currency"],
            t["payment_method"], t["card_id"], t["cash_terminal_id"], t["status"], t["type"],
            t["retry_count"], t["last_error"], t["external_txn_id"], t["created_at"],
            t["processed_at"], t["updated_at"]
        ) for t in self.transactions_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено транзакций: {len(data)}")

    def insert_transaction_logs(self):
        """Вставка логов транзакций"""
        if not self.transaction_logs_data:
            return

        insert_query = """
            INSERT INTO transaction_pipeline_log (log_id, transaction_id, step_name, status, payload, created_at)
            VALUES %s
            ON CONFLICT (log_id) DO NOTHING
        """

        data = [(l["log_id"], l["transaction_id"], l["step_name"], l["status"],
                 l["payload"], l["created_at"]) for l in self.transaction_logs_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено записей в лог: {len(data)}")

    def insert_transaction_retry_queue(self):
        """Вставка очереди повторных попыток"""
        if not self.transaction_retry_data:
            return

        insert_query = """
            INSERT INTO transaction_retry_queue (retry_id, transaction_id, scheduled_at, attempt_number, status)
            VALUES %s
            ON CONFLICT (retry_id) DO NOTHING
        """

        data = [(r["retry_id"], r["transaction_id"], r["scheduled_at"],
                 r["attempt_number"], r["status"]) for r in self.transaction_retry_data]

        extras.execute_values(self.cursor, insert_query, data, page_size=100)
        self.conn.commit()
        print(f"✓ Вставлено записей в очередь повторных попыток: {len(data)}")

    def generate_and_insert_all(self, count: int = 100, truncate: bool = False):
        """Полный цикл генерации и вставки"""
        try:
            self.connect()

            if truncate:
                self.truncate_tables()

            print(f"\n=== ГЕНЕРАЦИЯ ДАННЫХ ({count} записей) ===\n")

            # Генерация данных в правильном порядке
            self.generate_users(count)
            print(f"✓ Сгенерировано пользователей: {len(self.users_data)}")

            self.generate_accounts(count)
            print(f"✓ Сгенерировано счетов: {len(self.accounts_data)}")

            self.generate_payment_cards(count)
            print(f"✓ Сгенерировано карт: {len(self.payment_cards_data)}")

            self.generate_card_fingerprints()
            print(f"✓ Сгенерировано fingerprint'ов: {len(self.card_fingerprints_data)}")

            self.generate_transactions(1000000)
            print(f"✓ Сгенерировано транзакций: {len(self.transactions_data)}")

            self.generate_transaction_logs()
            print(f"✓ Сгенерировано логов: {len(self.transaction_logs_data)}")

            self.generate_transaction_retry_queue()
            print(f"✓ Сгенерировано записей в очереди повторных попыток: {len(self.transaction_retry_data)}")

            print("\n=== ВСТАВКА В POSTGRESQL ===\n")

            # Вставка в правильном порядке
            self.insert_users()
            self.insert_accounts()
            self.insert_payment_cards()
            self.insert_card_fingerprints()
            self.insert_transactions()
            self.insert_transaction_logs()
            self.insert_transaction_retry_queue()

            print("\n=== ГОТОВО ===")
            print(f"Все данные успешно вставлены в базу данных {self.conn_params['database']}")

        except Exception as e:
            print(f"\n✗ Ошибка: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()


# Пример использования
if __name__ == "__main__":
    # Параметры подключения (можно задать через .env файл или напрямую)
    conn_params = {
        'host': 'localhost',
        'port': 5433,  # Порт для основной БД
        'database': 'api_warehouse',
        'user': 'api_user',
        'password': 'api_password'
    }

    # Инициализация и запуск
    generator = DataGenerator(conn_params)

    # Генерация 100 записей и вставка в БД
    generator.generate_and_insert_all(
        count=1000,  # Количество записей для каждой таблицы
        truncate=False  # Очищать ли таблицы перед вставкой
    )