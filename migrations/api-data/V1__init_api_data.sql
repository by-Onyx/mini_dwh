-- =====================================================
-- DDL для генератора тестовых данных
-- База данных: PostgreSQL
-- =====================================================

-- 1. Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    user_id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE
);

-- 2. Таблица счетов
CREATE TABLE IF NOT EXISTS accounts (
    account_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    currency CHAR(3) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Таблица платежных карт
CREATE TABLE IF NOT EXISTS payment_cards (
    card_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    card_last4 CHAR(4) NOT NULL,
    card_type VARCHAR(50) NOT NULL,
    expiry_month SMALLINT NOT NULL CHECK (expiry_month BETWEEN 1 AND 12),
    expiry_year SMALLINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Таблица fingerprint'ов карт
CREATE TABLE IF NOT EXISTS card_fingerprints (
    fingerprint_id BIGSERIAL PRIMARY KEY,
    card_id BIGINT NOT NULL REFERENCES payment_cards(card_id) ON DELETE CASCADE,
    device_fp VARCHAR(255) NOT NULL,
    risk_score DECIMAL(5, 2) NOT NULL DEFAULT 0.00 CHECK (risk_score BETWEEN 0 AND 100)
);

-- 5. ENUM типы для транзакций
CREATE TYPE transaction_type_enum AS ENUM ('authorize', 'capture', 'refund', 'void');
CREATE TYPE payment_method_enum AS ENUM ('card', 'cash');
CREATE TYPE transaction_status_enum AS ENUM ('pending', 'success', 'failed', 'cancelled');

-- 6. Таблица транзакций
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    account_id BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    amount DECIMAL(15, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    payment_method payment_method_enum NOT NULL,
    card_id BIGINT REFERENCES payment_cards(card_id) ON DELETE SET NULL,
    cash_terminal_id VARCHAR(50),
    status transaction_status_enum NOT NULL DEFAULT 'pending',
    type transaction_type_enum NOT NULL,
    retry_count SMALLINT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error TEXT,
    external_txn_id VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Проверка: для card не может быть cash_terminal_id, и наоборот
    CONSTRAINT check_payment_method_fields CHECK (
        (payment_method = 'card' AND card_id IS NOT NULL AND cash_terminal_id IS NULL) OR
        (payment_method = 'cash' AND card_id IS NULL AND cash_terminal_id IS NOT NULL)
    )
);

-- 7. Таблица логов pipeline'а транзакций
CREATE TABLE IF NOT EXISTS transaction_pipeline_log (
    log_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    step_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 8. Таблица очереди повторных попыток
CREATE TABLE IF NOT EXISTS transaction_retry_queue (
    retry_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    scheduled_at TIMESTAMP NOT NULL,
    attempt_number SMALLINT NOT NULL CHECK (attempt_number > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);