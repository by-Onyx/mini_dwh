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

-- =====================================================
-- ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ
-- =====================================================
-- =====================================================
-- DDL для транзакционных данных с разделением на схемы
-- stage слой - БЕЗ foreign key
-- final слой - С foreign key
-- Оба слоя - BIGINT вместо BIGSERIAL
-- =====================================================

-- Создание схем
CREATE SCHEMA IF NOT EXISTS trnx_stage;
CREATE SCHEMA IF NOT EXISTS trnx_final;

-- =====================================================
-- СХЕМА trnx_stage (сырые данные, без FK)
-- =====================================================

-- 1. Таблица пользователей (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.users (
    user_id BIGINT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    is_blocked BOOLEAN NOT NULL
);

-- 2. Таблица счетов (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.accounts (
    account_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- 3. Таблица платежных карт (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.payment_cards (
    card_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    card_last4 CHAR(4) NOT NULL,
    card_type VARCHAR(50) NOT NULL,
    expiry_month SMALLINT NOT NULL,
    expiry_year SMALLINT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- 4. Таблица fingerprint'ов карт (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.card_fingerprints (
    fingerprint_id BIGINT PRIMARY KEY,
    card_id BIGINT NOT NULL,
    device_fp VARCHAR(255) NOT NULL,
    risk_score DECIMAL(5, 2) NOT NULL
);

-- 5. ENUM типы для транзакций (stage)
DO $$ BEGIN
    CREATE TYPE trnx_stage.transaction_type_enum AS ENUM ('authorize', 'capture', 'refund', 'void');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE trnx_stage.payment_method_enum AS ENUM ('card', 'cash');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE trnx_stage.transaction_status_enum AS ENUM ('pending', 'success', 'failed', 'cancelled');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- 6. Таблица транзакций (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.transactions (
    transaction_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    payment_method trnx_stage.payment_method_enum NOT NULL,
    card_id BIGINT,
    cash_terminal_id VARCHAR(50),
    status trnx_stage.transaction_status_enum NOT NULL,
    type trnx_stage.transaction_type_enum NOT NULL,
    retry_count SMALLINT NOT NULL,
    last_error TEXT,
    external_txn_id VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

-- 7. Таблица логов pipeline'а транзакций (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.transaction_pipeline_log (
    log_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    step_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL
);

-- 8. Таблица очереди повторных попыток (stage)
CREATE TABLE IF NOT EXISTS trnx_stage.transaction_retry_queue (
    retry_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    attempt_number SMALLINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- =====================================================
-- СХЕМА trnx_final (очищенные данные, с FK, но без SERIAL)
-- =====================================================

-- 1. Таблица пользователей (final)
CREATE TABLE IF NOT EXISTS trnx_final.users (
    user_id BIGINT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    is_blocked BOOLEAN NOT NULL
);
-- 2. Таблица счетов (final)
CREATE TABLE IF NOT EXISTS trnx_final.accounts (
    account_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_accounts_user FOREIGN KEY (user_id) REFERENCES trnx_final.users(user_id) ON DELETE CASCADE
);

-- 3. Таблица платежных карт (final)
CREATE TABLE IF NOT EXISTS trnx_final.payment_cards (
    card_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    card_last4 CHAR(4) NOT NULL,
    card_type VARCHAR(50) NOT NULL,
    expiry_month SMALLINT NOT NULL,
    expiry_year SMALLINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_payment_cards_user FOREIGN KEY (user_id) REFERENCES trnx_final.users(user_id) ON DELETE CASCADE
);

-- 4. Таблица fingerprint'ов карт (final)
CREATE TABLE IF NOT EXISTS trnx_final.card_fingerprints (
    fingerprint_id BIGINT PRIMARY KEY,
    card_id BIGINT NOT NULL,
    device_fp VARCHAR(255) NOT NULL,
    risk_score DECIMAL(5, 2) NOT NULL,
    CONSTRAINT fk_fingerprints_card FOREIGN KEY (card_id) REFERENCES trnx_final.payment_cards(card_id) ON DELETE CASCADE
);

-- 5. ENUM типы для транзакций (final)
DO $$ BEGIN
    CREATE TYPE trnx_final.transaction_type_enum AS ENUM ('authorize', 'capture', 'refund', 'void');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE trnx_final.payment_method_enum AS ENUM ('card', 'cash');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE trnx_final.transaction_status_enum AS ENUM ('pending', 'success', 'failed', 'cancelled');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- 6. Таблица транзакций (final)
CREATE TABLE IF NOT EXISTS trnx_final.transactions (
    transaction_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    payment_method trnx_final.payment_method_enum NOT NULL,
    card_id BIGINT,
    cash_terminal_id VARCHAR(50),
    status trnx_final.transaction_status_enum NOT NULL,
    type trnx_final.transaction_type_enum NOT NULL,
    retry_count SMALLINT NOT NULL,
    last_error TEXT,
    external_txn_id VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_transactions_user FOREIGN KEY (user_id) REFERENCES trnx_final.users(user_id) ON DELETE CASCADE,
    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES trnx_final.accounts(account_id) ON DELETE CASCADE,
    CONSTRAINT fk_transactions_card FOREIGN KEY (card_id) REFERENCES trnx_final.payment_cards(card_id) ON DELETE SET NULL,
    CONSTRAINT check_payment_method_fields CHECK (
        (payment_method = 'card' AND card_id IS NOT NULL AND cash_terminal_id IS NULL) OR
        (payment_method = 'cash' AND card_id IS NULL AND cash_terminal_id IS NOT NULL)
    )
);

-- 7. Таблица логов pipeline'а транзакций (final)
CREATE TABLE IF NOT EXISTS trnx_final.transaction_pipeline_log (
    log_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    step_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_logs_transaction FOREIGN KEY (transaction_id) REFERENCES trnx_final.transactions(transaction_id) ON DELETE CASCADE
);

-- 8. Таблица очереди повторных попыток (final)
CREATE TABLE IF NOT EXISTS trnx_final.transaction_retry_queue (
    retry_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    attempt_number SMALLINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_retry_transaction FOREIGN KEY (transaction_id) REFERENCES trnx_final.transactions(transaction_id) ON DELETE CASCADE
);

-- =====================================================
-- ИНДЕКСЫ
-- =====================================================
-- Индексы для stage
CREATE INDEX IF NOT EXISTS idx_stage_users_email ON trnx_stage.users(email);
CREATE INDEX IF NOT EXISTS idx_stage_accounts_user_id ON trnx_stage.accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_stage_payment_cards_user_id ON trnx_stage.payment_cards(user_id);
CREATE INDEX IF NOT EXISTS idx_stage_transactions_user_id ON trnx_stage.transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_stage_transactions_external_txn_id ON trnx_stage.transactions(external_txn_id);
CREATE INDEX IF NOT EXISTS idx_stage_transactions_status ON trnx_stage.transactions(status);
CREATE INDEX IF NOT EXISTS idx_stage_transactions_created_at ON trnx_stage.transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_stage_logs_transaction_id ON trnx_stage.transaction_pipeline_log(transaction_id);
CREATE INDEX IF NOT EXISTS idx_stage_retry_queue_transaction_id ON trnx_stage.transaction_retry_queue(transaction_id);
CREATE INDEX IF NOT EXISTS idx_stage_retry_queue_scheduled_at ON trnx_stage.transaction_retry_queue(scheduled_at);

-- Индексы для final
CREATE INDEX IF NOT EXISTS idx_final_users_email ON trnx_final.users(email);
CREATE INDEX IF NOT EXISTS idx_final_accounts_user_id ON trnx_final.accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_final_payment_cards_user_id ON trnx_final.payment_cards(user_id);
CREATE INDEX IF NOT EXISTS idx_final_transactions_user_id ON trnx_final.transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_final_transactions_external_txn_id ON trnx_final.transactions(external_txn_id);
CREATE INDEX IF NOT EXISTS idx_final_transactions_status ON trnx_final.transactions(status);
CREATE INDEX IF NOT EXISTS idx_final_transactions_created_at ON trnx_final.transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_final_logs_transaction_id ON trnx_final.transaction_pipeline_log(transaction_id);
CREATE INDEX IF NOT EXISTS idx_final_retry_queue_transaction_id ON trnx_final.transaction_retry_queue(transaction_id);
