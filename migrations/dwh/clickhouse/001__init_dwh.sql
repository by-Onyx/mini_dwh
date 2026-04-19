-- =====================================================
-- Создание схемы (database) в ClickHouse
-- =====================================================

-- Создание базы данных trnx
CREATE DATABASE IF NOT EXISTS trnx;

-- Использование базы данных
USE trnx;

-- =====================================================
-- Таблица users
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.users (
    user_id UInt64,
    email String,
    full_name String,
    created_at DateTime,
    is_blocked UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY user_id;

-- =====================================================
-- Таблица accounts
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.accounts (
    account_id UInt64,
    user_id UInt64,
    currency FixedString(3),
    balance Decimal(15, 2),
    version UInt64,
    updated_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY account_id;

-- =====================================================
-- Таблица payment_cards
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.payment_cards (
    card_id UInt64,
    user_id UInt64,
    card_last4 FixedString(4),
    card_type String,
    expiry_month UInt8,
    expiry_year UInt16,
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY card_id;

-- =====================================================
-- Таблица card_fingerprints
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.card_fingerprints (
    fingerprint_id UInt64,
    card_id UInt64,
    device_fp String,
    risk_score Decimal(5, 2)
) ENGINE = ReplacingMergeTree()
ORDER BY fingerprint_id;

-- =====================================================
-- Таблица transactions
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.transactions (
    transaction_id UInt64,
    user_id UInt64,
    account_id UInt64,
    amount Decimal(15, 2),
    currency FixedString(3),
    payment_method Enum8('card' = 1, 'cash' = 2),
    card_id UInt64,
    cash_terminal_id String,
    status Enum8('pending' = 1, 'success' = 2, 'failed' = 3, 'cancelled' = 4),
    type Enum8('authorize' = 1, 'capture' = 2, 'refund' = 3, 'void' = 4),
    retry_count UInt16,
    last_error String,
    external_txn_id String,
    created_at DateTime,
    processed_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY transaction_id;

-- =====================================================
-- Таблица transaction_pipeline_log
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.transaction_pipeline_log (
    log_id UInt64,
    transaction_id UInt64,
    step_name String,
    status String,
    payload String,  -- JSON как String в ClickHouse
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY log_id;

-- =====================================================
-- Таблица transaction_retry_queue
-- =====================================================
CREATE TABLE IF NOT EXISTS trnx.transaction_retry_queue (
    retry_id UInt64,
    transaction_id UInt64,
    scheduled_at DateTime,
    attempt_number UInt16,
    status String,
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY retry_id;