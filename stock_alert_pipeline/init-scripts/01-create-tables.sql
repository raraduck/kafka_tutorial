-- 주식 가격 테이블 생성
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(10, 2) NOT NULL,
    high NUMERIC(10, 2) NOT NULL,
    low NUMERIC(10, 2) NOT NULL,
    close NUMERIC(10, 2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date);

-- 알림 테이블 생성
CREATE TABLE IF NOT EXISTS stock_alerts (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    change_percent NUMERIC(8, 4) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 분석 결과 테이블 생성
CREATE TABLE IF NOT EXISTS stock_analysis_results (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    ma_short NUMERIC(10, 2),
    ma_long NUMERIC(10, 2),
    rsi NUMERIC(6, 2),
    macd NUMERIC(10, 4),
    signal NUMERIC(10, 4),
    buy_signal VARCHAR(50),
    sell_signal VARCHAR(50),
    signal_strength VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
