-- 주식 가격 데이터 테이블
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 이벤트 데이터 테이블
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    event_type VARCHAR(50) NOT NULL,  -- price_surge, price_drop, etc.
    timestamp TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL,  -- 이벤트 관련 데이터
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 감성 분석 결과 테이블
CREATE TABLE IF NOT EXISTS sentiment_results (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    source VARCHAR(20) NOT NULL,  -- news, reddit, etc.
    title TEXT,
    content TEXT,
    url TEXT,
    sentiment_score NUMERIC(4, 3) NOT NULL,
    sentiment_label VARCHAR(10) NOT NULL,  -- positive, negative, neutral
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_timestamp ON stock_prices(ticker, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_ticker_timestamp ON events(ticker, timestamp);
CREATE INDEX IF NOT EXISTS idx_sentiment_results_ticker_timestamp ON sentiment_results(ticker, timestamp); 