#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
""" Kafka 의존성을 제거한 테스트 코드 """
from config.config import STOCK_TICKERS
from utils.log_utils import setup_logger
from datetime import datetime
import yfinance as yf

logger = setup_logger("stock_data_collector_test", "logs/stock_data_collector_test.log")

def main():
    """메인 함수"""
    logger.info("주식 데이터 수집기 시작")
    
    try:
        # Kafka 대신 데이터만 출력
        for ticker in STOCK_TICKERS:
            try:
                data = yf.download(ticker, period='1d', interval='1m')
                if not data.empty:
                    latest = data.iloc[-1]
                    message = {
                        'ticker': ticker,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'open': float(latest['Open']),
                        'high': float(latest['High']),
                        'low': float(latest['Low']),
                        'close': float(latest['Close']),
                        'volume': int(latest['Volume']),
                        'change_pct': float((latest['Close'] - data.iloc[-2]['Close']) / data.iloc[-2]['Close'] * 100) if len(data) > 1 else 0.0
                    }
                    print(f"종목: {ticker}, 현재가: {message['close']:.2f}, 변동률: {message['change_pct']:.2f}%")
            except Exception as e:
                logger.error(f"종목 {ticker} 데이터 수집 중 오류 발생: {e}")
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")

if __name__ == "__main__":
    main()