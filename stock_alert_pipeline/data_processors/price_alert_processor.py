#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 가격 알림 프로세서 모듈
Kafka에서 주식 가격 데이터를 소비하고 설정된 조건에 따라 알림을 생성합니다.
"""

import sys
import os

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    STOCK_PRICES_TOPIC, 
    PRICE_CHANGE_THRESHOLD, STOCK_TICKERS
)
from utils.kafka_utils import create_consumer
from utils.log_utils import setup_logger
from notifiers.slack_notifier import SlackNotifier
from notifiers.email_notifier import EmailNotifier

# 로깅 설정
logger = setup_logger("price_alert_processor", "logs/price_alert_processor.log")

class PriceAlertProcessor:
    """주식 가격 알림 프로세서 클래스"""
    
    def __init__(self):
        """초기화 함수"""
        self.consumer = create_consumer(STOCK_PRICES_TOPIC, "price-alert-group")
        self.slack_notifier = SlackNotifier()
        self.email_notifier = EmailNotifier()
        self.previous_prices = {}  # 이전 가격 저장용 딕셔너리
        self.alert_thresholds = {}  # 종목별 알림 임계값 (기본값은 PRICE_CHANGE_THRESHOLD 사용)
        
        # 기본 알림 임계값 설정
        for ticker in STOCK_TICKERS:
            self.alert_thresholds[ticker] = PRICE_CHANGE_THRESHOLD
            
        logger.info(f"주식 가격 알림 프로세서 초기화 완료. 알림 임계값: {self.alert_thresholds}")
    
    def set_alert_threshold(self, ticker, threshold):
        """특정 종목의 알림 임계값 설정"""
        self.alert_thresholds[ticker] = threshold
        logger.info(f"종목 {ticker}의 알림 임계값을 {threshold}%로 설정")
    
    def process_message(self, message):
        """메시지 처리"""
        try:
            ticker = message['ticker']
            current_price = message['close']
            timestamp = message['timestamp']
            change_pct = message.get('change_pct', 0.0)
            
            # 이전 가격이 없는 경우 현재 가격 저장 후 처리 종료
            if ticker not in self.previous_prices:
                self.previous_prices[ticker] = current_price
                logger.info(f"종목 {ticker}의 초기 가격 {current_price:.2f} 설정")
                return
            
            # 가격 변동률 계산
            previous_price = self.previous_prices[ticker]
            if previous_price > 0:
                calc_change_pct = (current_price - previous_price) / previous_price * 100
            else:
                calc_change_pct = 0.0
                
            # message에 change_pct가 없는 경우 계산된 값 사용
            if change_pct == 0.0 and calc_change_pct != 0.0:
                change_pct = calc_change_pct
            
            # 임계값 초과 여부 확인
            threshold = self.alert_thresholds.get(ticker, PRICE_CHANGE_THRESHOLD)
            
            if abs(change_pct) >= threshold:
                # 알림 메시지 생성
                direction = "상승" if change_pct > 0 else "하락"
                message_text = f"{ticker} 주가가 {threshold}% 이상 {direction}했습니다: {change_pct:.2f}%"
                
                # 알림 전송
                logger.info(f"가격 알림 발생: {message_text}")
                self.slack_notifier.send_price_alert(ticker, current_price, change_pct, message_text)
                self.email_notifier.send_price_alert(ticker, current_price, change_pct, message_text)
            
            # 현재 가격 저장
            self.previous_prices[ticker] = current_price
            
        except Exception as e:
            logger.error(f"메시지 처리 중 오류 발생: {e}")
    
    def run(self):
        """알림 프로세서 실행"""
        logger.info("주식 가격 알림 프로세서 시작")
        
        try:
            for message in self.consumer:  # 무한 루프 - Kafka에서 메시지가 오면 처리
                self.process_message(message.value)
        except KeyboardInterrupt:
            logger.info("사용자에 의해 프로그램이 종료되었습니다.")
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka Consumer 연결 종료")

def main():
    """메인 함수"""
    processor = PriceAlertProcessor()
    processor.run()

if __name__ == "__main__":
    main() 