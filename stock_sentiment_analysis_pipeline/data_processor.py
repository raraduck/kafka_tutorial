#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
데이터 처리기
주식 가격 변동 감지 및 감성 분석을 수행합니다.
"""

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
import sys
import os
import json

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config import MONITORED_STOCKS, PRICE_ALERT_CONFIG, SENTIMENT_CONFIG
from utils.logging_utils import setup_logger
from utils.kafka_utils import KafkaHelper
from database.db_helper import DatabaseHelper

# 로거 설정
logger = setup_logger(__name__, 'processor.log')

# NLTK 데이터 다운로드 (필요시)
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

class DataProcessor:
    """주식 데이터 및 뉴스/Reddit 데이터 처리기"""
    
    def __init__(self):
        """데이터 처리기 초기화"""
        self.stocks = MONITORED_STOCKS
        self.price_config = PRICE_ALERT_CONFIG
        self.sentiment_config = SENTIMENT_CONFIG
        
        # Kafka 헬퍼 초기화
        self.kafka = KafkaHelper()
        
        # 데이터베이스 헬퍼 초기화
        self.db = DatabaseHelper()
        
        # 감성 분석기 초기화
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
        # 주식별 이전 가격 데이터
        self.prev_prices = {}
        
        # 주식별 마지막 알림 시간
        self.last_alert = {}
        for ticker in self.stocks:
            self.last_alert[ticker] = {
                'price_surge': datetime.now() - timedelta(days=1),
                'price_drop': datetime.now() - timedelta(days=1),
                'volume_surge': datetime.now() - timedelta(days=1)
            }
    
    def process_stock_data(self, message):
        """주식 가격 데이터 처리 및 변동 감지"""
        ticker = message['ticker']
        price = message['price']
        volume = message['volume']
        timestamp = datetime.fromisoformat(message['timestamp'])
        
        # 이전 가격 데이터가 없는 경우 초기화
        if ticker not in self.prev_prices:
            # 최근 가격 데이터 가져오기
            recent_prices = self.db.get_recent_prices(ticker, limit=20)
            
            if recent_prices:
                # 이전 가격 계산 (평균)
                prev_price_sum = sum(float(record['price']) for record in recent_prices)
                prev_volume_sum = sum(int(record['volume']) for record in recent_prices)
                self.prev_prices[ticker] = {
                    'price': prev_price_sum / len(recent_prices),
                    'volume': prev_volume_sum / len(recent_prices)
                }
            else:
                # 이전 데이터가 없으면 현재 값으로 초기화
                self.prev_prices[ticker] = {
                    'price': price,
                    'volume': volume
                }
            
            # 초기화 단계에서는 처리 종료
            return
        
        # 가격 변동 계산
        prev_price = self.prev_prices[ticker]['price']
        price_change_pct = ((price - prev_price) / prev_price) * 100
        
        # 거래량 변동 계산
        prev_volume = self.prev_prices[ticker]['volume']
        volume_change_pct = ((volume - prev_volume) / prev_volume) * 100 if prev_volume else 0
        
        # 가격 급등 감지
        if (price_change_pct >= self.price_config['price_surge_threshold'] and 
            (datetime.now() - self.last_alert[ticker]['price_surge']).total_seconds() > self.price_config['cooldown_seconds']):
            
            # 이벤트 데이터 생성
            event_data = {
                "type": "price_surge",
                "ticker": ticker,
                "price": price,
                "prev_price": prev_price,
                "change_pct": price_change_pct,
                "timestamp": timestamp.isoformat()
            }
            
            # 이벤트 Kafka 전송
            self.kafka.send_message(
                topic=self.kafka.config['topics']['processed_events'],
                key=ticker,
                value=event_data
            )
            
            # 알림 메시지 Kafka 전송
            alert_message = {
                "type": "price_surge",
                "ticker": ticker,
                "message": f"{ticker} 가격 급등: ${prev_price:.2f} → ${price:.2f} ({price_change_pct:.2f}%)",
                "severity": "high",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka.send_message(
                topic=self.kafka.config['topics']['alerts'],
                key=ticker,
                value=alert_message
            )
            
            # 데이터베이스에 이벤트 저장
            self.db.store_event(
                ticker=ticker,
                event_type="price_surge",
                timestamp=timestamp,
                data=json.dumps(event_data)
            )
            
            # 알림 시간 업데이트
            self.last_alert[ticker]['price_surge'] = datetime.now()
            logger.info(f"{ticker} 가격 급등 감지: {price_change_pct:.2f}%")
        
        # 가격 급락 감지
        elif (price_change_pct <= self.price_config['price_drop_threshold'] and 
              (datetime.now() - self.last_alert[ticker]['price_drop']).total_seconds() > self.price_config['cooldown_seconds']):
            
            # 이벤트 데이터 생성
            event_data = {
                "type": "price_drop",
                "ticker": ticker,
                "price": price,
                "prev_price": prev_price,
                "change_pct": price_change_pct,
                "timestamp": timestamp.isoformat()
            }
            
            # 이벤트 Kafka 전송
            self.kafka.send_message(
                topic=self.kafka.config['topics']['processed_events'],
                key=ticker,
                value=event_data
            )
            
            # 알림 메시지 Kafka 전송
            alert_message = {
                "type": "price_drop",
                "ticker": ticker,
                "message": f"{ticker} 가격 급락: ${prev_price:.2f} → ${price:.2f} ({price_change_pct:.2f}%)",
                "severity": "high",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka.send_message(
                topic=self.kafka.config['topics']['alerts'],
                key=ticker,
                value=alert_message
            )
            
            # 데이터베이스에 이벤트 저장
            self.db.store_event(
                ticker=ticker,
                event_type="price_drop",
                timestamp=timestamp,
                data=json.dumps(event_data)
            )
            
            # 알림 시간 업데이트
            self.last_alert[ticker]['price_drop'] = datetime.now()
            logger.info(f"{ticker} 가격 급락 감지: {price_change_pct:.2f}%")
        
        # 거래량 급증 감지
        elif (volume_change_pct >= self.price_config['volume_surge_threshold'] and 
              (datetime.now() - self.last_alert[ticker]['volume_surge']).total_seconds() > self.price_config['cooldown_seconds']):
            
            # 이벤트 데이터 생성
            event_data = {
                "type": "volume_surge",
                "ticker": ticker,
                "volume": volume,
                "prev_volume": prev_volume,
                "change_pct": volume_change_pct,
                "timestamp": timestamp.isoformat()
            }
            
            # 이벤트 Kafka 전송
            self.kafka.send_message(
                topic=self.kafka.config['topics']['processed_events'],
                key=ticker,
                value=event_data
            )
            
            # 알림 메시지 Kafka 전송
            alert_message = {
                "type": "volume_surge",
                "ticker": ticker,
                "message": f"{ticker} 거래량 급증: {prev_volume:.0f} → {volume:.0f} ({volume_change_pct:.2f}%)",
                "severity": "medium",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka.send_message(
                topic=self.kafka.config['topics']['alerts'],
                key=ticker,
                value=alert_message
            )
            
            # 데이터베이스에 이벤트 저장
            self.db.store_event(
                ticker=ticker,
                event_type="volume_surge",
                timestamp=timestamp,
                data=json.dumps(event_data)
            )
            
            # 알림 시간 업데이트
            self.last_alert[ticker]['volume_surge'] = datetime.now()
            logger.info(f"{ticker} 거래량 급증 감지: {volume_change_pct:.2f}%")
        
        # 이전 가격 업데이트 (이동 평균)
        self.prev_prices[ticker] = {
            'price': (self.prev_prices[ticker]['price'] * 0.7) + (price * 0.3),  # 가중 평균
            'volume': (self.prev_prices[ticker]['volume'] * 0.7) + (volume * 0.3)  # 가중 평균
        }
    
    def process_news_data(self, message):
        """뉴스 데이터 처리 및 감성 분석"""
        ticker = message['ticker']
        title = message['title']
        content = message.get('content', '')
        source = message['source']
        url = message.get('url', '')
        published_at = message.get('published_at', datetime.now().isoformat())
        
        # 감성 분석
        text = f"{title} {content}"
        sentiment_scores = self.sentiment_analyzer.polarity_scores(text)
        compound_score = sentiment_scores['compound']
        
        # 감성 라벨 결정
        if compound_score >= self.sentiment_config['positive_threshold']:
            sentiment_label = "positive"
        elif compound_score <= self.sentiment_config['negative_threshold']:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # 감성 분석 결과 데이터베이스 저장
        self.db.store_sentiment_result(
            ticker=ticker,
            source=source,
            title=title,
            content=content,
            url=url,
            sentiment_score=compound_score,
            sentiment_label=sentiment_label,
            timestamp=datetime.fromisoformat(published_at)
        )
        
        # 매우 긍정적이거나 부정적인 뉴스에 대한 알림
        if compound_score > 0.5 or compound_score < -0.5:
            # 알림 메시지 Kafka 전송
            alert_message = {
                "type": "sentiment_alert",
                "ticker": ticker,
                "message": f"{ticker} {'매우 긍정적' if compound_score > 0.5 else '매우 부정적'} 뉴스: {title}",
                "sentiment_score": compound_score,
                "sentiment_label": sentiment_label,
                "source": source,
                "url": url,
                "severity": "medium",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka.send_message(
                topic=self.kafka.config['topics']['alerts'],
                key=ticker,
                value=alert_message
            )
            
            logger.info(f"{ticker} {sentiment_label} 뉴스 감지: {compound_score:.2f} - {title}")
    
    def run(self):
        """데이터 처리 메인 루프"""
        logger.info("데이터 처리기 시작")
        
        # Kafka 토픽 확인
        self.kafka.ensure_topics_exist()
        
        # raw_data 토픽 컨슈머 생성
        if not self.kafka.create_consumer(self.kafka.config['topics']['raw_data']):
            logger.error("Kafka 컨슈머 생성 실패. 프로그램을 종료합니다.")
            return
        
        try:
            for message in self.kafka.consumer:
                data = message.value
                
                # 데이터 타입에 따른 처리
                data_type = data.get('data_type', '')
                
                if data_type == 'stock_price':
                    self.process_stock_data(data)
                elif data_type in ['news', 'reddit']:
                    self.process_news_data(data)
                else:
                    logger.warning(f"알 수 없는 데이터 타입: {data_type}")
                
        except KeyboardInterrupt:
            logger.info("데이터 처리기 중지 요청")
        finally:
            # 리소스 정리
            self.kafka.close()
            self.db.disconnect()
            logger.info("데이터 처리기 종료")

if __name__ == "__main__":
    processor = DataProcessor()
    processor.run() 