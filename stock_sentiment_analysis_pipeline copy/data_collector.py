#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
데이터 수집기
주식 가격 데이터와 뉴스/Reddit 데이터를 수집합니다.
"""

import time
import yfinance as yf
import requests
import praw
from datetime import datetime, timedelta
import sys
import os

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config import MONITORED_STOCKS, NEWS_API_CONFIG, REDDIT_API_CONFIG, COLLECTION_INTERVAL
from utils.logging_utils import setup_logger
from utils.kafka_utils import KafkaHelper
from database.db_helper import DatabaseHelper

# 로거 설정
logger = setup_logger(__name__, 'collector.log')

class DataCollector:
    """다양한 소스에서 데이터를 수집하는 통합 수집기"""
    
    def __init__(self):
        """데이터 수집기 초기화"""
        self.stocks = MONITORED_STOCKS
        self.news_config = NEWS_API_CONFIG
        self.reddit_config = REDDIT_API_CONFIG
        self.collection_interval = COLLECTION_INTERVAL
        
        # Kafka 헬퍼 초기화
        self.kafka = KafkaHelper()
        
        # 데이터베이스 헬퍼 초기화
        self.db = DatabaseHelper()
        
        # 수집 시간 추적용 딕셔너리
        self.last_collection = {
            "stock": datetime.now() - timedelta(hours=1),
            "news": datetime.now() - timedelta(hours=1),
            "reddit": datetime.now() - timedelta(hours=1)
        }
    
    def collect_stock_data(self):
        """모니터링 대상 주식 데이터 수집"""
        logger.info(f"주식 데이터 수집 시작 - {len(self.stocks)}개 종목")
        
        for ticker in self.stocks:
            try:
                # yfinance로 주식 데이터 가져오기
                stock = yf.Ticker(ticker)
                hist = stock.history(period="1d")
                
                if hist.empty:
                    logger.warning(f"{ticker} 데이터를 찾을 수 없습니다.")
                    continue
                
                # 최신 주가 정보 추출
                latest = hist.iloc[-1]
                timestamp = datetime.now()
                price = latest['Close']
                volume = latest['Volume']
                
                # 주가 데이터 메시지 생성
                stock_data = {
                    "ticker": ticker,
                    "price": price,
                    "volume": volume,
                    "timestamp": timestamp.isoformat(),
                    "data_type": "stock_price"
                }
                
                # Kafka에 데이터 전송
                self.kafka.send_message(
                    topic=self.kafka.config['topics']['raw_data'],
                    key=ticker,
                    value=stock_data
                )
                
                # 데이터베이스에 저장
                self.db.store_price_data(ticker, timestamp, price, volume)
                
                logger.info(f"{ticker} 데이터 수집 완료: ${price:.2f}, 거래량: {volume}")
                
            except Exception as e:
                logger.error(f"{ticker} 데이터 수집 실패: {e}")
        
        self.last_collection["stock"] = datetime.now()
        logger.info("주식 데이터 수집 완료")
    
    def collect_news_data(self):
        """뉴스 API를 통한 주식 관련 뉴스 수집"""
        logger.info("뉴스 데이터 수집 시작")
        
        for ticker in self.stocks:
            try:
                # 뉴스 API 쿼리 파라미터 구성
                params = {
                    'q': ticker,
                    'apiKey': self.news_config['api_key'],
                    'language': self.news_config['language'],
                    'sortBy': self.news_config['sort_by'],
                    'pageSize': self.news_config['max_articles_per_request']
                }
                
                # 뉴스 API 요청
                response = requests.get(self.news_config['base_url'], params=params)
                
                if response.status_code != 200:
                    logger.error(f"뉴스 API 요청 실패: {response.status_code}, {response.text}")
                    continue
                
                news_data = response.json()
                
                if 'articles' not in news_data or not news_data['articles']:
                    logger.warning(f"{ticker}에 대한 뉴스가 없습니다.")
                    continue
                
                # 각 뉴스 기사 처리
                for article in news_data['articles']:
                    # 뉴스 데이터 메시지 생성
                    news_item = {
                        "ticker": ticker,
                        "title": article.get('title', ''),
                        "content": article.get('description', ''),
                        "source": "news",
                        "url": article.get('url', ''),
                        "published_at": article.get('publishedAt', datetime.now().isoformat()),
                        "timestamp": datetime.now().isoformat(),
                        "data_type": "news"
                    }
                    
                    # Kafka에 데이터 전송
                    self.kafka.send_message(
                        topic=self.kafka.config['topics']['raw_data'],
                        key=ticker,
                        value=news_item
                    )
                
                logger.info(f"{ticker}에 대한 뉴스 {len(news_data['articles'])}개 수집 완료")
                
            except Exception as e:
                logger.error(f"{ticker} 뉴스 수집 실패: {e}")
        
        self.last_collection["news"] = datetime.now()
        logger.info("뉴스 데이터 수집 완료")
    
    def collect_reddit_data(self):
        """Reddit API를 통한 주식 관련 게시물 수집"""
        logger.info("Reddit 데이터 수집 시작")
        
        try:
            # Reddit API 인증
            reddit = praw.Reddit(
                client_id=self.reddit_config['client_id'],
                client_secret=self.reddit_config['client_secret'],
                user_agent=self.reddit_config['user_agent']
            )
            
            # 모니터링 대상 서브레딧
            for subreddit_name in self.reddit_config['subreddits']:
                subreddit = reddit.subreddit(subreddit_name)
                
                # 인기 게시물 가져오기
                for submission in subreddit.hot(limit=self.reddit_config['limit']):
                    # 관련 주식 확인
                    related_stocks = []
                    for ticker in self.stocks:
                        if ticker.lower() in submission.title.lower() or ticker.lower() in submission.selftext.lower():
                            related_stocks.append(ticker)
                    
                    if not related_stocks:
                        continue
                    
                    # 각 관련 주식에 대해 메시지 생성
                    for ticker in related_stocks:
                        # Reddit 게시물 데이터 메시지 생성
                        reddit_item = {
                            "ticker": ticker,
                            "title": submission.title,
                            "content": submission.selftext,
                            "source": "reddit",
                            "url": f"https://www.reddit.com{submission.permalink}",
                            "published_at": datetime.fromtimestamp(submission.created_utc).isoformat(),
                            "score": submission.score,
                            "num_comments": submission.num_comments,
                            "timestamp": datetime.now().isoformat(),
                            "data_type": "reddit"
                        }
                        
                        # Kafka에 데이터 전송
                        self.kafka.send_message(
                            topic=self.kafka.config['topics']['raw_data'],
                            key=ticker,
                            value=reddit_item
                        )
                
                logger.info(f"서브레딧 {subreddit_name}에서 데이터 수집 완료")
                
        except Exception as e:
            logger.error(f"Reddit 데이터 수집 실패: {e}")
        
        self.last_collection["reddit"] = datetime.now()
        logger.info("Reddit 데이터 수집 완료")
    
    def run(self):
        """데이터 수집 메인 루프"""
        logger.info("데이터 수집기 시작")
        
        # Kafka 토픽 확인
        self.kafka.ensure_topics_exist()
        
        try:
            while True:
                now = datetime.now()
                
                # 주식 데이터 수집 (매 간격마다)
                if (now - self.last_collection["stock"]).total_seconds() >= self.collection_interval:
                    self.collect_stock_data()
                
                # 뉴스 데이터 수집 (매 간격마다)
                if (now - self.last_collection["news"]).total_seconds() >= self.collection_interval:
                    self.collect_news_data()
                
                # Reddit 데이터 수집 (매 간격마다)
                if (now - self.last_collection["reddit"]).total_seconds() >= self.collection_interval:
                    self.collect_reddit_data()
                
                # 잠시 대기
                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("데이터 수집기 중지 요청")
        finally:
            # 리소스 정리
            self.kafka.close()
            self.db.disconnect()
            logger.info("데이터 수집기 종료")

if __name__ == "__main__":
    collector = DataCollector()
    collector.run() 