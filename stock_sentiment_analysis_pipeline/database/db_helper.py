#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
데이터베이스 헬퍼 클래스
PostgreSQL 데이터베이스 연결 및 쿼리 실행을 위한 유틸리티 클래스입니다.
"""

import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import sys
import os

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_CONFIG

logger = logging.getLogger(__name__)

class DatabaseHelper:
    """PostgreSQL 데이터베이스 연결 및 쿼리 실행을 위한 헬퍼 클래스"""
    
    def __init__(self):
        """데이터베이스 헬퍼 초기화"""
        self.config = DB_CONFIG
        self.conn = None
    
    def connect(self):
        """데이터베이스에 연결"""
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                dbname=self.config['dbname']
            )
            logger.info("데이터베이스에 연결되었습니다.")
            return True
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("데이터베이스 연결이 종료되었습니다.")
    
    def execute_query(self, query, params=None):
        """쿼리 실행"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            self.conn.rollback()
            return False
    
    def fetch_all(self, query, params=None):
        """쿼리 실행 후 모든 결과 반환"""
        if not self.conn:
            if not self.connect():
                return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            return []
    
    def fetch_one(self, query, params=None):
        """쿼리 실행 후 단일 결과 반환"""
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            return None
    
    # 주식 가격 저장
    def store_price_data(self, ticker, timestamp, price, volume):
        """주가 데이터 저장"""
        query = """
        INSERT INTO stock_prices (ticker, timestamp, price, volume) 
        VALUES (%s, %s, %s, %s)
        """
        return self.execute_query(query, (ticker, timestamp, price, volume))
    
    # 이벤트 저장
    def store_event(self, ticker, event_type, timestamp, data):
        """이벤트 데이터 저장"""
        query = """
        INSERT INTO events (ticker, event_type, timestamp, data) 
        VALUES (%s, %s, %s, %s)
        """
        return self.execute_query(query, (ticker, event_type, timestamp, json.dumps(data)))
    
    # 감성 분석 결과 저장
    def store_sentiment_result(self, ticker, source, title, content, url, sentiment_score, sentiment_label, timestamp):
        """감성 분석 결과 저장"""
        query = """
        INSERT INTO sentiment_results 
        (ticker, source, title, content, url, sentiment_score, sentiment_label, timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        return self.execute_query(
            query, 
            (ticker, source, title, content, url, sentiment_score, sentiment_label, timestamp)
        )
    
    # 최근 주가 데이터 조회
    def get_recent_prices(self, ticker, limit=100):
        """최근 주가 데이터 조회"""
        query = """
        SELECT * FROM stock_prices 
        WHERE ticker = %s 
        ORDER BY timestamp DESC 
        LIMIT %s
        """
        return self.fetch_all(query, (ticker, limit))
    
    # 특정 기간 내 이벤트 조회
    def get_events_by_period(self, ticker, start_time, end_time):
        """특정 기간 내 이벤트 조회"""
        query = """
        SELECT * FROM events 
        WHERE ticker = %s AND timestamp BETWEEN %s AND %s 
        ORDER BY timestamp DESC
        """
        return self.fetch_all(query, (ticker, start_time, end_time))
    
    # 최근 감성 분석 결과 조회
    def get_recent_sentiment(self, ticker, limit=20):
        """최근 감성 분석 결과 조회"""
        query = """
        SELECT * FROM sentiment_results 
        WHERE ticker = %s 
        ORDER BY timestamp DESC 
        LIMIT %s
        """
        return self.fetch_all(query, (ticker, limit)) 