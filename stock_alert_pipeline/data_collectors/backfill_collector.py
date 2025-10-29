#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 데이터 수집기 모듈
yfinance 라이브러리를 사용하여 주식 데이터를 수집하고 Kafka로 전송합니다.
"""

import yfinance as yf
import json
import time, random
import sys
import argparse
import os
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS, STOCK_PRICES_TOPIC, STOCK_TICKERS, STOCK_COLLECTION_INTERVAL

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stock_backfill.log")
    ]
)
logger = logging.getLogger("stock_backfill")

def parse_args():
    """명령줄 인수를 파싱합니다."""
    parser = argparse.ArgumentParser(description='주식 배치 분석 작업')
    
    parser.add_argument('--start-date', type=str, required=True,
                        help='분석 시작 날짜 (YYYY-MM-DD 형식).')
    
    parser.add_argument('--end-date', type=str, required=True,
                        help='분석 종료 날짜 (YYYY-MM-DD 형식).')
    
    parser.add_argument('--tickers', type=str, default=None,
                        help='분석할 주식 종목 코드 (쉼표로 구분)')
    
    parser.add_argument('--kafka-topic', type=str, default='stock_prices_topic',
                        help='카프카 토픽 이름')
    
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='카프카 서버 주소')
    
    parser.add_argument('--interval', type=str, default='1d',
                        help='example: 1d, 1h, 1m')
    
    args = parser.parse_args()
    
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # 종목 리스트 처리
    if args.tickers:
        args.tickers = args.tickers.split(',')
    
    return args

def create_kafka_producer(args):
    """Kafka Producer 생성"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers, # KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"Kafka Producer 연결 성공: {args.bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Kafka Producer 연결 실패: {e}")
        return None


def collect_stock_data(args, producer):
    """주식 데이터 수집 및 Kafka로 전송"""
    if not producer:
        logger.error("Kafka Producer가 없습니다. 데이터 수집을 중단합니다.")
        return

    logger.info(f"다음 종목들의 데이터 수집 시작: {', '.join(args.tickers)}")
    
    # while True:
    for ticker in args.tickers:
        try:
            # logger.info(f"디버깅중...")
            time.sleep(random.uniform(10, 30)) # ✅ 종목별 요청 간격 랜덤
            # 최근 1일 데이터 가져오기 (1분 간격)
            data = yf.download(
                ticker, 
                start=args.start_date, 
                end=args.end_date,
                interval=args.interval
            )
        
            if not data.empty:
                # 가장 최근 데이터 추출
                latest = data.iloc[-1]
                
                # Kafka에 전송할 메시지 구성
                message = {
                    'ticker': ticker,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'), # datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'open': float(latest['Open']),
                    'high': float(latest['High']),
                    'low': float(latest['Low']),
                    'close': float(latest['Close']),
                    'volume': int(latest['Volume']),
                    'change_pct': float((latest['Close'] - data.iloc[-2]['Close']) / data.iloc[-2]['Close'] * 100) if len(data) > 1 else 0.0
                }
                
                # Kafka 토픽에 메시지 전송
                producer.send(args.kafka_topic, message)
                logger.info(f"종목 {ticker} 데이터 전송 완료: 현재가 {message['close']:.2f}, 변동률 {message['change_pct']:.2f}%")
            else:
                logger.warning(f"종목 {ticker}에 대한 데이터를 가져올 수 없습니다.")
        
        except Exception as e:
            logger.error(f"종목 {ticker} 데이터 수집 중 오류 발생: {e}")
        
        # # 다음 데이터 수집까지 대기
        # logger.info(f"{STOCK_COLLECTION_INTERVAL}초 후 다음 데이터 수집을 시작합니다.")
        # time.sleep(STOCK_COLLECTION_INTERVAL)


def main():
    """메인 함수"""
    # 명령줄 인수 파싱
    args = parse_args()
    # logger.info("주식 데이터 수집기 시작")
    logger.info(f"주식 백필링 시작: {args.start_date.strftime('%Y-%m-%d')} ~ {args.end_date.strftime('%Y-%m-%d')}")
    if args.tickers:
        logger.info(f"분석 대상 종목: {', '.join(args.tickers)}")
    producer = create_kafka_producer(args)
    
    try:
        collect_stock_data(args, producer)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka Producer 연결 종료")

    # try:
    #     # 시작 메시지
    #     print(f"주식 백필링 시작: {args.start_date.strftime('%Y-%m-%d')} ~ {args.end_date.strftime('%Y-%m-%d')}")
    #     if args.tickers:
    #         print(f"분석 대상 종목: {', '.join(args.tickers)}")
        
    #     stock_df = yf.download(
            
    #     )
    #     # 데이터 소스에 따라 데이터 로드 방식 결정
    #     if args.data_source == 'kafka':
    #         # 카프카에서 직접 데이터 로드
    #         stock_df = analyzer.load_data_from_kafka(
    #             topic=args.kafka_topic,
    #             group_id=args.kafka_group,
    #             start_time=args.start_date,
    #             end_time=args.end_date
    #         )
    #     else:
    #         # 파일이나 DB에서 데이터 로드 
    #         stock_df = analyzer.load_stock_data(
    #             db_url=args.db_url,
    #             db_properties=None,
    #             table_name=args.table_name,
    #             start_date=args.start_date,
    #             end_date=args.end_date
    #         )
        
    #     # 데이터가 비어있는지 확인
    #     if stock_df.count() == 0:
    #         print("분석할 데이터가 없습니다. 프로그램을 종료합니다.")
    #         return
        
    #     # 종목 필터링
    #     if args.tickers:
    #         stock_df = stock_df.filter(col("ticker").isin(args.tickers))
        
    #     # 필터링 후 데이터가 비어있는지 확인
    #     if stock_df.count() == 0:
    #         print(f"지정한 종목 {', '.join(args.tickers)}에 대한 데이터가 없습니다. 프로그램을 종료합니다.")
    #         return

    #     # 일별 OHLCV 데이터로 집계
    #     print("일별 OHLCV 데이터로 집계하는 중...")
    #     stock_data = analyzer.calculate_daily_ohlcv(stock_df)
        
    #     # 기술적 지표 계산 (일별 데이터 사용)
    #     stock_data = analyzer.run_analysis_pipeline(stock_data)
        
    #     # 크로스오버 감지 (일별 데이터 사용)
    #     stock_data.daily_data = analyzer.detect_crossovers(stock_data.daily_data)
        
    #     # 가격 패턴 식별 (일별 데이터 사용)
    #     stock_data.daily_data = analyzer.identify_price_patterns(stock_data.daily_data)
        
    #     # 거래 신호 생성 (일별 데이터 사용)
    #     stock_data.daily_data = analyzer.generate_trading_signals(stock_data.daily_data)
        
    #     # 종목별 요약 (일별 데이터 사용)
    #     summary_df = analyzer.summarize_by_ticker(stock_data.daily_data)
        
    #     # 결과 저장
    #     analyzer.save_results(summary_df, output_path=args.output_path, format=args.format)
        
    #     # 원본 데이터도 저장하고 싶다면:
    #     if args.save_raw_data:
    #         raw_output_path = f"{args.output_path}_raw"
    #         analyzer.save_results(stock_data.raw_data, output_path=raw_output_path, format=args.format)

    #     print(f"분석 완료. 결과가 {args.output_path}에 저장되었습니다.")
        
    # except Exception as e:
    #     print(f"분석 중 오류 발생: {e}")
    #     import traceback
    #     traceback.print_exc()
    #     sys.exit(1)
    # finally:
    #     # Spark 세션 종료
    #     if analyzer and analyzer.spark:
    #         analyzer.spark.stop()
    #         print("Spark 세션 종료")

if __name__ == "__main__":
    main() 