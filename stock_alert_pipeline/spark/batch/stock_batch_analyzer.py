#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 배치 분석 모듈
주식 데이터에 대한 배치 분석을 수행하여 다양한 기술적 지표와 패턴을 계산합니다.
"""

import os
import sys
import argparse
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, avg, max, min, lag,
    from_json, to_timestamp, first, last, sum, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType
)
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql import DataFrame
from dataclasses import dataclass
from typing import Optional

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS, STOCK_PRICES_TOPIC,
    MA_SHORT_PERIOD, MA_LONG_PERIOD, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD,
    MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD
)

@dataclass
class StockData:
    """주식 데이터를 담는 데이터 클래스"""
    raw_data: DataFrame  # 원본 데이터 (분/시간 단위)
    daily_data: Optional[DataFrame] = None  # 일별 집계 데이터

    def get_data(self, frequency: str = 'daily') -> DataFrame:
        """지정된 주기의 데이터 반환"""
        if frequency == 'raw':
            return self.raw_data
        return self.daily_data if self.daily_data is not None else self.raw_data

class StockBatchAnalyzer:
    """주식 배치 분석 클래스"""
    
    def __init__(self, spark_master=None, app_name="StockBatchAnalyzer"):
        """초기화 함수"""
        self.spark_master = spark_master
        self.app_name = app_name
        self.spark = self._create_spark_session()
        
        # 기술적 지표 설정
        self.ma_short_period = MA_SHORT_PERIOD
        self.ma_long_period = MA_LONG_PERIOD
        self.rsi_period = RSI_PERIOD
        self.rsi_overbought = RSI_OVERBOUGHT
        self.rsi_oversold = RSI_OVERSOLD
        self.macd_fast_period = MACD_FAST_PERIOD
        self.macd_slow_period = MACD_SLOW_PERIOD
        self.macd_signal_period = MACD_SIGNAL_PERIOD

        self.ticker_window = Window.partitionBy("ticker").orderBy("date")
    
    def _create_spark_session(self):
        """Spark 세션 생성"""
        return SparkSession.builder \
            .appName(self.app_name) \
            .master(self.spark_master if self.spark_master else "local[*]") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
    
    def load_stock_data(self, db_url, db_properties, table_name, start_date=None, end_date=None):
        """
        데이터베이스나 파일에서 주식 데이터를 로드합니다.
        
        Args:
            db_url: 데이터베이스 URL 또는 파일 경로
            db_properties: 데이터베이스 연결 속성
            table_name: 테이블 이름 또는 컬렉션 이름
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            DataFrame: 로드된 주식 데이터
        """
        try:
            # 데이터베이스에서 로드
            if db_url and db_properties:
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", db_url) \
                    .option("dbtable", table_name) \
                    .options(**db_properties) \
                    .load()
            # 파일에서 로드 (예: Parquet 파일)
            elif db_url and not db_properties:
                df = self.spark.read.parquet(db_url)
            # Kafka에서 저장된 데이터 로드
            else:
                # 여기서는 예시로 기본 경로에서 Parquet 파일을 로드합니다
                # 실제 구현에서는 적절한 데이터 소스를 사용해야 합니다
                default_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "stock_prices")
                if os.path.exists(default_path):
                    df = self.spark.read.parquet(default_path)
                else:
                    raise ValueError(f"유효한 데이터 소스를 제공해야 합니다. 기본 경로 {default_path}가 존재하지 않습니다.")
            
            # 날짜 필터링
            if start_date:
                df = df.filter(col("date") >= start_date)
            if end_date:
                df = df.filter(col("date") <= end_date)
                
            return df
            
        except Exception as e:
            print(f"데이터 로드 중 오류 발생: {e}")
            raise
        
    def load_data_from_kafka(self, topic, group_id, start_time=None, end_time=None, batch_duration=None):
        """
        카프카에서 직접 데이터를 로드하여 파싱합니다.
        
        Args:
            topic (str): 카프카 토픽 이름
            group_id (str): 컨슈머 그룹 ID
            start_time (datetime): 시작 시간
            end_time (datetime): 종료 시간
            batch_duration (int): 배치 처리 시간(초)
            
        Returns:
            DataFrame: 파싱된 주식 데이터
        """
        try:
            print(f"Spark-Kafka 통합 API를 사용하여 '{topic}' 토픽에서 데이터 로드 시작")
            
            # Kafka에서 데이터 로드
            kafka_df = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            
            if kafka_df.count() == 0:
                print("Kafka 토픽에서 데이터를 찾을 수 없습니다. 기본 경로에서 데이터를 로드합니다.")
                default_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "stock_prices")
                if os.path.exists(default_path):
                    return self.spark.read.parquet(default_path)
                else:
                    print(f"기본 경로 {default_path}가 존재하지 않습니다. 빈 DataFrame을 반환합니다.")
                    return self.spark.createDataFrame([], self._get_stock_schema())
            
            # JSON 스키마 정의
            json_schema = StructType([
                StructField("ticker", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("volume", IntegerType(), True),
                StructField("change_pct", DoubleType(), True)
            ])
            
            # 데이터 파싱
            parsed_df = kafka_df \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(from_json(col("value"), json_schema).alias("data")) \
                .select("data.*") \
                .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("date", col("timestamp").cast("date"))
            
            # 날짜 필터링
            if start_time:
                parsed_df = parsed_df.filter(col("timestamp") >= start_time)
            if end_time:
                parsed_df = parsed_df.filter(col("timestamp") <= end_time)
            
            return parsed_df
            
        except Exception as e:
            print(f"Spark-Kafka 통합 API를 사용한 데이터 로드 중 오류 발생: {e}")
            raise
           
    def calculate_daily_ohlcv(self, df: DataFrame) -> StockData:
        """
        주가 데이터를 일별 OHLCV로 집계하고, 원본 데이터와 함께 반환합니다.
        
        Args:
            df (DataFrame): 원본 주가 DataFrame
            
        Returns:
            StockData: 원본 데이터와 일별 집계 데이터를 포함한 객체
        """
        try:
            print("날짜별, 종목별로 데이터를 집계하여 OHLC(시가, 고가, 저가, 종가) 계산 중...")
            
            # 일별 집계
            daily_df = df.groupBy("ticker", "date").agg(
                first("open").alias("open"),
                last("close").alias("close"),
                max("high").alias("high"),
                min("low").alias("low"),
                sum("volume").alias("volume"),
                count("*").alias("data_points")
            )
            
            # 최종 스키마에 맞게 변환
            daily_df = daily_df.select(
                "ticker",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume"
            )
            
            row_count = daily_df.count()
            if row_count == 0:
                print("집계된 데이터가 없습니다. 원본 데이터만 반환합니다.")
                return StockData(raw_data=df)
                
            print(f"일별 집계 결과 {count}개의 레코드가 생성되었습니다.")
            
            return StockData(raw_data=df, daily_data=daily_df)
            
        except Exception as e:
            print(f"일별 OHLCV 집계 중 오류 발생: {e}")
            raise       
       
    def calculate_moving_averages(self, stock_df, windows=[5, 10, 20, 50, 200]):
        """
        주가 데이터에 대한 이동평균선을 계산합니다.
        
        Args:
            stock_df: 주가 DataFrame
            windows: 이동평균선 기간 리스트
            
        Returns:
            DataFrame: 이동평균선이 추가된 DataFrame
        """
        df = stock_df
        
        for window in windows:
            window_col = f"ma_{window}"
            df = df.withColumn(
                window_col,
                avg("close").over(
                    self.ticker_window.rowsBetween(-window + 1, 0)
                )
            )
        
        return df   

    def calculate_rsi(self, df):
        """
        상대강도지수(RSI)를 계산합니다.
        
        Args:
            df: 주가 DataFrame
            
        Returns:
            DataFrame: RSI가 추가된 DataFrame
        """
        # 종가 변화량
        df = df.withColumn("prev_close", lag("close", 1).over(self.ticker_window))
        df = df.withColumn("price_change", col("close") - col("prev_close"))
        
        # 상승과 하락 구분
        df = df.withColumn("gain", expr(f"CASE WHEN price_change > 0 THEN price_change ELSE 0 END"))
        df = df.withColumn("loss", expr(f"CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END"))
        
        # RSI 기간 동안의 평균 상승/하락 계산
        df = df.withColumn(
            "avg_gain",
            avg("gain").over(self.ticker_window.rowsBetween(-self.rsi_period + 1, 0))
        )
        df = df.withColumn(
            "avg_loss",
            avg("loss").over(self.ticker_window.rowsBetween(-self.rsi_period + 1, 0))
        )
        
        # 상대강도(RS) 및 RSI 계산
        df = df.withColumn("rs", expr("avg_gain / NULLIF(avg_loss, 0)"))
        df = df.withColumn("rsi", expr("100 - (100 / (1 + rs))"))
        
        return df
    
    def calculate_macd(self, df):
        """
        MACD(이동평균수렴확산)를 계산합니다.
        
        Args:
            df: 주가 DataFrame
            
        Returns:
            DataFrame: MACD가 추가된 DataFrame
        """
        # 지수이동평균(EMA) 계산 - Spark에서는 복잡하므로 근사값으로 단순이동평균 사용
        df = df.withColumn(
            "ema_fast",
            avg("close").over(self.ticker_window.rowsBetween(-self.macd_fast_period + 1, 0))
        )
        df = df.withColumn(
            "ema_slow",
            avg("close").over(self.ticker_window.rowsBetween(-self.macd_slow_period + 1, 0))
        )
        
        # MACD 및 시그널 라인 계산
        df = df.withColumn("macd", col("ema_fast") - col("ema_slow"))
        df = df.withColumn(
            "macd_signal",
            avg("macd").over(self.ticker_window.rowsBetween(-self.macd_signal_period + 1, 0))
        )
        df = df.withColumn("macd_histogram", col("macd") - col("macd_signal"))
        
        return df
    
    def run_analysis_pipeline(self, stock_data: StockData) -> StockData:
        """
        다양한 기술적 지표를 계산합니다.
        
        Args:
            stock_data: StockData 객체
            
        Returns:
            StockData: 기술적 지표가 추가된 StockData 객체
        """
        # 일별 데이터에 대해 기술적 지표 계산
        df = stock_data.get_data('daily')
        
        # 이동평균선 계산
        df = self.calculate_moving_averages(
            df, 
            windows=[self.ma_short_period, self.ma_long_period, 50, 200]
        )
        
        # RSI 계산
        df = self.calculate_rsi(df)
        
        # MACD 계산
        df = self.calculate_macd(df)
        
        return StockData(raw_data=stock_data.raw_data, daily_data=df)
    
    def detect_crossovers(self, stock_df):
        """
        이동평균선 크로스오버 등의 신호를 감지합니다.
        
        Args:
            stock_df: 주가 DataFrame
            
        Returns:
            DataFrame: 크로스오버 신호가 추가된 DataFrame
        """
        df = stock_df
        
        # 이전 값 구하기 - lag 함수 사용 (Window 함수 중첩 방지)
        ma_short_col = f"ma_{self.ma_short_period}"
        ma_long_col = f"ma_{self.ma_long_period}"
        
        df = df.withColumn(f"prev_{ma_short_col}", lag(col(ma_short_col), 1).over(self.ticker_window))
        df = df.withColumn(f"prev_{ma_long_col}", lag(col(ma_long_col), 1).over(self.ticker_window))
        
        # 골든크로스 감지 (단기 > 장기로 교차)
        df = df.withColumn(
            "golden_cross",
            expr(f"CASE WHEN {ma_short_col} > {ma_long_col} AND prev_{ma_short_col} <= prev_{ma_long_col} THEN 1 ELSE 0 END")
        )
        
        # 데드크로스 감지 (단기 < 장기로 교차)
        df = df.withColumn(
            "death_cross",
            expr(f"CASE WHEN {ma_short_col} < {ma_long_col} AND prev_{ma_short_col} >= prev_{ma_long_col} THEN 1 ELSE 0 END")
        )
        
        # RSI 과매수/과매도 신호
        df = df.withColumn(
            "rsi_overbought",
            expr(f"CASE WHEN rsi > {self.rsi_overbought} THEN 1 ELSE 0 END")
        )
        df = df.withColumn(
            "rsi_oversold",
            expr(f"CASE WHEN rsi < {self.rsi_oversold} THEN 1 ELSE 0 END")
        )
        
        # MACD 신호선 교차 - lag 함수 사용 (Window 함수 중첩 방지)
        df = df.withColumn("prev_macd", lag(col("macd"), 1).over(self.ticker_window))
        df = df.withColumn("prev_macd_signal", lag(col("macd_signal"), 1).over(self.ticker_window))
        
        df = df.withColumn(
            "macd_bullish_cross",
            expr("CASE WHEN macd > macd_signal AND prev_macd <= prev_macd_signal THEN 1 ELSE 0 END")
        )
        df = df.withColumn(
            "macd_bearish_cross",
            expr("CASE WHEN macd < macd_signal AND prev_macd >= prev_macd_signal THEN 1 ELSE 0 END")
        )
        
        return df
    
    def identify_price_patterns(self, stock_df):
        """
        가격 차트 패턴을 식별합니다.
        
        Args:
            stock_df: 주가 DataFrame
            
        Returns:
            DataFrame: 가격 패턴 식별 결과가 추가된 DataFrame
        """
        # 이 메서드는 주식 차트 패턴 식별을 위한 복잡한 로직이 필요합니다.
        # 여기서는 기본적인 추세 식별만 구현합니다.
        df = stock_df
        
        # 10일 고점/저점
        df = df.withColumn(
            "high_10d",
            max("high").over(self.ticker_window.rowsBetween(-9, 0))
        )
        df = df.withColumn(
            "low_10d",
            min("low").over(self.ticker_window.rowsBetween(-9, 0))
        )
        
        # 추세 식별
        df = df.withColumn(
            "trend",
            expr("""
                CASE
                    WHEN close > ma_50 AND ma_50 > ma_200 THEN 'bullish'
                    WHEN close < ma_50 AND ma_50 < ma_200 THEN 'bearish'
                    ELSE 'neutral'
                END
            """)
        )
        
        return df
    
    def generate_trading_signals(self, stock_df):
        """
        거래 신호를 생성합니다.
        
        Args:
            stock_df: 주가 DataFrame
            
        Returns:
            DataFrame: 거래 신호가 추가된 DataFrame
        """
        df = stock_df
        
        # 매수 신호 생성
        df = df.withColumn(
            "buy_signal",
            expr("""
                CASE
                    WHEN golden_cross = 1 THEN 'golden_cross'
                    WHEN rsi_oversold = 1 THEN 'rsi_oversold'
                    WHEN macd_bullish_cross = 1 THEN 'macd_bullish'
                    ELSE NULL
                END
            """)
        )
        
        # 매도 신호 생성
        df = df.withColumn(
            "sell_signal",
            expr("""
                CASE
                    WHEN death_cross = 1 THEN 'death_cross'
                    WHEN rsi_overbought = 1 THEN 'rsi_overbought'
                    WHEN macd_bearish_cross = 1 THEN 'macd_bearish'
                    ELSE NULL
                END
            """)
        )
        
        # 신호 강도 계산
        df = df.withColumn(
            "signal_strength",
            expr("""
                CASE
                    WHEN (golden_cross = 1 OR death_cross = 1) AND (rsi_oversold = 1 OR rsi_overbought = 1) THEN 'strong'
                    WHEN golden_cross = 1 OR death_cross = 1 OR rsi_oversold = 1 OR rsi_overbought = 1 THEN 'medium'
                    WHEN macd_bullish_cross = 1 OR macd_bearish_cross = 1 THEN 'weak'
                    ELSE NULL
                END
            """)
        )
        
        return df
    
    def summarize_by_ticker(self, stock_df):
        """
        종목별 분석 결과를 요약합니다.
        
        Args:
            stock_df: 주가 DataFrame
            
        Returns:
            DataFrame: 종목별 요약 DataFrame
        """
        # 최근 데이터만 선택
        latest_data = stock_df.groupBy("ticker").agg(
            max("date").alias("latest_date")
        )
        
        # 두 DataFrame을 조인할 때 명시적으로 컬럼을 선택
        df = stock_df.alias("s").join(
            latest_data.alias("l"),
            (col("s.ticker") == col("l.ticker")) & (col("s.date") == col("l.latest_date")),
            "inner"
        )
        
        # 필요한 열만 선택 (중복을 피하기 위해 stock_df의 컬럼만 사용)
        summary_df = df.select(
            col("s.ticker"), col("s.date"), col("s.open"), col("s.high"), col("s.low"), 
            col("s.close"), col("s.volume"),
            col(f"s.ma_{self.ma_short_period}"), col(f"s.ma_{self.ma_long_period}"), 
            col("s.ma_50"), col("s.ma_200"),
            col("s.rsi"), col("s.macd"), col("s.macd_signal"), col("s.macd_histogram"),
            col("s.trend"), col("s.buy_signal"), col("s.sell_signal"), col("s.signal_strength")
        )
        
        # 추가 계산 - Window 함수를 사용하지 않고 직접 계산
        summary_df = summary_df.withColumn(
            "ma_50_200_diff_pct", 
            (col("ma_50") / col("ma_200") - 1.0) * 100
        )

        # RSI 값이 0인지 확인하고 필요한 경우에만 재계산
        summary_df = summary_df.withColumn(
            "rsi_status",
            expr(f"""
                CASE
                    WHEN rsi IS NULL OR rsi = 0 THEN 'unknown'
                    WHEN rsi > {self.rsi_overbought} THEN 'overbought'
                    WHEN rsi < {self.rsi_oversold} THEN 'oversold'
                    ELSE 'neutral'
                END
            """)
        )
        
        # 이동평균선 상태 추가
        summary_df = summary_df.withColumn(
            "ma_status",
            expr(f"""
                CASE
                    WHEN close > ma_{self.ma_short_period} AND ma_{self.ma_short_period} > ma_{self.ma_long_period} THEN 'bullish'
                    WHEN close < ma_{self.ma_short_period} AND ma_{self.ma_short_period} < ma_{self.ma_long_period} THEN 'bearish'
                    ELSE 'neutral'
                END
            """)
        )
        
        return summary_df
    
    def save_results(self, df, output_path=None, format="parquet"):
        """
        분석 결과를 저장합니다.
        
        Args:
            df: 저장할 DataFrame
            output_path: 저장 경로
            format: 저장 형식 (parquet, csv, json)
        """
        if not output_path:
            output_path = f"stock_analysis_results_{datetime.now().strftime('%Y%m%d')}"
            
        try:
            if format.lower() == "parquet":
                df.write.parquet(output_path, mode="overwrite")
            elif format.lower() == "csv":
                df.write.csv(output_path, mode="overwrite", header=True)
            elif format.lower() == "json":
                # JSON 형식으로 저장하되, 종목별로 구조화
                # PySpark DataFrame을 Pandas DataFrame으로 변환
                pandas_df = df.toPandas()
                
                # 종목별로 데이터 구조화
                result_dict = {}
                for _, row in pandas_df.iterrows():
                    ticker = row['ticker']
                    # 시그널 정보 추가
                    signals = []
                    
                    if row.get('buy_signal'):
                        signals.append({
                            'type': row['buy_signal'],
                            'direction': 'buy',
                            'strength': row.get('signal_strength', 'medium'),
                            'description': f"{row['buy_signal']} 매수 신호 감지"
                        })
                    
                    if row.get('sell_signal'):
                        signals.append({
                            'type': row['sell_signal'],
                            'direction': 'sell',
                            'strength': row.get('signal_strength', 'medium'),
                            'description': f"{row['sell_signal']} 매도 신호 감지"
                        })
                    
                    # 결과 딕셔너리 구성
                    ticker_dict = {
                        'ticker': ticker,
                        'date': row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date']),
                        'open': self._safe_float(row['open']),
                        'high': self._safe_float(row['high']),
                        'low': self._safe_float(row['low']),
                        'close': self._safe_float(row['close']),
                        'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
                        'ma_short': self._safe_float(row[f'ma_{self.ma_short_period}']),
                        'ma_long': self._safe_float(row[f'ma_{self.ma_long_period}']),
                        'ma_50': self._safe_float(row['ma_50']),
                        'ma_200': self._safe_float(row['ma_200']),
                        'rsi': self._safe_float(row['rsi']),
                        'macd': self._safe_float(row['macd']),
                        'macd_signal': self._safe_float(row['macd_signal']),
                        'macd_histogram': self._safe_float(row['macd_histogram']),
                        'trend': row['trend'],
                        'ma_status': row.get('ma_status', 'unknown'),
                        'rsi_status': row.get('rsi_status', 'unknown'),
                        'signals': signals
                    }
                    
                    # 변동률 계산 (첫 데이터가 아니라면)
                    if ticker in result_dict:
                        prev_close = result_dict[ticker]['close']
                        if prev_close > 0:
                            ticker_dict['change_pct'] = (ticker_dict['close'] - prev_close) / prev_close * 100
                    
                    result_dict[ticker] = ticker_dict
                
                # JSON 파일 저장
                with open(output_path, 'w') as f:
                    json.dump(result_dict, f, indent=2)
            else:
                raise ValueError(f"지원하지 않는 저장 형식: {format}")
                
            print(f"결과가 {output_path}에 {format} 형식으로 저장되었습니다.")
            return True
        except Exception as e:
            print(f"결과 저장 중 오류 발생: {e}")
            return False
    
    def save_to_postgres(self, df, db_url, db_properties, table_name, mode="append"):
        """
        분석 결과를 PostgreSQL 데이터베이스에 저장합니다.
        
        Args:
            df: 저장할 DataFrame
            db_url: 데이터베이스 URL
            db_properties: 데이터베이스 연결 속성
            table_name: 테이블 이름
            mode: 저장 모드 (append, overwrite)
        """
        try:
            df.write \
              .format("jdbc") \
              .option("url", db_url) \
              .option("dbtable", table_name) \
              .option("user", db_properties.get("user")) \
              .option("password", db_properties.get("password")) \
              .mode(mode) \
              .save()
            
            print(f"결과가 {db_url}의 {table_name} 테이블에 저장되었습니다.")
            return True
        except Exception as e:
            print(f"데이터베이스 저장 중 오류 발생: {e}")
            return False

    def _get_stock_schema(self):
        """주식 데이터의 스키마를 반환합니다."""
        return StructType([
            StructField("ticker", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("date", DateType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", IntegerType(), True)
        ])

    def _safe_float(self, value):
        """
        안전하게 float로 변환하는 유틸리티 함수
        
        Args:
            value: 변환할 값
            
        Returns:
            float: 변환된 값 또는 기본값
        """
        
        try:
            # Series인 경우 첫 번째 요소만 사용
            if isinstance(value, pd.Series):
                value = value.iloc[0]

            # 문자열로 변환해서 비교
            str_val = str(value).strip().lower()
            if value is None or pd.isna(value) or str_val in ('nan', 'none'):
                return 0.0
            
            return float(value)

        except (ValueError, TypeError) as e:
            print(f"[ERROR] Float 변환 실패: {value} (type: {type(value)}) → {e}")
            return 0.0

def parse_args():
    """명령줄 인수를 파싱합니다."""
    parser = argparse.ArgumentParser(description='주식 배치 분석 작업')
    
    parser.add_argument('--output_path', type=str, required=True,
                        help='분석 결과를 저장할 파일 경로')
    
    parser.add_argument('--start_date', type=str, required=True,
                        help='분석 시작 날짜 (YYYY-MM-DD 형식). Airflow ds 기준 2일 후부터 데이터 처리.')
    
    parser.add_argument('--end_date', type=str, required=True,
                        help='분석 종료 날짜 (YYYY-MM-DD 형식). Airflow ds 기준 3일 후까지 데이터 처리.')
    
    parser.add_argument('--tickers', type=str, default=None,
                        help='분석할 주식 종목 코드 (쉼표로 구분)')
    
    parser.add_argument('--db_url', type=str, default=None,
                        help='데이터베이스 URL 또는 데이터 파일 경로')
    
    parser.add_argument('--table_name', type=str, default='stock_prices',
                        help='데이터베이스 테이블 이름')
    
    parser.add_argument('--format', type=str, default='json',
                        choices=['json', 'parquet', 'csv'],
                        help='출력 파일 형식 (json, parquet, csv)')
    
    parser.add_argument('--data_source', type=str, default='kafka',
                        choices=['file', 'db', 'kafka'],
                        help='데이터 소스 (file, db, kafka)')
    
    parser.add_argument('--kafka_topic', type=str, default=STOCK_PRICES_TOPIC,
                        help='카프카 토픽 이름')
    
    parser.add_argument('--kafka_group', type=str, default='stock-batch-analyzer',
                        help='카프카 컨슈머 그룹 ID')
    
    parser.add_argument('--save_raw_data', type=bool, default=False,
                        help='원본 데이터 저장 여부')
    
    parser.add_argument('--save_detailed_results', type=bool, default=False,
                        help='상세 분석 결과 저장 여부')
    
    args = parser.parse_args()
    
    # Airflow의 execution_date(ds) 특성을 고려한 날짜 보정
    # - Airflow의 ds는 실제로 전날을 가리킴
    # - 데이터 수집 및 처리 시차를 고려하여 2-3일 조정
    # - 예: ds가 2024-01-01인 경우
    #   - start_date: 2024-01-03 (ds + 2일)
    #   - end_date: 2024-01-04 (ds + 3일)
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')+timedelta(days=2)
    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')+timedelta(days=3)
    
    # 종목 리스트 처리
    if args.tickers:
        args.tickers = args.tickers.split(',')
    
    return args

def main():
    """메인 함수"""
    # 명령줄 인수 파싱
    args = parse_args()
    
    # Spark 세션 생성 및 분석기 초기화
    analyzer = StockBatchAnalyzer()
    
    try:
        # 시작 메시지
        print(f"주식 배치 분석 시작: {args.start_date.strftime('%Y-%m-%d')} ~ {args.end_date.strftime('%Y-%m-%d')}")
        if args.tickers:
            print(f"분석 대상 종목: {', '.join(args.tickers)}")
        
        # 데이터 소스에 따라 데이터 로드 방식 결정
        if args.data_source == 'kafka':
            # 카프카에서 직접 데이터 로드
            stock_df = analyzer.load_data_from_kafka(
                topic=args.kafka_topic,
                group_id=args.kafka_group,
                start_time=args.start_date,
                end_time=args.end_date
            )
        else:
            # 파일이나 DB에서 데이터 로드 
            stock_df = analyzer.load_stock_data(
                db_url=args.db_url,
                db_properties=None,
                table_name=args.table_name,
                start_date=args.start_date,
                end_date=args.end_date
            )
        
        # 데이터가 비어있는지 확인
        if stock_df.count() == 0:
            print("분석할 데이터가 없습니다. 프로그램을 종료합니다.")
            return
        
        # 종목 필터링
        if args.tickers:
            stock_df = stock_df.filter(col("ticker").isin(args.tickers))
        
        # 필터링 후 데이터가 비어있는지 확인
        if stock_df.count() == 0:
            print(f"지정한 종목 {', '.join(args.tickers)}에 대한 데이터가 없습니다. 프로그램을 종료합니다.")
            return

        # 일별 OHLCV 데이터로 집계
        print("일별 OHLCV 데이터로 집계하는 중...")
        stock_data = analyzer.calculate_daily_ohlcv(stock_df)
        
        # 기술적 지표 계산 (일별 데이터 사용)
        stock_data = analyzer.run_analysis_pipeline(stock_data)
        
        # 크로스오버 감지 (일별 데이터 사용)
        stock_data.daily_data = analyzer.detect_crossovers(stock_data.daily_data)
        
        # 가격 패턴 식별 (일별 데이터 사용)
        stock_data.daily_data = analyzer.identify_price_patterns(stock_data.daily_data)
        
        # 거래 신호 생성 (일별 데이터 사용)
        stock_data.daily_data = analyzer.generate_trading_signals(stock_data.daily_data)
        
        # 종목별 요약 (일별 데이터 사용)
        summary_df = analyzer.summarize_by_ticker(stock_data.daily_data)
        
        # 결과 저장
        analyzer.save_results(summary_df, output_path=args.output_path, format=args.format)
        
        # 원본 데이터도 저장하고 싶다면:
        if args.save_raw_data:
            raw_output_path = f"{args.output_path}_raw"
            analyzer.save_results(stock_data.raw_data, output_path=raw_output_path, format=args.format)

        print(f"분석 완료. 결과가 {args.output_path}에 저장되었습니다.")
        
    except Exception as e:
        print(f"분석 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Spark 세션 종료
        if analyzer and analyzer.spark:
            analyzer.spark.stop()
            print("Spark 세션 종료")

if __name__ == "__main__":
    main() 