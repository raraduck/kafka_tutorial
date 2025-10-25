#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 배치 분석 DAG
주식 데이터를 정기적으로 분석하고 결과를 저장/보고하는 Airflow DAG입니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import sys
import json

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.insert(0, "/opt/airflow")

# 필요한 모듈 import
try:
    from notifiers.email_notifier import EmailNotifier
    from notifiers.slack_notifier import SlackNotifier
    from config.config import STOCK_TICKERS
except ImportError as e:
    print(f"모듈 가져오기 오류: {e}")
    # 모듈을 가져올 수 없는 경우 기본값 설정
    STOCK_TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]

# 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# 프로젝트 경로 설정
PROJECT_PATH = '/opt/airflow'
SPARK_APP_PATH = os.path.join(PROJECT_PATH, 'spark', 'batch', 'stock_batch_analyzer.py')
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'data', 'output')

# 결과 처리 함수
def process_analysis_results(**context):
    """분석 결과를 처리하고 요약 정보를 생성합니다."""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    
    # 분석 결과 경로
    result_path = os.path.join(OUTPUT_PATH, f"stock_analysis_{execution_date.strftime('%Y-%m-%d')}.json")
    
    try:
        with open(result_path, 'r') as f:
            results = json.load(f)
        
        # 요약 정보 생성
        summary = {
            'date': (execution_date + timedelta(days=2)).strftime('%Y-%m-%d'),
            'tickers': [],
            'alerts': []
        }
        
        for ticker, data in results.items():
            ticker_summary = {
                'ticker': ticker,
                'open': data.get('open', 0),
                'close': data.get('close', 0),
                'high': data.get('high', 0),
                'low': data.get('low', 0),
                'change_pct': data.get('change_pct', 0),
                'rsi': data.get('rsi', 0),
                'rsi_status': data.get('rsi_status', 'unknown'),
                'ma_status': data.get('ma_status', 'unknown')
            }
            summary['tickers'].append(ticker_summary)
            
            # 알림 생성
            if 'signals' in data:
                for signal in data['signals']:
                    if signal.get('strength', '') == 'strong':
                        alert = f"{ticker}: {signal.get('type', '')} 신호 감지 - {signal.get('description', '')}"
                        summary['alerts'].append(alert)
            
            # RSI 기반 알림 추가
            rsi_status = data.get('rsi_status', '')
            if rsi_status == 'overbought':
                alert = f"{ticker}: RSI 과매수 상태 (RSI: {data.get('rsi', 0):.1f}) - 매도 고려"
                summary['alerts'].append(alert)
            elif rsi_status == 'oversold':
                alert = f"{ticker}: RSI 과매도 상태 (RSI: {data.get('rsi', 0):.1f}) - 매수 고려"
                summary['alerts'].append(alert)
            
            # 이동평균선 상태 기반 알림 추가
            ma_status = data.get('ma_status', '')
            if ma_status == 'bullish':
                alert = f"{ticker}: 강세 추세 - 단기 이동평균선이 장기 이동평균선 위에 위치"
                summary['alerts'].append(alert)
            elif ma_status == 'bearish':
                alert = f"{ticker}: 약세 추세 - 단기 이동평균선이 장기 이동평균선 아래에 위치"
                summary['alerts'].append(alert)
        
        # 결과 저장
        task_instance.xcom_push(key='analysis_summary', value=summary)
        return summary
    
    except Exception as e:
        print(f"결과 처리 중 오류 발생: {e}")
        raise

def send_analysis_report(**context):
    """분석 결과 보고서를 이메일과 Slack으로 전송합니다."""
    task_instance = context['task_instance']
    summary = task_instance.xcom_pull(task_ids='process_results', key='analysis_summary')
    
    if not summary:
        print("분석 요약 정보가 없습니다.")
        return
    
    try:
        # 이메일 알림 전송
        email_notifier = EmailNotifier()
        email_result = email_notifier.send_daily_summary(summary)
        
        # Slack 알림 전송
        slack_notifier = SlackNotifier()
        slack_result = slack_notifier.send_daily_summary(summary)
        
        print(f"알림 전송 결과 - 이메일: {email_result}, Slack: {slack_result}")
        return {
            'email_sent': email_result,
            'slack_sent': slack_result
        }
    except Exception as e:
        print(f"알림 전송 중 오류 발생: {e}")
        return {
            'email_sent': False,
            'slack_sent': False,
            'error': str(e)
        }

# DAG 정의
with DAG(
    'stock_batch_analyzer',
    default_args=default_args,
    description='주식 데이터 배치 분석 DAG',
    schedule_interval='0 18 * * 1-5',  # 주중 오후 6시 실행
    catchup=False,
    tags=['stock', 'analysis', 'batch'],
) as dag:
    
    # 출력 디렉토리 생성
    create_output_dir = BashOperator(
        task_id='create_output_dir',
        bash_command=f'mkdir -p {OUTPUT_PATH}',
    )
    
    # Spark 작업 제출 (BashOperator로 실행)
    analyze_stocks = BashOperator(
        task_id='analyze_stocks',
        bash_command=(
            "docker exec spark-master "
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--name StockBatchAnalyzer "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.kafka:kafka-clients:2.6.0 "
            "/opt/bitnami/spark/work/spark/batch/stock_batch_analyzer.py "
            "--output_path /opt/airflow/data/output/stock_analysis_{{ ds }}.json "
            "--start_date {{ ds }} --end_date {{ ds }} --tickers {{ params.tickers }}"
        ),
        params={'tickers': ','.join(STOCK_TICKERS)},
    )
    
    # 결과 처리
    process_results = PythonOperator(
        task_id='process_results',
        python_callable=process_analysis_results,
        provide_context=True,
    )
    
    # 보고서 전송
    send_report = PythonOperator(
        task_id='send_report',
        python_callable=send_analysis_report,
        provide_context=True,
    )
    
    # 작업 의존성 설정
    create_output_dir >> analyze_stocks >> process_results >> send_report
