#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로깅 유틸리티 모듈
애플리케이션 전체에서 일관된 로깅 설정을 제공합니다.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import datetime

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import LOG_LEVEL

# 로그 디렉토리 설정
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 로그 레벨 매핑
LOG_LEVEL_MAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

def get_log_level():
    """환경 변수에서 로그 레벨을 가져옵니다."""
    return LOG_LEVEL_MAP.get(LOG_LEVEL, logging.INFO)

def setup_logger(name, log_file=None, level=None, console_output=True, file_output=True,
                 max_bytes=10485760, backup_count=5):
    """
    로거 설정
    
    Args:
        name (str): 로거 이름
        log_file (str, optional): 로그 파일 경로. 기본값은 None으로, 이 경우 name.log 파일이 생성됩니다.
        level (int, optional): 로그 레벨. 기본값은 None으로, 이 경우 환경 변수에서 로그 레벨을 가져옵니다.
        console_output (bool, optional): 콘솔 출력 여부
        file_output (bool, optional): 파일 출력 여부
        max_bytes (int, optional): 로그 파일 최대 크기 (바이트)
        backup_count (int, optional): 백업 파일 수
    
    Returns:
        logging.Logger: 설정된 로거
    """
    if level is None:
        level = get_log_level()
    
    if log_file is None:
        log_file = os.path.join(LOG_DIR, f"{name}.log")
    
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 이미 핸들러가 설정되어 있으면 추가하지 않음
    if logger.handlers:
        return logger
    
    # 로그 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 출력 설정
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 파일 출력 설정
    if file_output:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def setup_daily_logger(name, log_dir=None, level=None, console_output=True, file_output=True,
                      backup_count=30):
    """
    일별 로그 파일을 생성하는 로거 설정
    
    Args:
        name (str): 로거 이름
        log_dir (str, optional): 로그 디렉토리 경로. 기본값은 None으로, 이 경우 기본 로그 디렉토리가 사용됩니다.
        level (int, optional): 로그 레벨. 기본값은 None으로, 이 경우 환경 변수에서 로그 레벨을 가져옵니다.
        console_output (bool, optional): 콘솔 출력 여부
        file_output (bool, optional): 파일 출력 여부
        backup_count (int, optional): 백업 파일 수
    
    Returns:
        logging.Logger: 설정된 로거
    """
    if level is None:
        level = get_log_level()
    
    if log_dir is None:
        log_dir = LOG_DIR
    
    # 로그 파일 경로 설정
    log_file = os.path.join(log_dir, f"{name}.log")
    
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 이미 핸들러가 설정되어 있으면 추가하지 않음
    if logger.handlers:
        return logger
    
    # 로그 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 출력 설정
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 파일 출력 설정 (일별 로그 파일)
    if file_output:
        file_handler = TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.suffix = "%Y-%m-%d"
        logger.addHandler(file_handler)
    
    return logger

def log_function_call(logger, func_name, args=None, kwargs=None):
    """
    함수 호출 로깅
    
    Args:
        logger (logging.Logger): 로거
        func_name (str): 함수 이름
        args (tuple, optional): 함수 인자
        kwargs (dict, optional): 함수 키워드 인자
    """
    args_str = str(args) if args else "()"
    kwargs_str = str(kwargs) if kwargs else "{}"
    logger.debug(f"함수 호출: {func_name}, 인자: {args_str}, 키워드 인자: {kwargs_str}")

def log_function_result(logger, func_name, result, execution_time=None):
    """
    함수 결과 로깅
    
    Args:
        logger (logging.Logger): 로거
        func_name (str): 함수 이름
        result: 함수 결과
        execution_time (float, optional): 실행 시간 (초)
    """
    time_str = f", 실행 시간: {execution_time:.4f}초" if execution_time is not None else ""
    logger.debug(f"함수 결과: {func_name}{time_str}, 결과: {result}")

def log_error(logger, func_name, error, traceback_info=None):
    """
    오류 로깅
    
    Args:
        logger (logging.Logger): 로거
        func_name (str): 함수 이름
        error (Exception): 오류
        traceback_info (str, optional): 트레이스백 정보
    """
    error_msg = f"오류 발생: {func_name}, 오류: {error}"
    if traceback_info:
        error_msg += f"\n트레이스백: {traceback_info}"
    logger.error(error_msg)

def log_api_request(logger, endpoint, method, params=None, headers=None):
    """
    API 요청 로깅
    
    Args:
        logger (logging.Logger): 로거
        endpoint (str): API 엔드포인트
        method (str): HTTP 메서드
        params (dict, optional): 요청 파라미터
        headers (dict, optional): 요청 헤더
    """
    params_str = str(params) if params else "{}"
    headers_str = str(headers) if headers else "{}"
    logger.debug(f"API 요청: {method} {endpoint}, 파라미터: {params_str}, 헤더: {headers_str}")

def log_api_response(logger, endpoint, status_code, response_data=None, execution_time=None):
    """
    API 응답 로깅
    
    Args:
        logger (logging.Logger): 로거
        endpoint (str): API 엔드포인트
        status_code (int): HTTP 상태 코드
        response_data: 응답 데이터
        execution_time (float, optional): 실행 시간 (초)
    """
    time_str = f", 실행 시간: {execution_time:.4f}초" if execution_time is not None else ""
    logger.debug(f"API 응답: {endpoint}, 상태 코드: {status_code}{time_str}, 응답: {response_data}")

def log_kafka_message(logger, topic, message, partition=None, offset=None):
    """
    Kafka 메시지 로깅
    
    Args:
        logger (logging.Logger): 로거
        topic (str): Kafka 토픽
        message: 메시지 내용
        partition (int, optional): 파티션
        offset (int, optional): 오프셋
    """
    partition_str = f", 파티션: {partition}" if partition is not None else ""
    offset_str = f", 오프셋: {offset}" if offset is not None else ""
    logger.debug(f"Kafka 메시지: {topic}{partition_str}{offset_str}, 메시지: {message}")

def log_db_query(logger, query, params=None, execution_time=None):
    """
    데이터베이스 쿼리 로깅
    
    Args:
        logger (logging.Logger): 로거
        query (str): SQL 쿼리
        params (tuple, optional): 쿼리 파라미터
        execution_time (float, optional): 실행 시간 (초)
    """
    params_str = str(params) if params else "()"
    time_str = f", 실행 시간: {execution_time:.4f}초" if execution_time is not None else ""
    logger.debug(f"DB 쿼리: {query}, 파라미터: {params_str}{time_str}")

def log_db_result(logger, query, result, execution_time=None):
    """
    데이터베이스 쿼리 결과 로깅
    
    Args:
        logger (logging.Logger): 로거
        query (str): SQL 쿼리
        result: 쿼리 결과
        execution_time (float, optional): 실행 시간 (초)
    """
    time_str = f", 실행 시간: {execution_time:.4f}초" if execution_time is not None else ""
    logger.debug(f"DB 결과: {query}{time_str}, 결과: {result}")

def function_logger(logger):
    """
    함수 로깅 데코레이터
    
    Args:
        logger (logging.Logger): 로거
    
    Returns:
        function: 데코레이터 함수
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.datetime.now()
            log_function_call(logger, func.__name__, args, kwargs)
            try:
                result = func(*args, **kwargs)
                execution_time = (datetime.datetime.now() - start_time).total_seconds()
                log_function_result(logger, func.__name__, result, execution_time)
                return result
            except Exception as e:
                import traceback
                log_error(logger, func.__name__, e, traceback.format_exc())
                raise
        return wrapper
    return decorator

# 테스트 코드
if __name__ == "__main__":
    # 기본 로거 설정
    logger = setup_logger("test_logger")
    logger.debug("디버그 메시지")
    logger.info("정보 메시지")
    logger.warning("경고 메시지")
    logger.error("오류 메시지")
    logger.critical("심각한 오류 메시지")
    
    # 일별 로거 설정
    daily_logger = setup_daily_logger("daily_test_logger")
    daily_logger.info("일별 로거 테스트 메시지")
    
    # 함수 로깅 데코레이터 테스트
    @function_logger(logger)
    def test_function(a, b, c=None):
        logger.info(f"테스트 함수 실행 중: {a}, {b}, {c}")
        return a + b
    
    result = test_function(1, 2, c=3)
    print(f"함수 결과: {result}")
    
    # API 로깅 테스트
    log_api_request(logger, "https://api.example.com/data", "GET", {"param1": "value1"})
    log_api_response(logger, "https://api.example.com/data", 200, {"data": "response"}, 0.5)
    
    # Kafka 로깅 테스트
    log_kafka_message(logger, "test-topic", {"key": "value"}, 0, 100)
    
    # DB 로깅 테스트
    log_db_query(logger, "SELECT * FROM table WHERE id = %s", (1,), 0.1)
    log_db_result(logger, "SELECT * FROM table", [{"id": 1, "name": "test"}], 0.1)