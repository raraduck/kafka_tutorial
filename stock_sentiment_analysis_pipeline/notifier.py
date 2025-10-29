#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
알림 처리기
이메일과 슬랙을 통해 알림을 전송합니다.
"""

import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import sys
import os
import json

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config import EMAIL_CONFIG, SLACK_CONFIG
from utils.logging_utils import setup_logger
from utils.kafka_utils import KafkaHelper

# 로거 설정
logger = setup_logger(__name__, 'notifier.log')

class Notifier:
    """이메일과 슬랙을 통한 알림 처리기"""
    
    def __init__(self):
        """알림 처리기 초기화"""
        self.email_config = EMAIL_CONFIG
        self.slack_config = SLACK_CONFIG
        
        # Kafka 헬퍼 초기화
        self.kafka = KafkaHelper()
        
        # 최근 전송된 알림을 추적하여 중복 방지
        self.recent_alerts = {}
    
    def send_email(self, subject, message, severity="info"):
        """
        이메일 알림 전송
        
        Args:
            subject (str): 이메일 제목
            message (str): 이메일 내용
            severity (str): 알림 심각도 (info, warning, critical)
        
        Returns:
            bool: 성공 여부
        """
        if not self.email_config['username'] or not self.email_config['password']:
            logger.warning("이메일 계정 정보가 설정되지 않았습니다.")
            return False
        
        try:
            # 이메일 메시지 생성
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender']
            msg['To'] = ", ".join(self.email_config['recipients'])
            msg['Subject'] = f"{self.email_config['subject_prefix']} {subject}"
            
            # HTML 형식 메시지 본문
            html_style = """
            <style>
                .container { font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #2c3e50; color: white; padding: 10px; text-align: center; }
                .content { padding: 20px; border: 1px solid #ddd; }
                .footer { font-size: 12px; color: #777; text-align: center; margin-top: 20px; }
                .info { border-left: 4px solid #3498db; }
                .warning { border-left: 4px solid #f39c12; }
                .critical { border-left: 4px solid #e74c3c; }
            </style>
            """
            
            html_body = f"""
            <div class="container">
                <div class="header">
                    <h2>주식 알림 시스템</h2>
                </div>
                <div class="content {severity}">
                    <h3>{subject}</h3>
                    <p>{message}</p>
                    <p>시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                <div class="footer">
                    <p>이 메일은 자동으로 생성되었습니다. 회신하지 마세요.</p>
                </div>
            </div>
            """
            
            # HTML 형식 메시지 첨부
            html_part = MIMEText(f"{html_style}\n{html_body}", 'html')
            msg.attach(html_part)
            
            # SMTP 서버 연결 및 인증
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            
            # 이메일 전송
            server.send_message(msg)
            server.quit()
            
            logger.info(f"이메일 알림 전송 성공: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 알림 전송 실패: {e}")
            return False
    
    def send_slack(self, message, severity="info"):
        """
        슬랙 알림 전송
        
        Args:
            message (str): 슬랙 메시지
            severity (str): 알림 심각도 (info, warning, critical)
        
        Returns:
            bool: 성공 여부
        """
        if not self.slack_config['webhook_url']:
            logger.warning("슬랙 웹훅 URL이 설정되지 않았습니다.")
            return False
        
        try:
            # 심각도에 따른 색상 설정
            color = "#3498db"  # 기본 파란색 (info)
            if severity == "warning":
                color = "#f39c12"  # 노란색
            elif severity == "critical" or severity == "high":
                color = "#e74c3c"  # 빨간색
            
            # 슬랙 메시지 페이로드 구성
            payload = {
                "channel": self.slack_config['channel'],
                "username": self.slack_config['username'],
                "icon_emoji": self.slack_config['icon_emoji'],
                "attachments": [
                    {
                        "color": color,
                        "pretext": "주식 알림 시스템",
                        "text": message,
                        "footer": f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    }
                ]
            }
            
            # 슬랙 웹훅 요청
            response = requests.post(
                self.slack_config['webhook_url'],
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"슬랙 알림 전송 성공")
                return True
            else:
                logger.error(f"슬랙 알림 전송 실패: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"슬랙 알림 전송 실패: {e}")
            return False
    
    def process_alert(self, alert):
        """
        알림 처리 및 전송
        
        Args:
            alert (dict): 알림 데이터
        """
        # 알림 타입과 메시지 추출
        alert_type = alert.get('type', 'unknown')
        message = alert.get('message', '알림 메시지 없음')
        severity = alert.get('severity', 'info')
        
        # 알림 중복 검사 (최근 5분 이내 동일 메시지 방지)
        alert_key = f"{alert_type}:{message}"
        current_time = datetime.now()
        
        if alert_key in self.recent_alerts:
            last_sent = self.recent_alerts[alert_key]
            time_diff = (current_time - last_sent).total_seconds()
            
            if time_diff < 300:  # 5분(300초) 내 중복 방지
                logger.info(f"최근에 전송된 동일 알림 무시: {alert_key}")
                return
        
        # 알림 타입에 따른 제목 설정
        subject = "주식 알림"
        if alert_type == 'price_surge':
            subject = "주가 급등 알림"
        elif alert_type == 'price_drop':
            subject = "주가 급락 알림"
        elif alert_type == 'volume_surge':
            subject = "거래량 급증 알림"
        elif alert_type == 'sentiment_alert':
            subject = "감성 분석 알림"
        
        # 이메일 발송
        self.send_email(subject, message, severity)
        
        # 슬랙 발송
        self.send_slack(message, severity)
        
        # 최근 알림 시간 업데이트
        self.recent_alerts[alert_key] = current_time
        
        logger.info(f"알림 처리 완료: {alert_type} - {message}")
    
    def run(self):
        """알림 처리 메인 루프"""
        logger.info("알림 처리기 시작")
        
        # Kafka 토픽 확인
        self.kafka.ensure_topics_exist()
        
        # alerts 토픽 컨슈머 생성
        if not self.kafka.create_consumer(self.kafka.config['topics']['alerts']):
            logger.error("Kafka 컨슈머 생성 실패. 프로그램을 종료합니다.")
            return
        
        try:
            for message in self.kafka.consumer:
                alert = message.value
                self.process_alert(alert)
                
        except KeyboardInterrupt:
            logger.info("알림 처리기 중지 요청")
        finally:
            # 리소스 정리
            self.kafka.close()
            logger.info("알림 처리기 종료")

if __name__ == "__main__":
    notifier = Notifier()
    notifier.run() 