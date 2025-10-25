#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
이메일 알림 모듈
SMTP를 사용하여 주식 가격 알림을 이메일로 전송합니다.
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import os
from datetime import datetime

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log_utils import setup_logger
from config.config import EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT

# 로깅 설정
logger = setup_logger("email_notifier", "logs/email_notifier.log")

class EmailNotifier:
    """이메일 알림 클래스"""
    
    def __init__(self, sender=None, password=None, recipient=None, smtp_server=None, smtp_port=None):
        """초기화 함수"""
        self.sender = sender or EMAIL_SENDER
        self.password = password or EMAIL_PASSWORD
        self.recipient = recipient or EMAIL_RECIPIENT
        self.smtp_server = smtp_server or EMAIL_SMTP_SERVER or "smtp.gmail.com"
        self.smtp_port = smtp_port or EMAIL_SMTP_PORT or 587
        
        if not self.sender or not self.password or not self.recipient:
            logger.warning("이메일 설정이 완료되지 않았습니다. 이메일 알림이 비활성화됩니다.")
        else:
            logger.info(f"이메일 알림 초기화 완료. 발신자: {self.sender}, 수신자: {self.recipient}")

    def send_email(self, subject, html_content, text_content=None):
        """이메일 전송"""
        if not self.sender or not self.password or not self.recipient:
            logger.warning("이메일 설정이 완료되지 않아 이메일을 전송할 수 없습니다.")
            return False
        
        # 수신자가 문자열이면 리스트로 변환
        recipients = self.recipient if isinstance(self.recipient, list) else [self.recipient]
        
        # 이메일 메시지 생성
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.sender
        message["To"] = ", ".join(recipients)
        
        # 일반 텍스트와 HTML 버전의 이메일 내용 추가
        if text_content:
            message.attach(MIMEText(text_content, "plain"))
        message.attach(MIMEText(html_content, "html"))
        
        try:
            # SMTP 서버 연결 및 로그인
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(self.sender, self.password)
                
                # 이메일 전송
                server.sendmail(self.sender, recipients, message.as_string())
                
            logger.info(f"이메일 전송 성공: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 중 오류 발생: {e}")
            return False
    
    def send_price_alert(self, ticker, price, change_pct, message):
        """주식 가격 알림 이메일 전송"""
        # 변동 방향에 따른 이모지와 색상 설정
        emoji = "🚀" if change_pct > 0 else "📉"
        color = "#28a745" if change_pct > 0 else "#dc3545"
        
        # 이메일 제목
        subject = f"{emoji} {ticker} 가격 알림: {change_pct:.2f}%"
        
        # HTML 이메일 내용
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                    .header {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; text-align: center; }}
                    .content {{ padding: 20px 0; }}
                    .price-box {{ padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px; }}
                    .price-change {{ color: {color}; font-weight: bold; }}
                    .footer {{ font-size: 12px; color: #6c757d; border-top: 1px solid #e9ecef; padding-top: 10px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h2>{emoji} {ticker} 가격 알림</h2>
                    </div>
                    <div class="content">
                        <div class="price-box">
                            <p><strong>현재가:</strong> ${price:.2f}</p>
                            <p><strong>변동률:</strong> <span class="price-change">{change_pct:.2f}%</span></p>
                        </div>
                        <p>{message}</p>
                    </div>
                    <div class="footer">
                        <p>이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.</p>
                        <p>스톡 얼럿 파이프라인 - 자동 생성 메일입니다.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # 일반 텍스트 이메일 내용
        text_content = f"""
        {ticker} 가격 알림
        
        현재가: ${price:.2f}
        변동률: {change_pct:.2f}%
        
        {message}
        
        이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.
        스톡 얼럿 파이프라인 - 자동 생성 메일입니다.
        """
        
        return self.send_email(subject, html_content, text_content)
    
    def send_volume_alert(self, ticker, volume, avg_volume, pct_change, message):
        """거래량 알림 이메일 전송"""
        subject = f"📊 {ticker} 거래량 알림: {pct_change:.2f}% 증가"
        
        # HTML 이메일 내용
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                    .header {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; text-align: center; }}
                    .content {{ padding: 20px 0; }}
                    .volume-box {{ padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px; }}
                    .volume-change {{ color: #007bff; font-weight: bold; }}
                    .footer {{ font-size: 12px; color: #6c757d; border-top: 1px solid #e9ecef; padding-top: 10px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h2>📊 {ticker} 거래량 알림</h2>
                    </div>
                    <div class="content">
                        <div class="volume-box">
                            <p><strong>현재 거래량:</strong> {volume:,}</p>
                            <p><strong>평균 거래량:</strong> {avg_volume:,}</p>
                            <p><strong>변동률:</strong> <span class="volume-change">{pct_change:.2f}%</span></p>
                        </div>
                        <p>{message}</p>
                    </div>
                    <div class="footer">
                        <p>이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.</p>
                        <p>스톡 얼럿 파이프라인 - 자동 생성 메일입니다.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # 일반 텍스트 이메일 내용
        text_content = f"""
        {ticker} 거래량 알림
        
        현재 거래량: {volume:,}
        평균 거래량: {avg_volume:,}
        변동률: {pct_change:.2f}%
        
        {message}
        
        이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.
        스톡 얼럿 파이프라인 - 자동 생성 메일입니다.
        """
        
        return self.send_email(subject, html_content, text_content)
    
    def send_daily_summary(self, summary_data):
        """일일 요약 보고서 이메일 전송"""
        subject = f"📅 주식 일일 요약 보고서: {summary_data['date']}"
        
        # HTML 테이블 생성
        ticker_rows = ""
        for ticker_data in summary_data['tickers']:
            ticker = ticker_data['ticker']
            change_pct = ticker_data['change_pct']
            color = "#28a745" if change_pct > 0 else "#dc3545" if change_pct < 0 else "#6c757d"
            emoji = "🚀" if change_pct > 0 else "📉" if change_pct < 0 else "➖"
            
            # RSI 상태 이모지
            rsi_value = ticker_data.get('rsi', 0)
            rsi_status = ticker_data.get('rsi_status', 'unknown')
            rsi_emoji = "⚠️" if rsi_status == 'overbought' else "✅" if rsi_status == 'oversold' else "➖"
            
            # 이동평균선 상태 이모지
            ma_status = ticker_data.get('ma_status', 'unknown')
            ma_emoji = "📈" if ma_status == 'bullish' else "📉" if ma_status == 'bearish' else "➖"
            
            ticker_rows += f"""
            <tr>
                <td>{ticker}</td>
                <td>${ticker_data['open']:.2f}</td>
                <td>${ticker_data['close']:.2f}</td>
                <td>${ticker_data['high']:.2f}</td>
                <td>${ticker_data['low']:.2f}</td>
                <td style="color: {color};">{emoji} {change_pct:.2f}%</td>
                <td>{rsi_emoji} {rsi_value:.1f}</td>
                <td>{ma_emoji} {ma_status}</td>
            </tr>
            """
        
        # 알림 목록 생성
        alerts_html = ""
        if summary_data.get('alerts'):
            alerts_html = "<h3>오늘의 주요 알림</h3><ul>"
            for alert in summary_data['alerts']:
                alerts_html += f"<li>{alert}</li>"
            alerts_html += "</ul>"
        
        # HTML 이메일 내용
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
                    .header {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; text-align: center; }}
                    .content {{ padding: 20px 0; }}
                    table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                    th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #e9ecef; }}
                    th {{ background-color: #f8f9fa; }}
                    .footer {{ font-size: 12px; color: #6c757d; border-top: 1px solid #e9ecef; padding-top: 10px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h2>📅 주식 일일 요약 보고서</h2>
                        <p>{summary_data['date']} 거래일</p>
                    </div>
                    <div class="content">
                        <h3>종목별 요약</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>종목</th>
                                    <th>시가</th>
                                    <th>종가</th>
                                    <th>고가</th>
                                    <th>저가</th>
                                    <th>변동률</th>
                                    <th>RSI</th>
                                    <th>MA 상태</th>
                                </tr>
                            </thead>
                            <tbody>
                                {ticker_rows}
                            </tbody>
                        </table>
                        {alerts_html}
                    </div>
                    <div class="footer">
                        <p>이 보고서는 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.</p>
                        <p><strong>범례:</strong> RSI ⚠️ 과매수 / ✅ 과매도 | MA 📈 강세 / 📉 약세</p>
                        <p>스톡 얼럿 파이프라인 - 자동 생성 메일입니다.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # 일반 텍스트 이메일 내용
        text_content = f"""
        주식 일일 요약 보고서: {summary_data['date']}
        
        종목별 요약:
        """
        
        for ticker_data in summary_data['tickers']:
            ticker = ticker_data['ticker']
            change_pct = ticker_data['change_pct']
            emoji = "↑" if change_pct > 0 else "↓" if change_pct < 0 else "-"
            
            text_content += f"""
        {ticker}:
        - 시가: ${ticker_data['open']:.2f}
        - 종가: ${ticker_data['close']:.2f}
        - 고가: ${ticker_data['high']:.2f}
        - 저가: ${ticker_data['low']:.2f}
        - 변동률: {emoji} {change_pct:.2f}%
            """
        
        if summary_data.get('alerts'):
            text_content += "\n\n오늘의 주요 알림:\n"
            for alert in summary_data['alerts']:
                text_content += f"- {alert}\n"
        
        text_content += f"""
        이 보고서는 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.
        스톡 얼럿 파이프라인 - 자동 생성 메일입니다.
        """
        
        return self.send_email(subject, html_content, text_content)

# 테스트 코드
if __name__ == "__main__":
    from datetime import datetime
    
    notifier = EmailNotifier()
    
    # 테스트 가격 알림
    notifier.send_price_alert("AAPL", 175.25, 2.5, "애플 주가가 2% 이상 상승했습니다.")
    
    # 테스트 거래량 알림
    notifier.send_volume_alert("AAPL", 80000000, 65000000, 23.08, "애플 거래량이 평균보다 20% 이상 증가했습니다.")
    
    # 테스트 일일 요약
    summary_data = {
        "date": "2025-05-01",
        "tickers": [
            {
                "ticker": "AAPL",
                "open": 170.25,
                "close": 175.25,
                "high": 176.30,
                "low": 169.50,
                "change_pct": 2.5
            },
            {
                "ticker": "MSFT",
                "open": 350.10,
                "close": 345.75,
                "high": 352.20,
                "low": 344.80,
                "change_pct": -1.2
            }
        ],
        "alerts": [
            "AAPL 주가 2.5% 상승",
            "MSFT 주가 1.2% 하락",
            "AAPL 거래량 23% 증가"
        ]
    }
    notifier.send_daily_summary(summary_data) 