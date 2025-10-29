#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 알림 대시보드
PostgreSQL 데이터베이스에서 데이터를 가져와 시각화합니다.
"""

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_helper import DatabaseHelper
from config.config import MONITORED_STOCKS
from utils.logging_utils import setup_logger

# 로거 설정
logger = setup_logger(__name__, 'dashboard.log')

# 데이터베이스 헬퍼 초기화
db = DatabaseHelper()

# 대시보드 애플리케이션 초기화
app = dash.Dash(__name__, title='주식 알림 대시보드')

# 대시보드 레이아웃 정의
app.layout = html.Div([
    html.H1('주식 알림 대시보드'),
    
    # 티커 선택 드롭다운
    html.Div([
        html.Label('종목 선택:'),
        dcc.Dropdown(
            id='ticker-dropdown',
            options=[{'label': ticker, 'value': ticker} for ticker in MONITORED_STOCKS],
            value=MONITORED_STOCKS[0] if MONITORED_STOCKS else None
        )
    ], style={'width': '300px', 'margin': '10px'}),
    
    # 기간 선택 라디오 버튼
    html.Div([
        html.Label('기간 선택:'),
        dcc.RadioItems(
            id='period-radio',
            options=[
                {'label': '1일', 'value': '1D'},
                {'label': '1주일', 'value': '1W'},
                {'label': '1개월', 'value': '1M'},
                {'label': '3개월', 'value': '3M'}
            ],
            value='1W'
        )
    ], style={'margin': '10px'}),
    
    # 주가 차트
    html.Div([
        html.H2('주가 변동'),
        dcc.Graph(id='price-chart')
    ], style={'margin': '20px 0'}),
    
    # 이벤트 타임라인
    html.Div([
        html.H2('주요 이벤트'),
        html.Div(id='events-timeline')
    ], style={'margin': '20px 0'}),
    
    # 감성 분석 결과
    html.Div([
        html.H2('감성 분석 결과'),
        dcc.Graph(id='sentiment-chart')
    ], style={'margin': '20px 0'}),
    
    # 최근 뉴스 및 감성
    html.Div([
        html.H2('최근 뉴스 및 감성'),
        html.Div(id='recent-news')
    ], style={'margin': '20px 0'}),
    
    # 자동 새로고침 간격
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # 1분마다 업데이트
        n_intervals=0
    )
])

# 시작 날짜 계산 함수
def get_start_date(period):
    """기간에 따른 시작 날짜 계산"""
    now = datetime.now()
    if period == '1D':
        return now - timedelta(days=1)
    elif period == '1W':
        return now - timedelta(weeks=1)
    elif period == '1M':
        return now - timedelta(days=30)
    elif period == '3M':
        return now - timedelta(days=90)
    else:
        return now - timedelta(weeks=1)

# 콜백: 주가 차트 업데이트
@app.callback(
    Output('price-chart', 'figure'),
    [Input('ticker-dropdown', 'value'),
     Input('period-radio', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_price_chart(ticker, period, n_intervals):
    """주가 차트 업데이트"""
    if not ticker:
        return {
            'data': [],
            'layout': go.Layout(title='종목을 선택해주세요')
        }
    
    # 시작 날짜 계산
    start_date = get_start_date(period)
    
    # 최근 주가 데이터 가져오기
    query = """
    SELECT ticker, timestamp, price, volume
    FROM stock_prices
    WHERE ticker = %s AND timestamp >= %s
    ORDER BY timestamp
    """
    
    stock_prices = db.fetch_all(query, (ticker, start_date))
    
    if not stock_prices:
        return {
            'data': [],
            'layout': go.Layout(title=f'{ticker} 주가 데이터 없음')
        }
    
    # 데이터프레임 생성
    df = pd.DataFrame(stock_prices)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['price'] = df['price'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    # 주가 차트
    price_trace = go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        name='주가',
        line={'color': 'blue'}
    )
    
    # 거래량 바 차트
    volume_trace = go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='거래량',
        marker={'color': 'lightgray'},
        opacity=0.5,
        yaxis='y2'
    )
    
    # 이벤트 데이터 가져오기
    query = """
    SELECT ticker, event_type, timestamp, data
    FROM events
    WHERE ticker = %s AND timestamp >= %s
    ORDER BY timestamp
    """
    
    events = db.fetch_all(query, (ticker, start_date))
    event_traces = []
    
    for event in events:
        event_time = pd.to_datetime(event['timestamp'])
        event_type = event['event_type']
        
        # 이벤트 유형에 따른 색상과 마커 설정
        color = 'red'
        symbol = 'circle'
        
        if event_type == 'price_surge':
            color = 'green'
            symbol = 'triangle-up'
        elif event_type == 'price_drop':
            color = 'red'
            symbol = 'triangle-down'
        elif event_type == 'volume_surge':
            color = 'orange'
            symbol = 'star'
        
        # 이벤트 시간에 해당하는 가격 찾기
        price_at_event = df[df['timestamp'] <= event_time]['price'].iloc[-1] if not df[df['timestamp'] <= event_time].empty else None
        
        if price_at_event is not None:
            # 이벤트 마커 추가
            event_traces.append(go.Scatter(
                x=[event_time],
                y=[price_at_event],
                mode='markers',
                marker=dict(
                    size=12,
                    color=color,
                    symbol=symbol
                ),
                name=event_type,
                hoverinfo='text',
                text=f"{event_type} - {event_time.strftime('%Y-%m-%d %H:%M')}"
            ))
    
    # 차트 레이아웃
    layout = go.Layout(
        title=f'{ticker} 주가 및 거래량',
        xaxis={'title': '시간'},
        yaxis={'title': '가격 ($)'},
        yaxis2={
            'title': '거래량',
            'overlaying': 'y',
            'side': 'right'
        },
        legend={'orientation': 'h', 'y': 1.1},
        height=500
    )
    
    return {
        'data': [price_trace, volume_trace] + event_traces,
        'layout': layout
    }

# 콜백: 이벤트 타임라인 업데이트
@app.callback(
    Output('events-timeline', 'children'),
    [Input('ticker-dropdown', 'value'),
     Input('period-radio', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_events_timeline(ticker, period, n_intervals):
    """이벤트 타임라인 업데이트"""
    if not ticker:
        return html.P('종목을 선택해주세요')
    
    # 시작 날짜 계산
    start_date = get_start_date(period)
    
    # 이벤트 데이터 가져오기
    query = """
    SELECT ticker, event_type, timestamp, data
    FROM events
    WHERE ticker = %s AND timestamp >= %s
    ORDER BY timestamp DESC
    """
    
    events = db.fetch_all(query, (ticker, start_date))
    
    if not events:
        return html.P(f'{ticker}에 대한 이벤트가 없습니다.')
    
    # 이벤트 목록 생성
    event_items = []
    for event in events:
        event_time = pd.to_datetime(event['timestamp'])
        event_type = event['event_type']
        data = event['data']
        
        # 이벤트 유형에 따른 스타일 설정
        style = {'padding': '10px', 'margin': '5px 0', 'border-left': '4px solid'}
        title = event_type
        
        if event_type == 'price_surge':
            style['border-left-color'] = 'green'
            title = '주가 급등'
        elif event_type == 'price_drop':
            style['border-left-color'] = 'red'
            title = '주가 급락'
        elif event_type == 'volume_surge':
            style['border-left-color'] = 'orange'
            title = '거래량 급증'
        
        # 데이터 파싱
        try:
            data_dict = data if isinstance(data, dict) else eval(data)
            details = []
            for key, value in data_dict.items():
                if key not in ['type', 'ticker', 'timestamp']:
                    details.append(f"{key}: {value}")
            details_text = ', '.join(details)
        except:
            details_text = str(data)
        
        # 이벤트 항목 생성
        event_items.append(html.Div([
            html.H4(title),
            html.P(f"시간: {event_time.strftime('%Y-%m-%d %H:%M')}"),
            html.P(f"상세 정보: {details_text}")
        ], style=style))
    
    return html.Div(event_items)

# 콜백: 감성 분석 차트 업데이트
@app.callback(
    Output('sentiment-chart', 'figure'),
    [Input('ticker-dropdown', 'value'),
     Input('period-radio', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_sentiment_chart(ticker, period, n_intervals):
    """감성 분석 차트 업데이트"""
    if not ticker:
        return {
            'data': [],
            'layout': go.Layout(title='종목을 선택해주세요')
        }
    
    # 시작 날짜 계산
    start_date = get_start_date(period)
    
    # 감성 분석 결과 가져오기
    query = """
    SELECT ticker, source, title, sentiment_score, sentiment_label, timestamp
    FROM sentiment_results
    WHERE ticker = %s AND timestamp >= %s
    ORDER BY timestamp
    """
    
    sentiment_results = db.fetch_all(query, (ticker, start_date))
    
    if not sentiment_results:
        return {
            'data': [],
            'layout': go.Layout(title=f'{ticker} 감성 분석 결과 없음')
        }
    
    # 데이터프레임 생성
    df = pd.DataFrame(sentiment_results)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['sentiment_score'] = df['sentiment_score'].astype(float)
    
    # 날짜별 평균 감성 점수 계산
    df['date'] = df['timestamp'].dt.date
    daily_sentiment = df.groupby('date').agg({
        'sentiment_score': 'mean',
        'timestamp': 'first'  # 날짜별 첫 번째 타임스탬프 사용
    }).reset_index()
    
    # 감성 분포 계산
    sentiment_counts = df['sentiment_label'].value_counts()
    
    # 감성 점수 시계열 차트
    time_trace = go.Scatter(
        x=daily_sentiment['date'],
        y=daily_sentiment['sentiment_score'],
        mode='lines+markers',
        name='평균 감성 점수',
        line={'color': 'blue'}
    )
    
    # 감성 분포 파이 차트
    colors = {'positive': 'green', 'neutral': 'gray', 'negative': 'red'}
    pie_colors = [colors.get(label, 'gray') for label in sentiment_counts.index]
    
    pie_trace = go.Pie(
        labels=sentiment_counts.index,
        values=sentiment_counts.values,
        domain={'x': [0.7, 1], 'y': [0.2, 0.8]},
        name='감성 분포',
        hole=0.4,
        marker={'colors': pie_colors}
    )
    
    # 차트 레이아웃
    layout = go.Layout(
        title=f'{ticker} 감성 분석 결과',
        xaxis={'title': '날짜'},
        yaxis={'title': '감성 점수'},
        height=500
    )
    
    return {
        'data': [time_trace, pie_trace],
        'layout': layout
    }

# 콜백: 최근 뉴스 및 감성 업데이트
@app.callback(
    Output('recent-news', 'children'),
    [Input('ticker-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_recent_news(ticker, n_intervals):
    """최근 뉴스 및 감성 업데이트"""
    if not ticker:
        return html.P('종목을 선택해주세요')
    
    # 최근 뉴스 및 감성 분석 결과 가져오기
    query = """
    SELECT ticker, source, title, sentiment_score, sentiment_label, timestamp, url, content
    FROM sentiment_results
    WHERE ticker = %s
    ORDER BY timestamp DESC
    LIMIT 10
    """
    
    news_items = db.fetch_all(query, (ticker,))
    
    if not news_items:
        return html.P(f'{ticker}에 대한 최근 뉴스가 없습니다.')
    
    # 뉴스 목록 생성
    news_elements = []
    for item in news_items:
        # 감성에 따른 스타일 설정
        style = {'padding': '10px', 'margin': '5px 0', 'border-left': '4px solid', 'background-color': '#f9f9f9'}
        
        if item['sentiment_label'] == 'positive':
            style['border-left-color'] = 'green'
        elif item['sentiment_label'] == 'negative':
            style['border-left-color'] = 'red'
        else:
            style['border-left-color'] = 'gray'
        
        # 뉴스 항목 생성
        news_content = html.Div([
            html.H4(item['title']),
            html.P(f"출처: {item['source']} | 시간: {pd.to_datetime(item['timestamp']).strftime('%Y-%m-%d %H:%M')}"),
            html.P(item['content']) if item['content'] else None,
            html.P([
                f"감성 점수: {float(item['sentiment_score']):.3f} ({item['sentiment_label']})",
                html.A(" 원문 링크", href=item['url'], target="_blank") if item['url'] else ""
            ])
        ], style=style)
        
        news_elements.append(news_content)
    
    return html.Div(news_elements)

# 서버 실행
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050) 