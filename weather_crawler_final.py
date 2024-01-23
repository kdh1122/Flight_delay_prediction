#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import requests
from bs4 import BeautifulSoup
import pandas as pd

date=[] # 크롤링한 데이터 중 날짜(달, 일)를 담을 리스트
weather=[] # 크롤링 한 데이터 중 날씨를 담을 리스트
portl=[] # 공항 코드를 담을 리스트
yearl=[] # 년도를 담을 리스트

# 날씨 사이트의 URL 주소 중 상위 10개 공항에 해당하는 부분
local=['146640', '146513', '145689', '145521', '145920', '145341', '145433', '146721', '146042', '145529']
# local 리스트 요소들에 해당하는 공항 코드 리스트
port=['ATL', 'ORD', 'DEN', 'PHX', 'DFW', 'LAX', 'LAS', 'DTW', 'IAH', 'SLC']

year = input('년도를 입력하세요') #년도 입력기받기

y = 0 # 아래 for문을 돌 때마다 portl에 append할 공항 코드를 port 리스트에서 추출하기 위해 인덱스로 사용할 변수 초기화

# for문으로 local 리스트를 돌면서 상위 10개 공항의 4계절 날씨 데이터 수집
# 웹 사이트의 특성 상 봄(3, 4, 5월), 여름(6, 7, 8월), 가을(9, 10, 11월), 겨울(12, 1, 2월)을 따로 수집해야 함
for i in local:
    # 입력받은 year와 i번째(local list) 공항의 날씨 사이트 주소 request
    res3 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/3/Historical') # 3:겨울
    res0 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/0/Historical') # 0:봄
    res1 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/1/Historical') # 1:여름
    res2 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/2/Historical') # 2:가을
    
    # 겨울
    soup = BeautifulSoup(res3.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ") # 웹에서 날짜(달, 일) 부분 크롤링
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ") # 웹에서 날씨 부분 크롤링
    for n in data:
        date.append(n.text.strip()) # 크롤링 한 날짜 데이터 스플릿해서 date 리스트에 모두 추가
        portl.append(port[y]) # 크롤링 한 날짜 수 만큼 portl 리스트에 해당 공항 코드 추가
        yearl.append(year) # 크롤링 한 날짜 수 만큼 yearl 리스트에 해당 년도 추가
    for n in ww:
        weather.append(n.text.strip()) # 크롤링 한 날씨 데이터 스플릿해서 weather 리스트에 모두 추가
    
    # 봄
    soup = BeautifulSoup(res0.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
        portl.append(port[y])
        yearl.append(year)
    for n in ww:
        weather.append(n.text.strip())
    
    # 여름
    soup = BeautifulSoup(res1.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
        portl.append(port[y])
        yearl.append(year)
    for n in ww:
        weather.append(n.text.strip())

    # 가을
    soup = BeautifulSoup(res2.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
        portl.append(port[y])
        yearl.append(year)
    for n in ww:
        weather.append(n.text.strip())
    y += 1 # port 리스트의 인덱스 증가시켜서 다음 공항 코드로 
    

# 년도, 날짜, 공항코드, 날씨 리스트 다 합쳐서 데이터 프레임 제작
DF=pd.DataFrame(zip(yearl, date, portl, weather))
print(DF)

# 데이터 프레임 csv로 저장
DF.to_csv(year+"weather.csv", mode='w')

