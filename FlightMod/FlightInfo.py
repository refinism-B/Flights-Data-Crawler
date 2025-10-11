import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
import os
from pathlib import Path




## 用於航班詳細資訊爬蟲

def trans_date_from_chinese(chinese_date):
    """將中文日期格式轉為datetime格式"""
    clean_date = chinese_date.split("(")[0].strip()
    fmt = "%Y年 %m月 %d日"
    trans = datetime.strptime(clean_date, fmt)

    return trans



def find_tag(div_list, target_str:str):
    """用於尋找特定字串標籤的index"""
    target=0
    for i in div_list:
        if i.get_text() == target_str:
            break
        else:
            target += 1
    return target



def safe_extract(func):
    """判斷一個soup物件是否存在/有值，若沒有則回傳None"""
    try:
        return func()
    except (IndexError, AttributeError):
        return None



def gate_exist(soup):
    """判斷一個航班頁面中是否有到/離閘口資料"""
    gate = 0
    x = soup
    for i in x('div'):
        if '閘口' in i.text:
            gate = 1
            break
    return gate



def split_tz(time_str:str):
    """將帶有時區的時間字串分割，得到[time, timezone]列表"""
    if "(" in time_str:
        time_str = time_str.split("(")[0].strip()

    time_, tz = time_str.split(' ')
    return time_, tz



def crawl_flight_data(soup, url, gate_exist:bool):
    """當該班機有到/離閘門資料時使用的爬蟲"""
    flight_data = []

    # 班機基本資料。較容易在各網頁中出現差異，故先使用函式取得定位，再去取得資訊
    div_list = soup('div', class_='flightPageDataLabel')

    # 航班編號、機型、航空公司、飛行距離
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageIdent')[0].h1.text.strip()))
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageDataRow')[find_tag(div_list, '機型')]('div', class_='flightPageData')[0].text.strip().replace('\xa0', ' ')))
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageDataRow')[find_tag(div_list, '航空公司')]('div', class_='flightPageData')[0].text.strip().split('\n')[0]))
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageDataRow')[find_tag(div_list, '距離')].span.text.strip().replace(',', '').split(' ')[1]))

    # 起飛機場、起飛城市
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageSummaryOrigin')[0]('span', class_='displayFlexElementContainer')[0].text.strip()))
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageSummaryOrigin')[0]('span', class_='flightPageSummaryCity')[0].text.strip()))

    # 降落機場、降落城市
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageSummaryDestination')[0]('span', class_='displayFlexElementContainer')[0].text.strip()))
    flight_data.append(safe_extract(lambda: soup('div', class_='flightPageSummaryDestination')[0]('span', class_='destinationCity')[0].text.strip()))

    # 起飛日期
    flight_data.append(safe_extract(lambda: soup('span', class_='flightPageSummaryDepartureDay')[0].text))
    
    if gate_exist == 1:
        # 預計/實際離開閘門時間
        lg_e_time, lg_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[0].span.text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        lg_a_time, lg_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[0]('div', class_="flightPageDataActualTimeText")[0].text.strip().replace('\xa0', ' ').replace('\\n', '').replace('\\t', '')))

        flight_data.append(lg_e_time)
        flight_data.append(lg_a_time)
        
        # 預計/實際起飛時間
        d_e_time, d_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[1]('span')[1].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        d_a_time, d_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[1]('span')[0].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        
        flight_data.append(d_e_time)
        flight_data.append(d_a_time)
    
    else:
        # 預計/實際離開閘門時間
        flight_data.append(None)
        flight_data.append(None)
        
        # 預計/實際起飛時間
        d_e_time, d_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[0]('span')[1].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        d_a_time, d_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[0]('span')[0].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        
        flight_data.append(d_e_time)
        flight_data.append(d_a_time)

    # 起飛時區
    flight_data.append(d_a_tz)

    # 抵達日期
    flight_data.append(safe_extract(lambda: soup('span', class_='flightPageSummaryArrivalDay')[0].text))

    if gate_exist == 1:
        # 預計/實際降落時間
        a_e_time, a_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[2]('span')[1].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        a_a_time, a_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[2]('span')[0].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))

        flight_data.append(a_e_time)
        flight_data.append(a_a_time)

        # 預計/實際抵達閘門時間
        ag_e_time, ag_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[3]('span')[1].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        ag_a_time, ag_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[3]('div', class_='flightPageDataActualTimeText')[0].span.text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))

        flight_data.append(ag_e_time)
        flight_data.append(ag_a_time)
    
    else:
        # 預計/實際降落時間
        a_e_time, a_e_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[1]('span')[1].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))
        a_a_time, a_a_tz = split_tz(safe_extract(lambda: soup('div', class_='flightPageDataTimesChild')[1]('span')[0].text.strip().replace('\xa0', ' ').replace('\n', '').replace('\t', '')))

        flight_data.append(a_e_time)
        flight_data.append(a_a_time)

        # 預計/實際抵達閘門時間
        flight_data.append(None)
        flight_data.append(None)

    # 降落時區
    flight_data.append(a_a_tz)

    # 紀錄該航班網址，若有需要可再重新訪問
    flight_data.append(url)

    return flight_data