import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import FlightMod.FlightInfo as fi


# 航空公司清單
flight_corp = ["EVA", "CAL", "SJX", "TTW"]

for corp in flight_corp:
    # 設定航班資訊的檔案路徑
    table_file = f'C:/Users/add41/Documents/Data_Engineer/Project/Flights-Data-Crawler/FlightData/{corp}_FlightsTable.csv'
    table_path = Path(table_file)

    # 若檔案不存在則先新建空的df並存檔
    if table_path.exists():
        df_table = pd.read_csv(table_file)

    else:
        columns = [
        'flight_NO',
        'flight_type',
        'flight_company',
        'fly_distance',
        'departure_airport_code',
        'departure_city',
        'arrival_airport_code',
        'arrival_city',
        'departure_date',
        'leave_gate_estimate',
        'leave_gate_actual',
        'departure_time_estimate',
        'departure_time_actual',
        'departure_timezone',
        'arrival_date',
        'landing_time_estimate',
        'landing_time_actual',
        'arrive_gate_estimate',
        'arrive_gate_actual',
        'arrive_timezone',
        'link'
        ]

        df_table = pd.DataFrame(columns=columns)


    # 設定航班列表的資料表路徑，並將列表資料讀入
    list_file = f'C:/Users/add41/Documents/Data_Engineer/Project/Flights-Data-Crawler/FlightData/{corp}_FlightList.csv'

    df_list = pd.read_csv(list_file)


    # 設定兩個mask條件篩選df_list中需要爬取的row，條件一：sync值為0（未爬取過）；條件二：日期為兩天以前，確保航行都已結束
    today = date.today()
    today = pd.Timestamp(today)

    df_list['query_date'] = pd.to_datetime(df_list['query_date'])

    mask_1 = (df_list['sync'] == 0)
    mask_2 = ((today - df_list['query_date']).dt.days >= 2)

    source = df_list[mask_1 & mask_2]


    # 建立dataframe需要的data list
    data = []

    # 建立selenium連線
    selenium_url = "http://localhost:4444/wd/hub"
    options = Options()
    options.add_argument("--headless")
    print('建立連線')

    # 根據df_list中的link欄位跑回圈，逐一進入網頁取得html編碼
    for url in source['link']:
        with webdriver.Remote(command_executor=selenium_url, options=options) as driver:
            try:
                driver.get(url)

                # 網頁內有JavaScript動態生成內容，故設定等待網頁讀取完畢後再動作
                wait = WebDriverWait(driver, 15)
                element = wait.until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "flightPageSummaryDepartureDay"))
                )

                # 如果有cookie選項的話選取同意，若沒有就跳過
                try:
                    driver.find_element(By.ID, 'onetrust-accept-btn-handler').click()
                    time.sleep(4)

                except:
                    pass

                # 取得網頁html碼，並轉換成soup物件
                page_source = driver.page_source
            
            except Exception as e:
                print(f"無法存取 {url}: {e}")
                continue

        soup = BeautifulSoup(page_source, 'html.parser')
        flight_no = soup('div', class_='flightPageIdent')[0].h1.text.strip()

        ## 根據取得的soup物件，開始抓取各項資訊
        print(f'開始查詢{flight_no}班機資訊資訊...')

        try:
            gate_exist = fi.gate_exist(soup)
            print(f'{flight_no}航班有無閘門資訊：{gate_exist}')
            flight_data = fi.crawl_flight_data(soup, url, gate_exist=gate_exist)
        
        except Exception as e:
            print(f'發生錯誤：{e}')

        data.append(flight_data)
        print(f'完成存取{flight_no}航班資料')
        time.sleep(7)

    columns = [
        'flight_NO',
        'flight_type',
        'flight_company',
        'fly_distance',
        'departure_airport_code',
        'departure_city',
        'arrival_airport_code',
        'arrival_city',
        'departure_date',
        'leave_gate_estimate',
        'leave_gate_actual',
        'departure_time_estimate',
        'departure_time_actual',
        'departure_timezone',
        'arrival_date',
        'landing_time_estimate',
        'landing_time_actual',
        'arrive_gate_estimate',
        'arrive_gate_actual',
        'arrive_timezone',
        'link'
        ]

    df2 = pd.DataFrame(columns=columns, data=data)

    # 將本次爬取航班中，航行未完成（沒有降落時間）的資料先去除，待下次再爬取
    df2 = df2.dropna(subset=['landing_time_actual'])

    # 將爬取的新資料與原本的table資料合併
    df_combine = pd.concat([df_table, df2], ignore_index=True)

    # 根據link欄位再去除可能的重複值
    df_table = df_combine.drop_duplicates(subset='link', keep = 'first')

    # 將去除重複後的資料存檔
    df_table.to_csv(table_file, index=False)

    # 將已經爬取過的航班sync欄位改為1，避免下次重複爬取
    mask_done = df_list['link'].isin(df2['link'])
    df_list.loc[mask_done, 'sync'] = 1

    # 將修改後的df_list再存檔回FlightList.csv檔案
    df_list.to_csv(list_file, index=False)


print('已更新所有資料！')