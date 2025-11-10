import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from Mods import pandas_mod as pdm
from airflow.decorators import dag, task
from utils.config import FLIGHT_LIST_COLUMNS, FLIGHT_CORP_LIST
from tasks import pandas_mod as pdm
from tasks import database_file_mod as dfm
from tasks import GCS_mod as gcs
from typing import Tuple


# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-1_pet_regis_count_daily",
    default_args=default_args,
    description="[每日更新]爬取每日寵物登記數",
    schedule_interval="0 */1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "daily", "registration"]
)
def D_01_Flights_List():
    @task
    def S_get_corp(corp_list: dict, index: int) -> str:
        return corp_list[index]

    def get_soup(corp: str, start_page: int, ss: requests.Session) -> BeautifulSoup:
        """訪問FlightAware網頁取得soup物件"""
        url = f'https://www.flightaware.com/live/fleet/{corp}?;offset={start_page};order=ident;sort=ASC'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'}
        res = ss.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

        return soup

    def page_exist_or_not(soup: BeautifulSoup) -> bool:
        """判斷這一頁是否有資料"""
        page_exist = True
        for tag in soup.find_all('i'):
            if tag.text == "Sorry. No matching flights found; try again later.":
                page_exist = False

        return page_exist

    def split_airport_code(code: str) -> Tuple[str, str]:
        """若機場代碼有兩種形式，會將兩者分開，回傳兩個代碼"""
        if code is not None:
            code = code.replace("(", "").replace(")", "")
            if "/" in code:
                code1, code2 = code.split("/")
                code1 = code1.strip()
                code2 = code2.strip()
            else:
                code1 = code
                code2 = code
        else:
            code1 = None
            code2 = None

        return code1, code2

    def safe_extract(func: BeautifulSoup):
        """判斷一個soup物件是否存在/有值，若沒有則回傳None"""
        try:
            return func()
        except (IndexError, AttributeError):
            return None

    def get_flight_info(table_list: list, logtime: datetime) -> list[list]:
        page_data = []
        for i in table_list:
            single = []
            flight_no = i('td')[0].span.a.text
            print(f'查詢{flight_no}班機資料...')

            # 紀錄日期
            single.append(logtime)

            # 班機編號
            single.append(safe_extract(lambda: i('td')[0].span.a.text))

            # 機型
            single.append(safe_extract(lambda: i('td')[1].span.a.text))

            # 起飛機場
            single.append(safe_extract(lambda: i('td')[
                2]('span', dir='ltr')[0].text))

            # 起飛機場代號（如有兩種則分開儲存，只有一種則重複儲存）
            code_d = safe_extract(
                lambda: i('td')[2]('span', dir='ltr')[1].text)
            code_d1, code_d2 = split_airport_code(code_d)
            single.append(code_d1)
            single.append(code_d2)

            # 降落機場
            single.append(safe_extract(lambda: i('td')[
                3]('span', dir='ltr')[0].text))

            # 降落機場代號（如有兩種則分開儲存，只有一種則重複儲存）
            code_a = safe_extract(
                lambda: i('td')[3]('span', dir='ltr')[1].text)
            code_a1, code_a2 = split_airport_code(code_a)
            single.append(code_a1)
            single.append(code_a2)

            # 連結
            single.append('https://www.flightaware.com' +
                          safe_extract(lambda: i('td')[0].span.a['href']))

            # 存回page_data list
            page_data.append(single)

        return page_data

    @task
    def E_get_list_data(corp: str) -> list[list]:
        data_list = []
        logtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        start_page = 0
        ss = requests.Session()

        while True:
            soup = get_soup()
            print(f'開始查詢{corp}的第{start_page}到{start_page + 20}筆資料...')

            if page_exist_or_not(soup=soup):
                table_list = soup('table', class_='prettyTable fullWidth')[
                    0]('tr')[2:]
                page_data = get_flight_info(table_list, logtime)

                data_list.extend(page_data)

                print(f'完成存取{corp}的第{start_page}到{start_page + 20}筆資料')
                start_page += 20
                time.sleep(5)

            else:
                print(f'{corp}沒有第{start_page}到{start_page + 20}筆資料')
                break

        return data_list

    @task
    def S_save_setting(date_str: str) -> dict:
        folder = Path(f"/opt/airflow/data/raw/flight_list/dt={date_str}")
        folder.mkdir(parents=True, exist_ok=True)
        file_name = "filghts_list.csv"

        return {"folder": folder, "file_name": file_name}

    @task
    def S_gcs_save_setting(date_str: str, local_setting: dict) -> dict:
        folder = local_setting["folder"]
        file_name = local_setting["file_name"]
        path = folder / file_name

        bucket_name = "flight-data-storage"
        destination = f"flights_list/raw/dt={date_str}/flights_list.csv"
        source_file_name = path

        return {
            "bucket_name": bucket_name,
            "destination": destination,
            "source_file_name": source_file_name
        }
#
#
#
#
#
#
    # 取得欄位名
    cols = FLIGHT_LIST_COLUMNS
    today_str = date.today().strftime("%Y-%m-%d")

    # 設定航空公司代號
    # 0為EVA、1為CAL、2為SJX、3為TTW
    EVA_corp = S_get_corp(corp_list=FLIGHT_CORP_LIST, index=0)
    CAL_corp = S_get_corp(corp_list=FLIGHT_CORP_LIST, index=1)
    SJX_corp = S_get_corp(corp_list=FLIGHT_CORP_LIST, index=2)
    TTW_corp = S_get_corp(corp_list=FLIGHT_CORP_LIST, index=3)

    # 分別爬取各公司的航班列表
    EVA_data = E_get_list_data(corp=EVA_corp)
    CAL_data = E_get_list_data(corp=CAL_corp)
    SJX_data = E_get_list_data(corp=SJX_corp)
    TTW_data = E_get_list_data(corp=TTW_corp)

    # 將資料轉換成dataframe
    EVA_df = pdm.T_transform_to_df_by_list(data=EVA_data, cols=cols)
    CAL_df = pdm.T_transform_to_df_by_list(data=CAL_data, cols=cols)
    SJX_df = pdm.T_transform_to_df_by_list(data=SJX_data, cols=cols)
    TTW_df = pdm.T_transform_to_df_by_list(data=TTW_data, cols=cols)

    # 合併四個df
    df_main = pdm.T_combine_four_dataframe(
        df1=EVA_df,
        df2=CAL_df,
        df3=SJX_df,
        df4=TTW_df
    )

    # 去除網址重複的資料
    df_main = pdm.T_drop_duplicated(df=df_main, col="link")

    # 取得存檔設定
    save_setting = S_save_setting(date_str=today_str)

    # 進行存檔
    dfm.L_save_file_to_csv_by_dict(save_setting=save_setting, df=df_main)

    # 設定GCS存檔資訊
    gcs_setting = S_gcs_save_setting(
        date_str=today_str, local_setting=save_setting)

    # 上傳至GCS
    gcs.L_upload_to_gcs(gcs_setting=gcs_setting)


D_01_Flights_List()
