import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
import os
from pathlib import Path
import Mods.CrawlList as fcl
from google.cloud import storage


def main():
    flight_corp = ["EVA", "CAL", "SJX", "TTW"]

    for corp in flight_corp:
        # 判斷總列表.csv檔是否存在，若不存在則先建立一個只有columns的空表格
        file = f'/app/FlightData/{corp}_FlightList.csv'
        list_path = Path(file)

        if list_path.exists():
            pass
        else:
            columns = [
                'query_date',
                'flight_no',
                'flight_type',
                'departure_airport',
                'departure_airport_code_1',
                'departure_airport_code_2',
                'arrival_airport',
                'arrival_airport_code_1',
                'arrival_airport_code_2',
                'link',
                'sync'
            ]

            df = pd.DataFrame(columns=columns)
            df.to_csv(file, index=False)


        # 讀入總表檔案
        df_list = pd.read_csv(file)


        # 建立空list（為建立dataframe預備）並設定起始頁數，建立ss連線
        data = []
        logtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        start_page = 0
        ss = requests.Session()


        while True:
            # 網頁為20筆一頁，設定從第0筆開始查詢，每次回圈+20，直到查無資料後break
            # 利用ss.get發出請求並轉換出soup物件
            url = f'https://www.flightaware.com/live/fleet/{corp}?;offset={start_page};order=ident;sort=ASC'
            headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'}
            res = ss.get(url, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            print(f'開始查詢{corp}的第{start_page}到{start_page + 20}筆資料...')


            # 先判斷是否有資料，若找到「查無資料」的標籤，則設 found = True 終止迴圈
            found = False
            for tag in soup.find_all('i'):
                if tag.text == "Sorry. No matching flights found; try again later.":
                    found = True
                    break


            # 若仍有資料 found = False 則繼續迴圈，分別尋找兩個標籤（兩種皆有連結）並合併list
            if found == False:
                table_list = soup('table', class_='prettyTable fullWidth')[0]('tr')[2:]

                for i in table_list:
                    single = []
                    flight_no = i('td')[0].span.a.text
                    print(f'查詢{flight_no}班機資料...')

                    # 紀錄日期
                    single.append(logtime)

                    # 班機編號
                    single.append(fcl.safe_extract(lambda: i('td')[0].span.a.text))

                    # 機型
                    single.append(fcl.safe_extract(lambda: i('td')[1].span.a.text))

                    # 起飛機場
                    single.append(fcl.safe_extract(lambda: i('td')[2]('span', dir='ltr')[0].text))

                    # 起飛機場代號（如有兩種則分開儲存，只有一種則重複儲存）
                    code_d = fcl.safe_extract(lambda: i('td')[2]('span', dir='ltr')[1].text)
                    code_d1, code_d2 = fcl.split_airport_code(code_d)
                    single.append(code_d1)
                    single.append(code_d2)


                    # 降落機場
                    single.append(fcl.safe_extract(lambda: i('td')[3]('span', dir='ltr')[0].text))

                    # 降落機場代號（如有兩種則分開儲存，只有一種則重複儲存）
                    code_a = fcl.safe_extract(lambda: i('td')[3]('span', dir='ltr')[1].text)
                    code_a1, code_a2 = fcl.split_airport_code(code_a)
                    single.append(code_a1)
                    single.append(code_a2)

                    # 連結
                    single.append('https://www.flightaware.com' + fcl.safe_extract(lambda: i('td')[0].span.a['href']))

                    # 同步標記
                    single.append(0)

                    data.append(single)
                    print(f'完成{flight_no}班機資料存取')


                # 完成後查詢筆數+20並稍微等待後在進行下一次迴圈
                print(f'完成存取{corp}的第{start_page}到{start_page + 20}筆資料')
                start_page += 20
                time.sleep(15)


            # 當查無資料時 found = True 顯示查無資料並終止迴圈
            else:
                print(f'{corp}沒有第{start_page}到{start_page + 20}筆資料')
                break

        print(f'已完成{corp}存取資料')



        # 根據爬蟲資料建立Dataframe
        columns = [
            'query_date',
            'flight_no',
            'flight_type',
            'departure_airport',
            'departure_airport_code_1',
            'departure_airport_code_2',
            'arrival_airport',
            'arrival_airport_code_1',
            'arrival_airport_code_2',
            'link',
            'sync'
        ]

        df2 = pd.DataFrame(columns=columns, data=data)
        print(f'{corp}新資料建檔完成')


        # 將機場名稱中的Int'l字樣去除，並去除前後空白
        df2['departure_airport'] = df2['departure_airport'].str.replace("Int\'l", "").str.replace("Intl", "").str.strip()
        df2['arrival_airport'] = df2['arrival_airport'].str.replace("Int\'l", "").str.replace("Intl", "").str.strip()


        # 直接將新資料與舊資料合併
        df_combine = pd.concat([df_list, df2], ignore_index=True)

        # 對合併後的資料使用drop_duplicates，將重複值刪去，並覆蓋回df_list
        df_list = df_combine.drop_duplicates(subset='link', keep = 'first').reset_index(drop=True)

        # 將query_date欄位轉換為datetime物件
        df_list['query_date'] = pd.to_datetime(df_list['query_date'])

        # 將新的df_list進行存檔
        df_list.to_csv(file, index=False)

        print(f'完成{corp}資料更新，目前資料筆數：{len(df_list)}')
        print('5秒後繼續...')
        time.sleep(5)

        try:
            # 將檔案上傳至GCS保存
            now = datetime.now().strftime("%Y%m%d_%H%M%S")
            now = str(now)

            bucket_name = "flight-data-storage"
            destination_file = f"flight-list-data/{corp}_FlightList_{now}.csv"
            credentials_path = "/app/key/tactile-pulsar-473901-a1-4763fa15e78b.json"

            client = storage.Client.from_service_account_json(credentials_path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_file)

            blob.upload_from_filename(file)
            print("已將檔案上傳至GCS！")

        except Exception as e:
            print(f'發生錯誤：{e}')

    print('已完成所有航空公司資料更新！')



if __name__ == "__main__":
    main()