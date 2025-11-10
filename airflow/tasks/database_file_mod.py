import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
from airflow.decorators import task
import pymysql


"""
這個模組主要用於資料或檔案的存取/讀取。
其中由於sqlalchemy版本相容問題無法使用，
而pymysql在寫入資料上較繁複，
暫時未編寫「將資料寫入資料庫」的函式。
通常寫作 import database_file_mod as dfm
"""


def create_pymysql_connect():
    """
    自動透過pymysql建立連線，回傳conn連線物件。
    所需各項資料請寫入.env檔案中。請勿直接寫於程式中。
    """

    load_dotenv()

    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    conn = pymysql.connect(
        host=target_ip,
        port=target_port,
        user=username,
        password=password,
        database=db_name,
        charset='utf8mb4'
    )

    return conn


@task
def L_save_file_to_csv_by_dict(save_setting: dict, df: pd.DataFrame):
    """
    提供"save_setting"的存檔設定dict，抓取其中的"folder"和"file_name"
    資料，自動將df存檔成csv檔案。
    """

    try:
        folder = Path(save_setting["folder"])
        folder.mkdir(parents=True, exist_ok=True)
        file_name = save_setting["file_name"]
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8-sig")

        print(f"{file_name}地端存檔成功！")
    except Exception as e:
        print({f"{file_name}地端存檔失敗：{e}"})


@task
def L_save_file_to_csv(folder: str, file_name: str, df: pd.DataFrame):
    """
    提供folder路徑和file_name檔案名稱，自動將df存檔成csv檔案。
    """

    try:
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8-sig")

        print(f"{file_name}地端存檔成功！")

    except Exception as e:
        print({f"{file_name}地端存檔失敗：{e}"})


@task
def E_load_file_from_csv_by_dict(read_setting: dict) -> pd.DataFrame:
    """
    提供"read_setting"讀檔設定dict，抓取其中的"folder"和"file_name"
    資料，自動讀取csv檔案並轉成dataframe。
    """

    folder = Path(read_setting["folder"])
    file_name = read_setting["file_name"]
    path = folder / file_name
    cols = pd.read_csv(path, nrows=0).columns

    dtype_dict = {col: str for col in cols if col.lower() == 'phone'}
    df = pd.read_csv(path, dtype=dtype_dict)

    return df


@task
def E_load_file_from_csv(folder: str, file_name: str) -> pd.DataFrame:
    """
    提供folder路徑和file_name檔案名稱，自動讀取csv檔案並轉成dataframe。
    """

    folder = Path(folder)
    path = folder / file_name
    cols = pd.read_csv(path, nrows=0).columns

    dtype_dict = {col: str for col in cols if col.lower() == 'phone'}
    df = pd.read_csv(path, dtype=dtype_dict)

    return df


@task
def E_load_from_sql(table_name: str) -> pd.DataFrame:
    """
    輸入欲查詢的表名table_name，透過pymysql連線資料庫，
    並取得該表後將其轉成dataframe。

    連線所需資訊請寫入.env中，請勿寫入程式中。
    """

    conn = create_pymysql_connect()
    sql = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql(sql, conn)
        return df.to_dict(orient='records')

    except Exception as e:
        raise Exception(f"讀取{table_name}表時發生錯誤：{e}")
