import pandas as pd
from pathlib import Path
import os


def read_or_build(folder, file, columns):
    """檢查路徑檔案是否存在，若有則讀取，無則建立空表格"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    # 若檔案不存在則先新建空的df並存檔
    if path.exists():
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=columns)

    return df, file_path


def exist_or_not(folder, file):
    """檢查路徑檔案是否存在，若有則讀取，無則建立空表格"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    return path.exists(), file_path


def reassign_id(df, id_col_name, id_str):
    """根據原有最後一筆資料進行自動編號"""

    # 先找出原本的id編號（id欄位非空）最大值
    nums = df.loc[df[id_col_name] != "",
                  id_col_name].str.extract(r"(\d+)").astype(int)

    if nums.empty:
        start_num = 1
    else:
        start_num = nums.max()[0] + 1

    # 計算需要新增的資料數
    empty_id = df[id_col_name] == ""
    empty_id_count = empty_id.sum()

    # 先列出編號list
    new_id = [f"{id_str}{i:03d}" for i in range(
        start_num, start_num + empty_id_count)]

    # 將list放入df欄位
    df.loc[empty_id, id_col_name] = new_id

    return df
