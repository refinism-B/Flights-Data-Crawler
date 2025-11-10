import pandas as pd
from pathlib import Path
import os
from airflow.decorators import task
from typing import Tuple


"""
這個模組是關於pandas應用的自訂函式
通常簡寫為 import pandas_mod as pdm
"""


@task
def read_or_build(folder: str, file: str, columns: list) -> Tuple[pd.DataFrame, str]:
    """檢查路徑檔案是否存在，若有則讀取，無則建立空表格"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    # 若檔案不存在則先新建空的df並存檔
    if path.exists():
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=columns)

    return df, file_path


@task
def exist_or_not(folder: str, file: str) -> Tuple[bool, str]:
    """檢查路徑檔案是否存在，並回傳確認結果"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    return path.exists(), file_path


@task
def T_reassign_id(df: pd.DataFrame, setting_dict: dict) -> pd.DataFrame:
    """
    功能為針對id欄位自動編號。
    需要提供要加上id編號的dataframe，
    以及"setting_dict"編號設定檔。
    設定檔中需定義："id_cols"為id欄位的名稱；
    "id_str"為編號的前綴字串。

    注意：尚未編號的資料請先填入空字串「""」。
    """

    id_col_name = setting_dict["id_cols"]
    id_str = setting_dict["id_str"]

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
    new_id = [f"{id_str}{i:04d}" for i in range(
        start_num, start_num + empty_id_count)]

    # 將list放入df欄位
    df.loc[empty_id, id_col_name] = new_id

    return df


@task
def T_combine_dataframe(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """concat兩個dataframe"""
    df_combine = pd.concat([df1, df2], ignore_index=True)

    return df_combine


@task
def T_combine_six_dataframe(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
    df5: pd.DataFrame,
    df6: pd.DataFrame,
) -> pd.DataFrame:
    """concat六個dataframe"""
    df_combine = pd.concat([df1, df2, df3, df4, df5, df6], ignore_index=True)

    return df_combine


@task
def T_combine_five_dataframe(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
    df5: pd.DataFrame,
) -> pd.DataFrame:
    """concat五個dataframe"""
    df_combine = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)

    return df_combine


@task
def T_combine_four_dataframe(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
) -> pd.DataFrame:
    """concat四個dataframe"""
    df_combine = pd.concat([df1, df2, df3, df4], ignore_index=True)

    return df_combine


@task
def T_rename_columns(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    """
    將df的欄位名稱替換成list中的內容。
    請注意欄位數與list內數量需一致。
    """

    if len(df.columns) != len(col_list):
        raise ValueError("DF欄位數與列表長度不符合！")
    else:
        df.columns = col_list
        return df


@task
def T_drop_columns(df: pd.DataFrame, drop_list: list) -> pd.DataFrame:
    """去除給定的欄位名"""
    missing_cols = [col for col in drop_list if col not in df.columns]
    if len(missing_cols) != 0:
        raise ValueError("某些欄位不存在df中！")
    else:
        df = df.drop(columns=drop_list, axis=1)
        return df


@task
def T_sort_columns(df: pd.DataFrame, new_cols: list) -> pd.DataFrame:
    """
    將df欄位順序改為list中的順序。
    注意：欄位名需一致，僅順序可以不同。
    """

    missing_cols = [col for col in new_cols if col not in df.columns]
    if len(missing_cols) != 0:
        raise ValueError("某些欄位不存在df中！")
    else:
        df = df[new_cols]
        return df


@task
def T_transform_to_df(data: list[dict]) -> pd.DataFrame:
    """將包含多個dict的list轉換成dataframe"""
    df = pd.DataFrame(data=data)
    return df


@task
def T_transform_to_df_by_list(data: list, cols: list) -> pd.DataFrame:
    """將包含多個dict的list轉換成dataframe"""
    df = pd.DataFrame(data=data, columns=cols)
    return df


@task
def S_count_data(df: pd.DataFrame) -> int:
    """計算並回傳df的資料筆數"""
    count = len(df)
    return count


@task
def T_drop_duplicated(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.drop_duplicates(subset=col, keep="first")

    return df
