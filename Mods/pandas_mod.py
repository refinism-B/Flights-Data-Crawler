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
