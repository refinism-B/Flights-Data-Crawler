import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
import os
from pathlib import Path


## 用於航班列表爬蟲

def safe_extract(func):
    """判斷一個soup物件是否存在/有值，若沒有則回傳None"""
    try:
        return func()
    except (IndexError, AttributeError):
        return None


def split_airport_code(code):
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