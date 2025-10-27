from datetime import date, datetime, timedelta


def get_yesterday(fmt="%Y%m%d"):
    """取得昨天日期"""
    today = date.today()
    yesterday = (today - timedelta(days=1)).strftime(fmt)

    return yesterday


def get_2days_ago(fmt="%Y%m%d"):
    """取得兩天前日期"""
    today = date.today()
    days_ago = (today - timedelta(days=2)).strftime(fmt)

    return days_ago
