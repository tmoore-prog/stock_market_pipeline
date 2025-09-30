import pandas_market_calendars as mcal
from bigquery_client import BigQueryManager


def get_trading_days(start_date, end_date, calendar='NYSE'):
    calendar = mcal.get_calendar(calendar)
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    trading_days = schedule.index

    return trading_days


def get_completed_dates():
    bq_manager = BigQueryManager()
    return bq_manager.get_completed_dates()
