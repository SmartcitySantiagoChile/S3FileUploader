from datetime import datetime, timedelta
import argparse


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y%m%d")
    except ValueError:
        msg: str = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def get_date_list_between_two_given_dates(start_date: datetime, end_date: datetime) -> list:
    if start_date > end_date:
        raise ValueError("End date cannot be before start date.")
    delta: timedelta = timedelta(days=1)
    dates: list[datetime] = []

    while start_date <= end_date:
        dates.append(start_date)
        start_date += delta
    return dates
