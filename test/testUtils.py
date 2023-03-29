from unittest import TestCase
from utils import valid_date, get_date_list_between_two_given_dates
import datetime
import argparse

class UtilsTest(TestCase):

    def test_valid_date(self):
        date: str = '20101010'
        expected_date: datetime = datetime.strptime(date, "%Y%m%d")
        # valid date
        self.assertEqual(expected_date, valid_date(date))
        # not valid date
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date('1')
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date('2010-01-01')
        
    def test_get_date_list_between_two_given_dates(self):
        start_date: datetime.date = datetime.date(2022, 1, 1)
        end_date: datetime.date = datetime.date(2022, 1, 10)
        expected_date_list: list[datetime.date] = [
            datetime.date(2022, 1, 1),
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 3),
            datetime.date(2022, 1, 4),
            datetime.date(2022, 1, 5),
            datetime.date(2022, 1, 6),
            datetime.date(2022, 1, 7),
            datetime.date(2022, 1, 8),
            datetime.date(2022, 1, 9),
            datetime.date(2022, 1, 10)
        ]
        self.assertEqual(expected_date_list, get_date_list_between_two_given_dates(start_date, end_date))

    def test_get_date_list_between_two_given_dates_same_day_case(self):
        start_date: datetime.date = datetime.date(2022, 1, 1)
        end_date: datetime.date = datetime.date(2022, 1, 1)
        expected_date_list: list[datetime.date] = [
            datetime.date(2022, 1, 1)
        ]
        self.assertEqual(expected_date_list, get_date_list_between_two_given_dates(start_date, end_date))

    def test_get_date_list_between_two_given_dates_same_day_case(self):
        start_date: datetime.date = datetime.date(2022, 1, 1)
        end_date: datetime.date = datetime.date(2019, 1, 1)

        with self.assertRaises(ValueError):
                    get_date_list_between_two_given_dates(start_date, end_date)

