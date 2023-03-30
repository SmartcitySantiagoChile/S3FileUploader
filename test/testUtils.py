import csv
import os
from unittest import TestCase
from utils import (
    valid_date,
    get_date_list_between_two_given_dates,
    update_file_by_tuples,
    retrieve_objects_with_pattern,
    valid_three_tuple_list,
    is_gzipfile,
    get_file_object_copy,
)
import datetime
import argparse


class TestValidDate(TestCase):
    def test_valid_date(self):
        date: str = "2010-10-10"
        expected_date: datetime = datetime.datetime.strptime(date, "%Y-%m-%d")
        self.assertEqual(expected_date, valid_date(date))

    def test_not_valid_date(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date("1")

    def test_malformed_date(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date("20100101")


class TestValidThreeTupleList(TestCase):
    def test_three_not_valid_sub_list(self):
        not_valid_list: str = "[1,2]"
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_three_tuple_list(not_valid_list)

    def test_three_not_valid_list(self):
        not_valid_list: str = "[0,b,c [1, ]2]"
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_three_tuple_list(not_valid_list)

    def test_no_digit_list(self):
        not_valid_list: str = "[a,b,c] [1,2,3]"
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_three_tuple_list(not_valid_list)

    def test_valid_list(self):
        valid_list: str = "[1,2,3]"
        expected_list: list = [["1", "2", "3"]]
        self.assertEqual(expected_list, valid_three_tuple_list(valid_list))

    def test_valid_list_of_lists(self):
        valid_list: str = "[1,2,3] [1,METRO,BUS]"
        expected_list: list = [["1", "2", "3"], ["1", "METRO", "BUS"]]
        self.assertEqual(expected_list, valid_three_tuple_list(valid_list))


class TestGetDateList(TestCase):
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
            datetime.date(2022, 1, 10),
        ]
        self.assertEqual(
            expected_date_list,
            get_date_list_between_two_given_dates(start_date, end_date),
        )

    def test_get_date_list_between_two_given_dates_same_day_case(self):
        start_date: datetime.date = datetime.date(2022, 1, 1)
        end_date: datetime.date = datetime.date(2022, 1, 1)
        expected_date_list: list[datetime.date] = [datetime.date(2022, 1, 1)]
        self.assertEqual(
            expected_date_list,
            get_date_list_between_two_given_dates(start_date, end_date),
        )

    def test_get_date_list_between_two_given_dates_same_day_case(self):
        start_date: datetime.date = datetime.date(2022, 1, 1)
        end_date: datetime.date = datetime.date(2019, 1, 1)

        with self.assertRaises(ValueError):
            get_date_list_between_two_given_dates(start_date, end_date)


class TestUpdateFileByTuples(TestCase):
    def setUp(self) -> None:
        input_file: str = os.path.join(
            os.path.dirname(__file__), "files", self.file_name_list[0]
        )
        output_file: str = input_file + "-updated"
        tuple_list: list[tuple] = [("3", "2", "1"), ("3", "5", "1")]
        update_file_by_tuples(input_file, output_file, tuple_list)
        with open(input_file, "r") as input, open(output_file, "r") as output:
            reader_input: csv.reader = csv.reader(input, delimiter="|")
            reader_output: csv.reader = csv.reader(output, delimiter="|")
            for row_input, row_output in zip(reader_input, reader_output):
                if row_input[3] == "2":
                    self.assertEqual(row_output[3], "1")
                if row_input[3] == "5":
                    self.assertEqual(row_output[3], "1")
        os.remove(output_file)


class TestRetrieveObjectsWithPattern(TestCase):
    def test_not_matched_case(self):
        pattern = "*.trip*"
        aws_object_list: list[dict[str, str]] = [
            {"name": "example.bip"},
            {"name": "example2.bip"},
            {"name": "example3.bip"},
        ]
        self.assertEqual(
            [],
            retrieve_objects_with_pattern(pattern, aws_object_list),
        )

    def test_matched_case(self):
        pattern = "*.bip*"
        aws_object_list: list[dict[str, str]] = [
            {"name": "example.bip"},
            {"name": "example2.bip"},
            {"name": "example3.bip"},
        ]
        self.assertEqual(
            ["example.bip", "example2.bip", "example3.bip"],
            retrieve_objects_with_pattern(pattern, aws_object_list),
        )

    def test_matched_case_for_gz_and_zip(self):
        pattern = "example.bip*"
        aws_object_list: list[dict[str, str]] = [
            {"name": "example.bip"},
            {"name": "example.bip.gz"},
            {"name": "example.bip.zip"},
        ]
        self.assertEqual(
            ["example.bip", "example.bip.gz", "example.bip.zip"],
            retrieve_objects_with_pattern(pattern, aws_object_list),
        )


class TestIsGzipFile(TestCase):
    def test_not_gzip_case(self):
        file: str = os.path.join(os.path.dirname(__file__), "files", "2021-06-30.bip")
        self.assertFalse(is_gzipfile(file))

    def test_gzip_case(self):
        file: str = os.path.join(
            os.path.dirname(__file__), "files", "2021-06-30.bip.gz"
        )
        self.assertTrue(is_gzipfile(file))


class TestGetFileObjectCopy(TestCase):
    def test_zip_file_case(self):
        file: str = os.path.join(
            os.path.dirname(__file__), "files", "2021-05-30.bip.zip"
        )
        expected_file: str =  os.path.join(os.path.dirname(__file__), "files", "2021-05-30.bip.copy")
        expected_compress_mode: str = "zip"
        opened_file, compress_mode = get_file_object_copy(file)
        self.assertEqual(expected_file, opened_file)
        self.assertEqual(expected_compress_mode, compress_mode)
        os.remove(expected_file)

    def test_gz_file_case(self):
        file: str = os.path.join(
            os.path.dirname(__file__), "files", "2021-06-30.bip.gz"
        )
        opened_file = get_file_object_copy(file)
        self.assertIsNotNone(opened_file)

    def test_file_case(self):
        file: str = os.path.join(os.path.dirname(__file__), "files", "2021-06-30.bip")
        opened_file = get_file_object_copy(file)
        self.assertIsNotNone(opened_file)
