from datetime import datetime, timedelta
import argparse
import csv
import fnmatch
import io
import os
import shutil
import zipfile
import gzip


def valid_date(s: str) -> datetime.date:
    """This is a function that validate a date with the format YYYY-mm-dd.

    Args:
        s (str): Date string to validate

    Raises:
        argparse.ArgumentTypeError: In case of wrong format raise wrong argument

    Returns:
        datetime.date: The date string parsed as date object
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg: str = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def valid_three_tuple_list(tuple_list: str) -> list:
    """This is a function that validate a three tuple list with the format [a,b,c] [d,e,f] ... [x,y,z].

    Args:
        tuple_list (str): A string that represents a three tuple list

    Raises:
        ValueError: Raise this error in case of malformed tuple_list

    Returns:
        list: A formatted three tuple list
    """
    split_tuple_list: list = tuple_list.split()
    three_tuple_list: list = []
    for three_tuple_string in split_tuple_list:
        try:
            three_tuple: list = [x for x in three_tuple_string.strip("[]").split(",")]
            if len(three_tuple) != 3:
                raise argparse.ArgumentTypeError(
                    f"Malformed input: {three_tuple_string}. Must be a three tuple. Check if the string has wrong spaces."
                )
            if len(three_tuple) and not three_tuple[0].isnumeric():
                raise argparse.ArgumentTypeError(
                    f"Malformed input: {three_tuple_string}. The first element must be a digit."
                )
            three_tuple_list.append(three_tuple)

        except ValueError:
            raise argparse.ArgumentTypeError(f"Malformed input: {three_tuple_string}")
    return three_tuple_list


def retrieve_objects_with_pattern(pattern: str, aws_object_list: list) -> list:
    """This is a function that retrieves all filenames that match a pattern by checking an AWS object list.

    Args:
        pattern (str): filename pattern
        aws_object_list (list): AWS object list

    Returns:
        list: object matched list
    """
    object_matched_list: list = []
    for object in aws_object_list:
        if object.get("name") and fnmatch.fnmatch(object.get("name"), pattern):
            object_matched_list.append(object["name"])
    return object_matched_list


def get_date_list_between_two_given_dates(
    start_date: datetime, end_date: datetime
) -> list:
    """This function make a list of dates between two given dates.

    Args:
        start_date (datetime): The initial date
        end_date (datetime): The last date (included)

    Raises:
        ValueError: Throw this error if the end date is before the start date

    Returns:
        list: An ordered list with datetime objects between start_date and end_date
    """
    if start_date > end_date:
        raise ValueError("End date cannot be before start date.")
    delta: timedelta = timedelta(days=1)
    dates: list[datetime] = []

    while start_date <= end_date:
        dates.append(start_date)
        start_date += delta
    return dates


def update_file_by_tuples(
    input_file_name: str, output_file_name: str, tuples: list, delimiter="|"
):
    """This function update a csv-like file with tuples values using the delimiter.

    Args:
        input_file_name (str): The file to update
        output_file_name (str): The output file name
        tuples (list): The tuples list with tuples in format [(column_to_check, value_to_replace, new_value)...]
        delimiter (str, optional): File's delimiter. Defaults to "|".
    """
    with open(input_file_name, "r") as input_file, open(
        output_file_name, "w"
    ) as output_file:
        reader: csv.reader = csv.reader(input_file, delimiter=delimiter)
        writer: csv.writer = csv.writer(output_file, delimiter=delimiter)
        for row in reader:
            for column_index, previous_value, new_value in tuples:
                if int(column_index) < len(row) and row[int(column_index)] == str(
                    previous_value
                ):
                    row[int(column_index)] = new_value

            writer.writerow(row)


def is_gzipfile(file_path: str) -> bool:
    """This function validate if a file_path is a gzip file.

    Args:
        file_path (str): The file path

    Returns:
        bool: True if is a gzip file
    """
    with gzip.open(file_path) as file_obj:
        try:
            file_obj.read(1)
            return True
        except IOError:
            return False


def get_file_object(file_path: str):
    """This function is designed to open a file in one of three possible formats: gzip, zip, or no compressed.
      Then, extract the file if it is compressed and make a copy.
    Args:
        file_path (_type_): File path to the object to copy

    Returns:
        str: Return the filepath of the copy and the compress type
    """
    compress_mode: str = ""
    file_name: str = os.path.basename(file_path)
    path: str = os.path.dirname(file_path)
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, "r") as zip_file:
            zip_file.extractall(path=path)
            inside_file: str = file_name.split(".zip")[0]
            compress_mode = "zip"
            file_name = os.path.join(path, inside_file)
    elif is_gzipfile(file_path):
        with gzip.open(file_path, str("rt"), encoding="utf-8") as f_in:
            inside_file: str = file_name.split(".gz")[0]
            with open(os.path.join(path, inside_file), "wt") as f_out:
                shutil.copyfileobj(f_in, f_out)
                file_name = os.path.join(path, inside_file)
                compress_mode = "gz"
    else:
        file_name = file_path

    return file_name, compress_mode
