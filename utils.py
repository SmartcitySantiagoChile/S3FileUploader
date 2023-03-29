from datetime import datetime, timedelta
import argparse
import csv

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
    
def retrieve_objects_with_pattern(pattern: str, aws_object_list: list) -> list:
    """AI is creating summary for retrieve_objects_with_pattern

    Args:
        pattern (str): [description]
        aws_object_list (list): [description]

    Returns:
        list: [description]
    """
    return []


def get_date_list_between_two_given_dates(start_date: datetime, end_date: datetime) -> list:
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

def update_file_by_tuples(input_file_name: str, output_file_name: str, tuples: list, delimiter="|"):
    """This function update a csv-like file with tuples values using the delimiter.

    Args:
        input_file_name (str): The file to update
        output_file_name (str): The output file name
        tuples (list): The tuples list with tuples in format [(column_to_check, value_to_replace, new_value)...]
        delimiter (str, optional): File's delimiter. Defaults to "|".
    """
    with open(input_file_name, 'r') as input_file, open(output_file_name, 'w') as output_file:
        reader: csv.reader = csv.reader(input_file, delimiter=delimiter)
        writer: csv.writer = csv.writer(output_file, delimiter=delimiter)
        for row in reader:
            for tuple_list in tuples:
                if int(tuple_list[0]) < len(row) and row[int(tuple_list[0])] == str(tuple_list[1]):
                    row[int(tuple_list[0])] = tuple_list[2]
                    
            writer.writerow(row)