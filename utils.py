from datetime import datetime, timedelta
import argparse
import csv

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
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

def update_file_by_tuples(input_file_name: str, output_file_name: str, tuples: list, delimiter="|"):
    with open(input_file_name, 'r') as input_file, open(output_file_name, 'w') as output_file:
        reader: csv.reader = csv.reader(input_file, delimiter=delimiter)
        writer: csv.writer = csv.writer(output_file, delimiter=delimiter)
        for row in reader:
            for tuple_list in tuples:
                if int(tuple_list[0]) < len(row) and row[int(tuple_list[0])] == str(tuple_list[1]):
                    row[int(tuple_list[0])] = tuple_list[2]
                    
            writer.writerow(row)