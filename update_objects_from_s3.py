import argparse
import gzip
import logging
import os
import sys

from utils import (
    get_date_list_between_two_given_dates,
    valid_date,
    valid_three_tuple_list,
)

new_path: str = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.append(new_path)

from aws import AWSSession


def main(argv):
    """
    This script will update one or more objects from S3 bucket.
    """

    # Arguments and description
    parser = argparse.ArgumentParser(
        description="update one or more objects from S3 bucket"
    )

    parser.add_argument("bucket", default=None, help="bucket name")
    parser.add_argument(
        "extension",
        help="Object bucket files extension, can be a pattern. Example: .bip*",
        type=str,
    )
    parser.add_argument(
        "start_date",
        help="End date to process in YYYY-MM-DD format .",
        type=valid_date,
    )
    parser.add_argument(
        "end_date",
        help="Start date to process in YYYY-MM-DD format .",
        type=valid_date,
    )

    parser.add_argument(
        "tuples",
        help="3-Tuples of values to be replaced on the files. Example: [0,2,3] [2,METRO,BUS]",
        type=valid_three_tuple_list,
    )
    parser.add_argument(
        "--destination-path",
        default=None,
        help="path where files will be saved, if it is not provided we will use current path",
    )

    args: argparse.Namespace = parser.parse_args(argv[1:])

    logger: logging.Logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    # Give names to arguments
    bucket_name: str = args.bucket
    start_date: str = args.start_date
    end_date: str = args.end_date
    extension: str = args.extension
    tuples_list: list = args.tuples
    destination_path: str = args.destination_path

    if destination_path is not None and not os.path.isdir(destination_path):
        logger.info(f"Path '{destination_path}' is not valid")
        exit(1)

    # Check start_date and end_date
    date_list: list = get_date_list_between_two_given_dates(start_date, end_date)
    aws_session = AWSSession()

    if not aws_session.check_bucket_exists(bucket_name):
        logger.info(f"Bucket '{bucket_name}' does not exist")
        exit(1)

    logger.info(f"Bucket name: {bucket_name} ...")

    # Get all objects from bucket
    object_list: list = aws_session.retrieve_obj_list(bucket_name)

    aws_session.update_files_from_bucket(
        date_list, bucket_name, extension, object_list, tuples_list, destination_path
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
