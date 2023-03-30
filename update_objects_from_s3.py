import argparse
import logging
import os
import sys
from botocore.exceptions import ClientError
from utils import valid_date, get_date_list_between_two_given_dates, retrieve_objects_with_pattern
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
        help="Object bucket files extension, can be a pattern.",
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
    destination_path: str = args.destination_path

    

    if destination_path is not None and not os.path.isdir(destination_path):
        logger.info(f"Path '{destination_path}' is not valid")
        exit(1)


    # Check start_date and end_date
    date_list: list= get_date_list_between_two_given_dates(start_date, end_date)
    aws_session = AWSSession()

    if not aws_session.check_bucket_exists(bucket_name):
        logger.info(f"Bucket '{bucket_name}' does not exist")
        exit(1)

    logger.info(f"Bucket name: {bucket_name} ...")

    object_list: list = aws_session.retrieve_obj_list(bucket_name)
    

    for datafile in date_list:
        data_filename_date: str =datafile.strftime('%Y-%m-%d')
        data_filename_pattern: str = f"{data_filename_date}{extension}"
        data_filename_list: list = retrieve_objects_with_pattern(data_filename_pattern, object_list)
        if len(data_filename_list):
            for filename in data_filename_list:
                data_filename: str = filename
                logger.info(f"Downloading object {data_filename} ...")
                filename = data_filename
                if destination_path is not None:
                    filename: str = os.path.join(destination_path, data_filename)
                try:
                    #aws_session.download_object_from_bucket(data_filename, bucket_name, filename)
                    pass
                except ClientError as e:
                    logger.error(e)
        else:
            logger.info(f"Not object found for date '{data_filename_date}' with extension '{extension}'")

if __name__ == "__main__":
    sys.exit(main(sys.argv))
