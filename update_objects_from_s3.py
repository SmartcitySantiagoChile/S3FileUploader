import argparse
import gzip
import logging
import os
import sys
import zipfile
from botocore.exceptions import ClientError
from utils import (
    valid_date,
    get_date_list_between_two_given_dates,
    retrieve_objects_with_pattern,
    valid_three_tuple_list,
    get_file_object,
    update_file_by_tuples,
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

    # Iterate over dates
    for datafile in date_list:
        data_filename_date: str = datafile.strftime("%Y-%m-%d")
        data_filename_pattern: str = f"{data_filename_date}{extension}"

        # Check if object exist bucket
        data_filename_list: list = retrieve_objects_with_pattern(
            data_filename_pattern, object_list
        )
        if len(data_filename_list):
            # In case of more than one object
            for filename in data_filename_list:
                data_filename: str = filename
                filename = data_filename
                if destination_path is not None:
                    filename: str = os.path.join(destination_path, data_filename)
                try:
                    # Download the file
                    logger.info(f"Downloading object {data_filename} ...")
                    aws_session.download_object_from_bucket(
                        data_filename, bucket_name, filename
                    )
                    # Extract and make copy
                    uncompress_filename, compress_type = get_file_object(filename)
                    # Rename the old files
                    logger.info(
                        f"Object '{os.path.basename(filename)}' renamed to '{os.path.basename(filename)}.old-version'..."
                    )
                    os.rename(filename, filename + ".old-version")
                    if compress_type:
                        os.rename(
                            uncompress_filename, uncompress_filename + ".old-version"
                        )
                    # Update the copy
                    logger.info(
                        f"Updating object {os.path.basename(data_filename)} ..."
                    )
                    update_file_by_tuples(
                        uncompress_filename + ".old-version",
                        uncompress_filename,
                        tuples_list,
                    )
                    # Check if it was a compressed file
                    if compress_type == "zip":
                        logger.info(
                            f"Compressing object {os.path.basename(uncompress_filename)} to {os.path.basename(uncompress_filename)}.zip ..."
                        )
                        with zipfile.ZipFile(
                            filename, "w", zipfile.ZIP_DEFLATED
                        ) as zipf:
                            zipf.write(
                                uncompress_filename,
                                os.path.basename(uncompress_filename),
                            )
                        logger.info(
                            f"Removing object {os.path.basename(uncompress_filename)} ..."
                        )
                        os.remove(uncompress_filename)
                        logger.info(
                            f"Uploading object {os.path.basename(uncompress_filename)}.zip ..."
                        )
                        aws_session.send_file_to_bucket(
                           uncompress_filename + ".zip",
                            os.path.basename(uncompress_filename) + ".zip",
                            bucket_name,
                        )
                    elif compress_type == "gz":
                        logger.info(
                            f"Compressing object {os.path.basename(uncompress_filename)} to {os.path.basename(uncompress_filename)}.gz ..."
                        )
                        with open(uncompress_filename, "rb") as f_in, gzip.open(
                            uncompress_filename + ".gz", "wb"
                        ) as f_out:
                            buffer_size = 65536
                            while True:
                                buffer = f_in.read(buffer_size)
                                if not buffer:
                                    break
                                f_out.write(buffer)
                        logger.info(
                            f"Removing object {os.path.basename(uncompress_filename)} ..."
                        )
                        os.remove(uncompress_filename)
                        logger.info(
                            f"Uploading object {os.path.basename(uncompress_filename)}.gz ..."
                        )
                        aws_session.send_file_to_bucket(
                            uncompress_filename + ".gz",
                            os.path.basename(uncompress_filename) + ".gz",
                            bucket_name,
                        )
                        # Remove the copy
                        logger.info(
                            f"Removing object {os.path.basename(uncompress_filename)}.old-version ..."
                        )
                        os.remove(uncompress_filename + ".old-version")
                    else:
                        logger.info(
                            f"Uploading object {os.path.basename(uncompress_filename)} ..."
                        )
                        aws_session.send_file_to_bucket(
                            uncompress_filename,
                            os.path.basename(uncompress_filename),
                            bucket_name,
                        )
                    logger.info(
                            f"Object {os.path.basename(uncompress_filename)} uploaded succesfully ..."
                        )
                    
                except ClientError as e:
                    logger.error(e)
        else:
            logger.info(
                f"Not object found for date '{data_filename_date}' with extension '{extension}'"
            )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
