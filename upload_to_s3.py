import argparse
import glob
import logging
import os
import sys
from datetime import datetime

from botocore.exceptions import ClientError

# add path so we can use function through command line
new_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.append(new_path)

from aws import AWSSession


def main(argv):
    """
    This script will move a file to S3 bucket automatically.
    Check bucket, file name and
    """

    # Arguments and description
    parser = argparse.ArgumentParser(description='move document to S3 bucket')

    parser.add_argument('file', nargs='+',
                        help='data file path. It can be a pattern, e.g. /path/to/file or /path/to/file*.zip')
    parser.add_argument('bucket', default=None, help='bucket name. Valid options are: ')
    parser.add_argument('--omit-filename-check', action='store_true',
                        help='It Accepts filenames with distinct format to YYYY-mm-dd.*')
    parser.add_argument('--replace', action='store_true',
                        help='It replaces file if exists in bucket, default behavior ask to user a confirmation')
    parser.add_argument('--ignore-if-exists', action='store_true',
                        help='It does not upload file if already exist in the bucket')

    args = parser.parse_args(argv[1:])

    # Give names to arguments
    datafiles = args.file
    bucket_name = args.bucket
    omit_filename_check = args.omit_filename_check
    replace = args.replace
    ignore_if_exists = args.ignore_if_exists

    aws_session = AWSSession()
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    if replace and ignore_if_exists:
        logger.info('replace and ignore-if-exists options are incompatible')
        exit(1)
    
    if not aws_session.check_bucket_exists(bucket_name):
        logger.info(f"Bucket {bucket_name} does not exist")
        exit(1)
    
    def send_file_to_s3(matched_file, filename):
        logger.info(f"{datetime.now().replace(microsecond=0)}: uploading file {matched_file}")
        aws_session.send_file_to_bucket(matched_file, filename, bucket_name)
        logger.info(f"{datetime.now().replace(microsecond=0)}: finished load of file {matched_file}")
        
    for datafile in datafiles:
        matched_files = glob.glob(datafile)
        if len(matched_files) == 0:
            logger.info(f'path "{datafile}" does not match with any file')
            continue

        for matched_file in matched_files:
            filename = matched_file.split(os.sep)[-1]
            if not omit_filename_check:
                filename_date_part = filename.split('.')[0]
                try:
                    datetime.strptime(filename_date_part, "%Y-%m-%d")
                except ValueError:
                    logger.error(f'\'{filename}\' does not have a valid format name')
                    continue

            try:
                file_exists = aws_session.check_file_exists(bucket_name, filename)
                if not file_exists:
                    send_file_to_s3(matched_file, filename)
                    continue

                if replace:
                    send_file_to_s3(matched_file, filename)
                elif ignore_if_exists:
                    continue
                else:
                    answer = input(f'file \'{filename}\' exists in bucket. Do you want to replace it? (y/n): ')
                    if answer not in ['y', 'Y']:
                        logger.info(f"file {filename} was not replaced")
                        continue
                    send_file_to_s3(matched_file, filename)
            except ClientError as e:
                # ignore it and continue uploading files
                logger.error(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
