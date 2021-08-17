import argparse
import logging
import os
import sys

from botocore.exceptions import ClientError

# add path so we can use function through command line
new_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.append(new_path)

from aws import AWSSession


def main(argv):
    """
    This script will download one or more objects from S3 bucket.
    """

    # Arguments and description
    parser = argparse.ArgumentParser(description='download one or more objects from S3 bucket')

    parser.add_argument('filename', nargs='+', help='one or more filenames')
    parser.add_argument('bucket', default=None, help='bucket name')
    parser.add_argument('--destination-path', default=None,
                        help='path where files will be saved, if it is not provided we will use current path')

    args = parser.parse_args(argv[1:])

    # Give names to arguments
    datafiles = args.filename
    bucket_name = args.bucket
    destination_path = args.destination_path
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    if destination_path is not None and not os.path.isdir(destination_path):
        logger.info(f"Path \'{destination_path}\' is not valid")
        exit(1)

    aws_session = AWSSession()

    if not aws_session.check_bucket_exists(bucket_name):
        logger.info(f"Bucket \'{bucket_name}\' does not exist")
        exit(1)

    for datafile in datafiles:
        filename = datafile
        if destination_path is not None:
            filename = os.path.join(destination_path, datafile)
        logger.info(f"downloading object {datafile} ...")
        try:
            aws_session.download_object_from_bucket(datafile, bucket_name, filename)
        except ClientError as e:
            logger.error(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
