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
    This script remove an object from S3 bucket.
    """
    # Arguments and description
    parser = argparse.ArgumentParser(description='delete an object from S3 bucket')

    parser.add_argument('filename', help='filename to delete in bucket')
    parser.add_argument('bucket', default=None, help='bucket name. Valid options are: ')

    args = parser.parse_args(argv[1:])

    # Give names to arguments
    filename = args.filename
    bucket_name = args.bucket

    aws_session = AWSSession()
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    if not aws_session.check_bucket_exists(bucket_name):
        logger.info(f"Bucket {bucket_name} does not exist")
        exit(1)

    try:
        aws_session.delete_object_in_bucket(filename, bucket_name)
        logger.info(f"Object {filename} was deleted successfully!")
    except ClientError as e:
        # ignore it and continue uploading files
        logger.error(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
