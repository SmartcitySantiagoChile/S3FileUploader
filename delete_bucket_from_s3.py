import argparse
import os
import sys

from botocore.exceptions import ClientError

# add path so we can use function through command line
new_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.append(new_path)

from aws import AWSSession


def main(argv):
    """
    This script will delete a S3 bucket.
    """

    # Arguments and description
    parser = argparse.ArgumentParser(description='delete S3 bucket')

    parser.add_argument('bucket_name', help='bucket name')

    args = parser.parse_args(argv[1:])

    # Give names to arguments
    bucket_name = args.bucket_name

    aws_session = AWSSession()

    if not aws_session.check_bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist")
        exit(1)

    try:
        aws_session.delete_bucket(bucket_name)
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
