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
    This script will move one or more objects from S3 bucket to another S3 bucket.
    """

    # Arguments and description
    parser = argparse.ArgumentParser(description='move one or more objects from source S3 bucket to target S3 bucket')

    parser.add_argument('source_bucket', help='source bucket name')
    parser.add_argument('target_bucket', help='target bucket name')
    parser.add_argument('-f', '--filename', dest='filename', default=None, nargs='*', help='one or more filenames')
    parser.add_argument('-e', dest='extension_filter', default=None, nargs='*',
                        help='only files with this extension will be moved')

    args = parser.parse_args(argv[1:])

    # Give names to arguments
    source_bucket_name = args.source_bucket
    target_bucket_name = args.target_bucket
    datafiles = args.filename
    extension = args.extension_filter

    aws_session = AWSSession()

    if not aws_session.check_bucket_exists(source_bucket_name):
        print(f"Bucket {source_bucket_name} does not exist")
        exit(1)

    if not aws_session.check_bucket_exists(target_bucket_name):
        print(f"Bucket {target_bucket_name} does not exist")
        exit(1)
    try:
        aws_session.move_files_from_bucket_to_bucket(source_bucket_name, target_bucket_name, datafiles, extension)
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
