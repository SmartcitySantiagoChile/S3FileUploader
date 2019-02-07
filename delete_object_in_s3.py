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

    if not aws_session.check_bucket_exists(bucket_name):
        print('Bucket \'{0}\' does not exist'.format(bucket_name))
        exit(1)

    try:
        aws_session.delete_object_in_bucket(filename, bucket_name)
        print('Object {0} was deleted successfully!'.format(filename))
    except ClientError as e:
        # ignore it and continue uploading files
        print(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
