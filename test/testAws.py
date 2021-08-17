from unittest import TestCase
from unittest import mock

from botocore.exceptions import ClientError

import aws


class AwsTest(TestCase):

    @mock.patch('aws.boto3.Session')
    def setUp(self, boto3_session):
        boto3_session.return_value = mock.MagicMock(resource=mock.MagicMock())
        self.aws_session = aws.AWSSession()

    def test_retrieve_obj_list(self):
        date = mock.MagicMock(size=1000, key='key', last_modified='today')
        dates = mock.Mock(return_value=[date])
        bucket = mock.MagicMock(objects=mock.MagicMock(all=dates))
        bucket.Bucket.return_value = bucket
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertEqual([{
            'name': 'key',
            'size': 0.00095367431640625,
            'last_modified': 'today',
            'url': 'https://s3.amazonaws.com/bucket_name/key'
        }], self.aws_session.retrieve_obj_list('bucket_name'))

    def test_check_bucket_exists_true(self):
        bucket = mock.MagicMock(
            meta=mock.MagicMock(client=mock.MagicMock(head_bucket=mock.MagicMock(return_value=True))))
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertTrue(self.aws_session.check_bucket_exists('bucket_name'))

    def test_check_bucket_exists_404_error(self):
        error = mock.MagicMock()
        error_response = {'Error': {'Code': '404'}}
        error.side_effect = ClientError(error_response=error_response, operation_name='mock error')
        bucket = mock.MagicMock(
            meta=mock.MagicMock(client=mock.MagicMock(head_bucket=error)))
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertFalse(self.aws_session.check_bucket_exists('bucket_name'))

    def test_check_bucket_exists_403_error(self):
        error = mock.MagicMock()
        error_response = {'Error': {'Code': '403'}}
        error.side_effect = ClientError(error_response=error_response, operation_name='mock error')
        bucket = mock.MagicMock(
            meta=mock.MagicMock(client=mock.MagicMock(head_bucket=error)))
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)

        with self.assertRaises(ValueError):
            self.aws_session.check_bucket_exists('bucket_name')

    def test_check_file_exists(self):
        load = mock.MagicMock()
        load.load.return_value = '2020-05-08.transaction.gz'
        bucket = mock.MagicMock()
        bucket.Object.return_value = load
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertTrue(self.aws_session.check_file_exists('2020-05-08.transaction.gz', 'key'))

    def test_check_file_exists_404_error(self):
        error = mock.MagicMock()
        error_response = {'Error': {'Code': '404'}}
        error.load.side_effect = ClientError(error_response=error_response, operation_name='mock error')
        bucket = mock.MagicMock()
        bucket.Object.return_value = error
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertFalse(self.aws_session.check_file_exists('2020-05-08.transaction.gz', 'key'))

    def test_check_file_exists_403_error(self):
        error = mock.MagicMock()
        error_response = {'Error': {'Code': '403'}}
        error.load.side_effect = ClientError(error_response=error_response, operation_name='mock error')
        bucket = mock.MagicMock()
        bucket.Object.return_value = error
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        with self.assertRaises(ValueError):
            self.aws_session.check_file_exists('2020-05-08.transaction.gz', 'key')

    def test__build_url(self):
        url = 'https://s3.amazonaws.com/bucket_name/key'
        self.assertEqual(url, self.aws_session._build_url('key', 'bucket_name'))

    @mock.patch('aws.AWSSession._build_url')
    def test_send_file_to_bucket(self, build_url):
        build_url.return_value = 'url'
        self.aws_session.session.resource = mock.MagicMock()
        self.assertEqual('url', self.aws_session.send_file_to_bucket('path', 'key', 'bucket_name'))

    @mock.patch('aws.AWSSession._build_url')
    def test_send_object_to_bucket(self, build_url):
        build_url.return_value = 'url'
        self.aws_session.session.resource = mock.MagicMock()
        self.assertEqual('url', self.aws_session.send_object_to_bucket('obj', 'key', 'bucket_name'))

    def test_delete_object_in_bucket(self):
        bucket = mock.MagicMock()
        bucket.Object.return_value = bucket
        bucket.delete.return_value = 'delete'
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.assertEqual('delete', self.aws_session.delete_object_in_bucket('key', 'bucket_name'))

    def test_download_object_from_bucket(self):
        bucket = mock.MagicMock(download_file=mock.MagicMock())
        bucket.Bucket.return_value = bucket
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.aws_session.download_object_from_bucket('key', 'name', 'path')

    def test_copy_files_from_bucket_to_bucket(self):
        target_bucket = mock.MagicMock(copy=mock.MagicMock())
        target_bucket.Bucket.return_value = target_bucket
        self.aws_session.session.resource = mock.MagicMock(return_value=target_bucket)
        self.aws_session.copy_file_from_bucket_to_bucket('source_bucket', 'target_bucket', 'file_name')

    def test_filter_by_extension(self):
        file_list = ['2020-01-01.trip.gz', '2020-01-01.viajes.gz']
        expected_list = ['2020-01-01.trip.gz']
        extension = ['.trip']
        self.assertEqual(expected_list, aws.filter_by_extension(file_list, extension))

        file_list = ['2020-01-01.trip.gz', '2020-01-01.viajes.gz']
        extension = ['.trip', '.viajes']
        self.assertEqual(file_list, aws.filter_by_extension(file_list, extension))

        file_list = ['2020-01-01.trip.gz', '2020-01-01.viajes.gz', '2020-01-01.trip', '2020-01-01.viajes']
        expected_list = ['2020-01-01.trip.gz', '2020-01-01.trip']
        extension = ['.trip']
        self.assertEqual(expected_list, aws.filter_by_extension(file_list, extension))

    def test_delete_bucket(self):
        file_list = [mock.MagicMock(key='2020-01-01.trip.gz')]
        object_bucket = mock.MagicMock(all=mock.MagicMock(return_value=file_list))
        bucket = mock.MagicMock()
        bucket.Bucket.return_value = mock.MagicMock(objects=object_bucket)
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        client = mock.MagicMock(delete_bucket=mock.MagicMock())
        self.aws_session.session.client = mock.MagicMock(return_value=client)
        self.aws_session.delete_object_in_bucket = mock.MagicMock()
        self.aws_session.delete_bucket('bucket_name')

    def test_move_files_from_bucket_to_bucket(self):
        # all files case
        source_bucket_name = 'source'
        target_bucket_name = 'target'
        datafiles = []
        extension_list = ['.trip']
        file_list = [mock.MagicMock(key='2020-01-01.trip.gz')]
        object_bucket = mock.MagicMock(all=mock.MagicMock(return_value=file_list))
        bucket = mock.MagicMock()
        bucket.Bucket.return_value = mock.MagicMock(objects=object_bucket)
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.aws_session.copy_file_from_bucket_to_bucket = mock.MagicMock()
        self.aws_session.delete_object_in_bucket = mock.MagicMock()
        self.aws_session.move_files_from_bucket_to_bucket(source_bucket_name, target_bucket_name, datafiles,
                                                          extension_list)
        # one file case
        datafiles = ['2020-01-01.trip.gz']
        object_bucket = mock.MagicMock(all=mock.MagicMock(return_value=file_list))
        bucket = mock.MagicMock()
        bucket.Bucket.return_value = mock.MagicMock(objects=object_bucket)
        self.aws_session.session.resource = mock.MagicMock(return_value=bucket)
        self.aws_session.copy_file_from_bucket_to_bucket = mock.MagicMock()
        self.aws_session.delete_object_in_bucket = mock.MagicMock()
        self.aws_session.move_files_from_bucket_to_bucket(source_bucket_name, target_bucket_name, datafiles,
                                                          extension_list)

        # one file does not exist case
        self.aws_session.check_file_exists = mock.MagicMock(return_value=False)
        self.aws_session.move_files_from_bucket_to_bucket(source_bucket_name, target_bucket_name, datafiles,
                                                          extension_list)
