from unittest import TestCase, mock
from botocore.exceptions import ClientError

from delete_object_in_s3 import main as delete_main
from upload_to_s3 import main as upload_main

from contextlib import redirect_stdout

import io


class DeleteObjectTest(TestCase):

    def test_without_params(self):
        command_name = ''

        with self.assertRaises(SystemExit):
            delete_main([command_name])

    def test_with_one_param(self):
        command_name = ''
        filename = 'aaa.txt'

        with self.assertRaises(SystemExit):
            delete_main([command_name, filename])

        self.assertEqual(4, 4)

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_does_not_exist(self, aws_session_mock):
        """  bucket does not exist """
        aws_session_mock.return_value.check_bucket_exists.return_value = False

        command_name = ''
        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(SystemExit):
                delete_main([command_name, filename, bucket_name])

                self.assertIn('Bucket \'{0}\' does not exist'.format(bucket_name), f.getvalue())

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_is_deleted(self, aws_session_mock):
        """  delete object in bucket """
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.delete_object_in_bucket.return_value = False

        command_name = ''
        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            delete_main([command_name, filename, bucket_name])

            self.assertIn('Object {0} was deleted successfully!'.format(filename), f.getvalue())

        aws_session_mock.return_value.delete_object_in_bucket.assert_called_once()
        aws_session_mock.return_value.delete_object_in_bucket.assert_called_with(filename, bucket_name)

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_raise_error_when_is_deleting(self, aws_session_mock):
        """  delete object in bucket """
        operation_name = 'delete'
        error_response = dict(Error=dict(Code=403, Message='forbidden'))
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.delete_object_in_bucket.side_effect = ClientError(error_response, operation_name)

        command_name = ''
        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            delete_main([command_name, filename, bucket_name])

            expected_answer = 'An error occurred ({0}) when calling the delete operation: {1}'.format(
                error_response['Error']['Code'], error_response['Error']['Message'])
            self.assertIn(expected_answer, f.getvalue())

        aws_session_mock.return_value.delete_object_in_bucket.assert_called_once()
        aws_session_mock.return_value.delete_object_in_bucket.assert_called_with(filename, bucket_name)
