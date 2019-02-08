import io
import os
from contextlib import redirect_stdout
from unittest import TestCase, mock

from botocore.exceptions import ClientError

from delete_object_in_s3 import main as delete_main
from upload_to_s3 import main as upload_main


class DeleteObjectTest(TestCase):

    def setUp(self):
        self.command_name = 'delete_object_in_s3'

    def test_without_params(self):
        with self.assertRaises(SystemExit):
            delete_main([self.command_name])

    def test_with_one_param(self):
        filename = 'aaa.txt'

        with self.assertRaises(SystemExit):
            delete_main([self.command_name, filename])

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_does_not_exist(self, aws_session_mock):
        """  bucket does not exist """
        aws_session_mock.return_value.check_bucket_exists.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(SystemExit):
                delete_main([self.command_name, filename, bucket_name])

                self.assertIn('Bucket \'{0}\' does not exist'.format(bucket_name), f.getvalue())

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_is_deleted(self, aws_session_mock):
        """  delete object in bucket """
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.delete_object_in_bucket.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            delete_main([self.command_name, filename, bucket_name])

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

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            delete_main([self.command_name, filename, bucket_name])

            expected_answer = 'An error occurred ({0}) when calling the delete operation: {1}'.format(
                error_response['Error']['Code'], error_response['Error']['Message'])
            self.assertIn(expected_answer, f.getvalue())

        aws_session_mock.return_value.delete_object_in_bucket.assert_called_once()
        aws_session_mock.return_value.delete_object_in_bucket.assert_called_with(filename, bucket_name)


class UploadObjectTest(TestCase):

    def setUp(self):
        self.command_name = 'upload_to_s3.py'

    def test_without_params(self):
        with self.assertRaises(SystemExit):
            upload_main([self.command_name])

    def test_with_one_param(self):
        filename = 'aaa.txt'

        with self.assertRaises(SystemExit):
            upload_main([self.command_name, filename])

    @mock.patch('upload_to_s3.AWSSession')
    def test_bucket_does_not_exist(self, aws_session_mock):
        """  bucket does not exist """
        aws_session_mock.return_value.check_bucket_exists.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(SystemExit):
                upload_main([self.command_name, filename, bucket_name])

                self.assertIn('Bucket \'{0}\' does not exist'.format(bucket_name), f.getvalue())

    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket(self, glob_mock, aws_session_mock):
        """  move file to bucket, filename has ok format and file does not exist """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = False
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name])
            self.assertIn('uploading file ', f.getvalue())
            self.assertIn('finished load of file', f.getvalue())

        mock_call.assert_called_once()
        mock_call.assert_called_with(filepath, filename, bucket_name)

    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_with_wrong_filename(self, glob_mock, aws_session_mock):
        """  filename does not have date format """
        filename = 'no_date_format.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        glob_mock.glob.return_value = [filepath]

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name])
            self.assertIn('does not have a valid format name', f.getvalue())

    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_with_wrong_filename_and_omit_filename_check_param(self, glob_mock, aws_session_mock):
        """  move file to bucket, filename does not have date format but was added omit-filename-check param """
        filename = 'no_date_format.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = False
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name, '--omit-filename-check'])
            self.assertNotIn('does not have a valid format name', f.getvalue())
            self.assertIn('uploading file ', f.getvalue())
            self.assertIn('finished load of file', f.getvalue())

        mock_call.assert_called_once()
        mock_call.assert_called_with(filepath, filename, bucket_name)

    @mock.patch('builtins.input')
    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_but_file_exists_and_it_is_replaced(self, glob_mock, aws_session_mock, input_mock):
        """  move file to bucket, filename has ok format but file exists, we answer yes to replace question """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = True
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        input_mock.return_value = 'y'

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name])
            self.assertIn('uploading file ', f.getvalue())
            self.assertIn('finished load of file', f.getvalue())

        mock_call.assert_called_once()
        mock_call.assert_called_with(filepath, filename, bucket_name)

    @mock.patch('builtins.input')
    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_but_file_exists_and_it_is_replaced_without_user_input(self, glob_mock,
                                                                                       aws_session_mock, input_mock):
        """  move file to bucket, filename has ok format but file exists, we add --replace input """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = True
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name, '--replace'])
            self.assertIn('uploading file ', f.getvalue())
            self.assertIn('finished load of file', f.getvalue())

        input_mock.assert_not_called()
        mock_call.assert_called_once()
        mock_call.assert_called_with(filepath, filename, bucket_name)

    @mock.patch('builtins.input')
    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_but_file_exists_and_it_is_not_replaced(self, glob_mock, aws_session_mock, input_mock):
        """  move file to bucket, filename has ok format but file exists, we answer not to replace question """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = True
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        input_mock.return_value = 'n'

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name])
            self.assertIn('uploading file ', f.getvalue())
            self.assertIn('file {0} was not replaced'.format(filename), f.getvalue())
            self.assertNotIn('finished load of file', f.getvalue())

        mock_call.assert_not_called()

    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_with_client_error(self, glob_mock, aws_session_mock):
        """  move file to bucket, filename has ok format and file does not exist """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        operation_name = 'delete'
        error_response = dict(Error=dict(Code=403, Message='forbidden'))
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = False
        aws_session_mock.return_value.send_file_to_bucket.side_effect = ClientError(error_response, operation_name)

        glob_mock.glob.return_value = [filepath]

        f = io.StringIO()
        with redirect_stdout(f):
            upload_main([self.command_name, filepath, bucket_name])
            expected_answer = 'An error occurred ({0}) when calling the delete operation: {1}'.format(
                error_response['Error']['Code'], error_response['Error']['Message'])
            self.assertIn(expected_answer, f.getvalue())
