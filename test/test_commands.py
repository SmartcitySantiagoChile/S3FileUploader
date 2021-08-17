import io
import os
from contextlib import redirect_stdout
from unittest import TestCase, mock

from botocore.exceptions import ClientError

from delete_bucket_from_s3 import main as delete_bucket_main
from delete_object_in_s3 import main as delete_main
from download_from_s3 import main as download_main
from move_bucket_from_s3 import main as move_bucket_main
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

        with self.assertLogs('delete_object_in_s3', level='INFO') as f:
            with self.assertRaises(SystemExit):
                delete_main([self.command_name, filename, bucket_name])

        expected_answer = 'INFO:delete_object_in_s3:Bucket aarrrp does not exist'
        self.assertIn(expected_answer, f.output)

    @mock.patch('delete_object_in_s3.AWSSession')
    def test_bucket_is_deleted(self, aws_session_mock):
        """  delete object in bucket """
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.delete_object_in_bucket.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        with self.assertLogs('delete_object_in_s3', level='INFO') as f:
            delete_main([self.command_name, filename, bucket_name])

        expected_answer = 'INFO:delete_object_in_s3:Object aaa.txt was deleted successfully!'
        self.assertIn(expected_answer, f.output)

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

        with self.assertLogs('delete_object_in_s3', level='INFO') as f:
            delete_main([self.command_name, filename, bucket_name])

        expected_answer = 'ERROR:delete_object_in_s3:An error occurred (403) when calling the delete operation: forbidden'
        self.assertIn(expected_answer, f.output)

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            with self.assertRaises(SystemExit):
                upload_main([self.command_name, filename, bucket_name])
        expected_answer = "INFO:upload_to_s3:Bucket aarrrp does not exist"
        self.assertIn(expected_answer, f.output)

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name])

        self.assertIn('uploading file', f.output[0])
        self.assertIn('finished load of file', f.output[1])

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name])
        self.assertIn('does not have a valid format name', f.output[0])

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name, '--omit-filename-check'])
        self.assertNotIn('does not have a valid format name', f.output[0])
        self.assertIn('uploading file ', f.output[0])
        self.assertIn('finished load of file', f.output[1])

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name])
        self.assertIn('uploading file ', f.output[0])
        self.assertIn('finished load of file', f.output[1])

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

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name, '--replace'])
        self.assertIn('uploading file ', f.output[0])
        self.assertIn('finished load of file', f.output[1])

        input_mock.assert_not_called()
        mock_call.assert_called_once()
        mock_call.assert_called_with(filepath, filename, bucket_name)

    @mock.patch('builtins.input')
    @mock.patch('upload_to_s3.AWSSession')
    @mock.patch('upload_to_s3.glob')
    def test_move_file_to_bucket_but_file_exists_and_it_is_not_replaced(self, glob_mock, aws_session_mock, input_mock):
        """  move file to bucket, filename has ok format but file exists, we answer "not" to replace question """
        filename = '2018-01-01.txt'
        filepath = os.path.join(__file__, filename)
        bucket_name = 'aarrrp'

        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.check_file_exists.return_value = True
        mock_call = aws_session_mock.return_value.send_file_to_bucket
        glob_mock.glob.return_value = [filepath]

        input_mock.return_value = 'n'

        with self.assertLogs('upload_to_s3', level='INFO') as f:
            upload_main([self.command_name, filepath, bucket_name])
        self.assertIn('file {0} was not replaced'.format(filename), f.output[0])
        self.assertNotIn('finished load of file', f.output[0])

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

        with self.assertLogs('upload_to_s3', level='ERROR') as f:
            upload_main([self.command_name, filepath, bucket_name])
            expected_answer = f"An error occurred ({error_response['Error']['Code']}) when calling the delete " \
                              f"operation: {error_response['Error']['Message']}"
            self.assertIn(expected_answer, f.output[0])


class DownloadObjectTest(TestCase):

    def setUp(self):
        self.command_name = 'download_from_s3'

    def test_without_params(self):
        with self.assertRaises(SystemExit):
            download_main([self.command_name])

    def test_with_one_param(self):
        filename = 'aaa.txt'

        with self.assertRaises(SystemExit):
            download_main([self.command_name, filename])

    @mock.patch('download_from_s3.AWSSession')
    def test_bucket_does_not_exist(self, aws_session_mock):
        """ bucket does not exist """
        aws_session_mock.return_value.check_bucket_exists.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        with self.assertLogs('download_from_s3', level='INFO') as f:
            with self.assertRaises(SystemExit):
                download_main([self.command_name, filename, bucket_name])

        self.assertIn(f'Bucket \'{bucket_name}\' does not exist', f.output[0])

    def test_path_is_not_valid(self):
        """ path is not valid """
        filename = 'aaa.txt'
        bucket_name = 'aarrrp'
        dest_option = '--destination-path'
        destination_path = 'asdasd-sf-sf'

        f = io.StringIO()
        with self.assertLogs('download_from_s3', level='INFO') as f:
            with self.assertRaises(SystemExit):
                download_main([self.command_name, filename, bucket_name, dest_option, destination_path])

        self.assertIn(f'Path \'{destination_path}\' is not valid', f.output[0])

    @mock.patch('download_from_s3.AWSSession')
    def test_bucket_is_downloaded(self, aws_session_mock):
        """ download object in bucket """
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.download_object_from_bucket.return_value = False

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'
        dest_option = '--destination-path'
        destination_path = str(os.getcwd())

        with self.assertLogs('download_from_s3', level='INFO') as f:
            download_main([self.command_name, filename, bucket_name, dest_option, destination_path])

        self.assertIn(f'downloading object {filename} ...', f.output[0])

        destination_path = os.path.join(destination_path, filename)
        aws_session_mock.return_value.download_object_from_bucket.assert_called_once()
        aws_session_mock.return_value.download_object_from_bucket.assert_called_with(filename, bucket_name,
                                                                                     destination_path)

    @mock.patch('download_from_s3.AWSSession')
    def test_bucket_raise_error_when_is_downloading(self, aws_session_mock):
        """ download object in bucket """
        operation_name = 'download'
        error_response = dict(Error=dict(Code=403, Message='forbidden'))
        aws_session_mock.return_value.check_bucket_exists.return_value = True
        aws_session_mock.return_value.download_object_from_bucket.side_effect = ClientError(error_response,
                                                                                            operation_name)

        filename = 'aaa.txt'
        bucket_name = 'aarrrp'

        with self.assertLogs('download_from_s3', level='INFO') as f:
            download_main([self.command_name, filename, bucket_name])

        expected_answer = f"An error occurred ({error_response['Error']['Code']}) when calling the download operation: {error_response['Error']['Message']}"
        self.assertIn(expected_answer, f.output[1])

        aws_session_mock.return_value.download_object_from_bucket.assert_called_once()
        aws_session_mock.return_value.download_object_from_bucket.assert_called_with(filename, bucket_name, filename)


class DeleteBucketTest(TestCase):
    def setUp(self):
        self.command_name = 'move_bucket_from_s3'

    def test_without_params(self):
        with self.assertRaises(SystemExit):
            move_bucket_main([self.command_name])

    @mock.patch('delete_bucket_from_s3.AWSSession.check_bucket_exists')
    def test_with_bucket_does_not_exist(self, check_bucket_exist):
        check_bucket_exist.return_value = False
        source = 'source'
        with self.assertRaises(SystemExit):
            delete_bucket_main([self.command_name, source])

    @mock.patch('delete_bucket_from_s3.AWSSession.delete_bucket')
    @mock.patch('delete_bucket_from_s3.AWSSession.check_bucket_exists')
    def test_bucket_raise_error_when_is_deleting(self, check_bucket_exists, delete_bucket):
        operation_name = 'deleting'
        error_response = dict(Error=dict(Code=403, Message='forbidden'))
        check_bucket_exists.return_value = True
        delete_bucket.side_effect = ClientError(error_response, operation_name)

        bucket_name = 'void_bucket'

        with self.assertLogs('delete_bucket_from_s3', level='INFO') as f:
            delete_bucket_main([self.command_name, bucket_name])

        expected_answer = 'ERROR:delete_bucket_from_s3:An error occurred (403) when calling the deleting operation: ' \
                          'forbidden'
        self.assertIn(expected_answer, f.output)

        delete_bucket.assert_called_once()
        delete_bucket.assert_called_with(bucket_name)


class MoveBucketTest(TestCase):
    def setUp(self):
        self.command_name = 'move_bucket_from_s3'

    def test_without_params(self):
        with self.assertRaises(SystemExit):
            move_bucket_main([self.command_name])

    def test_with_one_param(self):
        source_bucket = 'source'
        with self.assertRaises(SystemExit):
            move_bucket_main([self.command_name, source_bucket])

    @mock.patch('move_bucket_from_s3.AWSSession.check_bucket_exists')
    def test_with_bucket_does_not_exist(self, check_bucket_exist):
        check_bucket_exist.return_value = False
        source = 'source'
        target = 'target'
        with self.assertRaises(SystemExit):
            move_bucket_main([self.command_name, source, target])

    @mock.patch('move_bucket_from_s3.AWSSession.check_bucket_exists')
    def test_with_target_bucket_does_not_exist(self, check_bucket_exist):
        check_bucket_exist.side_effect = [True, False]
        source = 'source'
        target = 'target'
        with self.assertRaises(SystemExit):
            move_bucket_main([self.command_name, source, target])

    @mock.patch('move_bucket_from_s3.AWSSession.move_files_from_bucket_to_bucket')
    @mock.patch('move_bucket_from_s3.AWSSession.check_bucket_exists')
    def test_bucket_raise_error_when_is_moving(self, check_bucket_exists, move_files_from_bucket_to_bucket):
        operation_name = 'moving'
        error_response = dict(Error=dict(Code=403, Message='forbidden'))
        check_bucket_exists.return_value = [True, True]
        move_files_from_bucket_to_bucket.side_effect = ClientError(error_response, operation_name)
        source = 'source'
        target = 'target'

        with self.assertLogs('move_bucket_from_s3', level='INFO') as f:
            move_bucket_main([self.command_name, source, target])

        expected_answer = 'ERROR:move_bucket_from_s3:An error occurred (403) when calling the moving operation: ' \
                          'forbidden'
        self.assertIn(expected_answer, f.output)

        move_files_from_bucket_to_bucket.assert_called_once()
        move_files_from_bucket_to_bucket.assert_called_with(source, target, None, None)
