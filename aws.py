import gzip
import logging
import os
import pathlib
import urllib
import zipfile

import boto3
import botocore
from decouple import config
from utils import retrieve_objects_with_pattern, get_file_object, update_file_by_tuples
from botocore.exceptions import ClientError


class AWSSession:
    """
    Class to interact wit Amazon Web Service (AWS) API through boto3 library
    """

    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
        )
        self.logger = logging.getLogger(__name__)

    def retrieve_obj_list(self, bucket_name):
        s3 = self.session.resource("s3")
        bucket = s3.Bucket(bucket_name)

        obj_list = []
        for obj in bucket.objects.all():
            size_in_mb = float(obj.size) / (1024**2)
            url = self._build_url(obj.key, bucket_name)
            obj_list.append(
                dict(
                    name=obj.key,
                    size=size_in_mb,
                    last_modified=obj.last_modified,
                    url=url,
                )
            )

        return obj_list

    def check_bucket_exists(self, bucket_name):
        s3 = self.session.resource("s3")
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 403:
                raise ValueError("Private Bucket. Forbidden Access!")
            elif error_code == 404:
                return False

    def check_file_exists(self, bucket_name, key):
        s3 = self.session.resource("s3")
        try:
            s3.Object(bucket_name, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise ValueError(e.response["Error"])
        else:
            # The object exists.
            return True

    def _build_url(self, key, bucket_name):
        return "".join(
            ["https://s3.amazonaws.com/", bucket_name, "/", urllib.parse.quote(key)]
        )

    def send_file_to_bucket(self, file_path, file_key, bucket_name):
        s3 = self.session.resource("s3")
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(file_path, file_key)

        return self._build_url(file_key, bucket_name)

    def send_object_to_bucket(self, obj, obj_key, bucket_name):
        s3 = self.session.resource("s3")
        bucket = s3.Bucket(bucket_name)
        bucket.upload_fileobj(obj, obj_key)
        s3.Object(bucket_name, obj_key).Acl().put(ACL="public-read")

        return self._build_url(obj_key, bucket_name)

    def delete_object_in_bucket(self, obj_key, bucket_name):
        s3 = self.session.resource("s3")
        obj = s3.Object(bucket_name, obj_key)

        return obj.delete()

    def download_object_from_bucket(self, obj_key, bucket_name, file_path):
        s3 = self.session.resource("s3")
        bucket = s3.Bucket(bucket_name)
        bucket.download_file(obj_key, file_path)

    def copy_file_from_bucket_to_bucket(
        self, source_bucket_name, target_bucket_name, file_name
    ) -> None:
        """
        Copy file from source bucket to target bucket
        Args:
            source_bucket_name: source bucket
            target_bucket_name: target bucket
            file_name: file name
        """
        s3 = self.session.resource("s3")
        target_bucket = s3.Bucket(target_bucket_name)
        copy_source = {"Bucket": source_bucket_name, "Key": file_name}
        target_bucket.copy(copy_source, file_name)

    def move_files_from_bucket_to_bucket(
        self,
        source_bucket_name: str,
        target_bucket_name: str,
        datafiles: list,
        extension_list: list,
    ) -> None:
        """
        Move files from source bucket to target bucket
        Args:
            source_bucket_name: source bucket
            target_bucket_name: target bucket
            datafiles: list of files to move (optional)
            extension_list: list of extension filter (op)
        """
        s3 = self.session.resource("s3")
        source_bucket = s3.Bucket(source_bucket_name)

        if not datafiles:
            datafiles = [obj.key for obj in source_bucket.objects.all()]
            if extension_list:
                datafiles = filter_by_extension(datafiles, extension_list)
            for file in datafiles:
                self.copy_file_from_bucket_to_bucket(
                    source_bucket_name, target_bucket_name, file
                )
                self.delete_object_in_bucket(file, source_bucket_name)
                self.logger.info(
                    f"{file} moved from {source_bucket_name} to {target_bucket_name}"
                )
        else:
            if extension_list:
                datafiles = filter_by_extension(datafiles, extension_list)
            for file_name in datafiles:
                if self.check_file_exists(source_bucket_name, file_name):
                    self.copy_file_from_bucket_to_bucket(
                        source_bucket_name, target_bucket_name, file_name
                    )
                    self.delete_object_in_bucket(file_name, source_bucket_name)
                    self.logger.info(
                        f"{file_name} moved from {source_bucket_name} to {target_bucket_name}"
                    )
                else:
                    self.logger.info(
                        f"{file_name} does not exist in {source_bucket_name}"
                    )

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete bucket with all files
        Args:
            bucket_name: name of bucket to delete
        """
        s3 = self.session.resource("s3")
        client = self.session.client("s3")
        bucket = s3.Bucket(bucket_name)
        for file in bucket.objects.all():
            self.delete_object_in_bucket(file.key, bucket_name)
            self.logger.info(f"{file.key} deleted")

        client.delete_bucket(Bucket=bucket_name)
        self.logger.info(f"{bucket_name} deleted")

    def update_files_from_bucket(
        self,
        date_list: list,
        bucket_name: str,
        extension: str,
        tuples_list: list,
        destination_path: str,
    ) -> None:
        object_list: list = self.retrieve_obj_list(bucket_name)
        for datafile in date_list:
            data_filename_date: str = datafile.strftime("%Y-%m-%d")
            data_filename_pattern: str = f"{data_filename_date}{extension}"

            # Check if object exist bucket
            data_filename_list: list = retrieve_objects_with_pattern(
                data_filename_pattern, object_list
            )
            if len(data_filename_list):
                # In case of more than one object
                for filename in data_filename_list:
                    data_filename: str = filename
                    filename = data_filename
                    if destination_path is not None:
                        filename: str = os.path.join(destination_path, data_filename)
                    try:
                        # Download the file
                        self.logger.info(f"Downloading object {data_filename} ...")
                        self.download_object_from_bucket(
                            data_filename, bucket_name, filename
                        )
                        # Extract and make copy
                        uncompress_filename, compress_type = get_file_object(filename)
                        # Rename the copy
                        self.logger.info(
                            f"Object '{os.path.basename(filename)}' renamed to '{os.path.basename(filename)}.old-version'..."
                        )
                        os.rename(filename, filename + ".old-version")
                        if compress_type:
                            os.rename(
                                uncompress_filename,
                                uncompress_filename + ".old-version",
                            )
                        # Update the copy
                        self.logger.info(
                            f"Updating object {os.path.basename(data_filename)} ..."
                        )
                        update_file_by_tuples(
                            uncompress_filename + ".old-version",
                            uncompress_filename,
                            tuples_list,
                        )
                        # Check if it was a compressed file
                        uncompress_filename_basename: str = os.path.basename(
                            uncompress_filename
                        )

                        # Case is not compressed file
                        compress_filename: str = uncompress_filename
                        compress_filename_basename: str = uncompress_filename_basename

                        if compress_type == "zip":
                            # Update compressed filenames
                            compress_filename = f"{uncompress_filename}.zip"
                            compress_filename_basename = os.path.basename(
                                compress_filename
                            )
                            self.logger.info(
                                f"Compressing object {uncompress_filename_basename} to {compress_filename_basename} ..."
                            )
                            # Compress file
                            with zipfile.ZipFile(
                                filename, "w", zipfile.ZIP_DEFLATED
                            ) as zipf:
                                zipf.write(
                                    uncompress_filename,
                                    uncompress_filename_basename,
                                )
                            # Remove extracted copy
                            os.remove(uncompress_filename + ".old-version")
                        elif compress_type == "gz":
                            # Update compressed filenames
                            compress_filename = f"{uncompress_filename}.gz"
                            compress_filename_basename = os.path.basename(
                                compress_filename
                            )

                            self.logger.info(
                                f"Compressing object {uncompress_filename_basename} to {compress_filename_basename} ..."
                            )
                            # Compress file
                            with open(uncompress_filename, "rb") as f_in, gzip.open(
                                compress_filename, "wb"
                            ) as f_out:
                                buffer_size = 65536
                                while True:
                                    buffer = f_in.read(buffer_size)
                                    if not buffer:
                                        break
                                    f_out.write(buffer)
                            # Remove extracted copy
                            self.logger.info(
                                f"Removing object {uncompress_filename_basename}.old-version ..."
                            )
                            os.remove(uncompress_filename + ".old-version")

                        self.logger.info(
                            f"Uploading object {compress_filename_basename} ..."
                        )
                        self.send_file_to_bucket(
                            compress_filename,
                            compress_filename_basename,
                            bucket_name,
                        )
                        self.logger.info(
                            f"Removing object {compress_filename_basename} ..."
                        )
                        os.remove(compress_filename)
                        self.logger.info(
                            f"Object {uncompress_filename_basename} uploaded succesfully ..."
                        )

                    except ClientError as e:
                        self.logger.error(e)
            else:
                self.logger.info(
                    f"Not object found for date '{data_filename_date}' with extension '{extension}'"
                )


def filter_by_extension(file_list: list, extension_list: list) -> list:
    """
    Filter file_list returning only extension_list
    Args:
        file_list: list with filenames
        extension_list: list with extensions

    Returns:
        list: filtered list
    """
    return [
        file
        for file in file_list
        if any(ext in pathlib.Path(file).suffixes for ext in extension_list)
    ]
