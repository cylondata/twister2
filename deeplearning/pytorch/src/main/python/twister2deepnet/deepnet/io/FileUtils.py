#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  // http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

import os
import gzip
import tarfile
import zipfile
from zipfile import ZipFile
from gzip import GzipFile

from twister2deepnet.deepnet.exception.internal import ParameterError


class FileUtils:

    """
    TODO: refactor directory definitions
    """

    @staticmethod
    def check_exist_with_message(file_path=None, message='File Exists'):
        exists = False
        if os.path.exists(file_path):
            print("{}".format(file_path + ": {}".format(message)))
            exists = True
        return exists

    @staticmethod
    def mkdir(dir_path=None):
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path, 0o777)
            print("Directory Created")
        else:
            print("Directory Exists")

    @staticmethod
    def mkdir_with_access(dir_path=None):
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path, 0o777)
            print("Directory Created")
        else:
            print("Directory Exists")

    @staticmethod
    def mkdir_branch_with_access(dir_path=None):
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path, 0o777)
            print("Directory Created")
        else:
            print("Director Exists")

    @staticmethod
    def is_tar(filename):
        return filename.endswith(".tar")

    @staticmethod
    def is_targz(filename):
        return filename.endswith(".tar.gz")

    @staticmethod
    def is_gzip(filename):
        return filename.endswith(".gz") and not filename.endswith(".tar.gz")

    @staticmethod
    def is_zip(filename):
        return filename.endswith(".zip")

    @staticmethod
    def extract_archive(source_path, destination_path=None, clean=False):
        """
        This is a similar implementation referring to Pytorch Utils
        Reference:
        :param source_path: archive file source (str)
        :param destination_path: specific location to extract data (str)
        :param clean: remove the archive files after extracting (boolean)
        """
        if destination_path is None:
            destination_path = os.path.dirname(source_path)

        if FileUtils.is_tar(source_path):
            with tarfile.open(source_path, 'r') as tar:
                tar.extractall(path=destination_path)
        elif FileUtils.is_targz(source_path):
            with tarfile.open(source_path, 'r:gz') as tar:
                tar.extractall(path=destination_path)
        elif FileUtils.is_gzip(source_path):
            destination_path = os.path.join(destination_path,
                                            os.path.splitext(os.path.basename(source_path))[0])
            with open(destination_path, "wb") as output_file, GzipFile(source_path) as zip_file:
                output_file.write(zip_file.read())
        elif FileUtils.is_zip(source_path):
            with ZipFile(source_path, 'r') as zipfile:
                zipfile.extractall(destination_path)
        else:
            raise ParameterError("Unsupported Extract Format".format(source_path))

        if clean:
            os.remove(source_path)