# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
''' jars.py '''
import os
import fnmatch

import twister2.tools.cli.src.python.config as config


def pick(dirname, pattern):
    '''
    Get the job jars
    :param dirname:
    :param pattern:
    :return:
    '''
    file_list = fnmatch.filter(os.listdir(dirname), pattern)
    return file_list[0] if file_list else None


def job_jars():
    '''
    Get the job jars
    :return:
    '''
    jars = [
        os.path.join(config.get_twister2_lib_dir(), "*")
    ]
    return jars


def resource_scheduler_jars():
    '''
    Get the scheduler jars
    :return:
    '''
    jars = [
        os.path.join(config.get_twister2_lib_dir(), "*")
    ]
    return jars


def uploader_jars():
    '''
    Get the uploader jars
    :return:
    '''
    jars = [
        os.path.join(config.get_twister2_lib_dir(), "*")
    ]
    return jars


def statemgr_jars():
    '''
    Get the statemgr jars
    :return:
    '''
    jars = [
        os.path.join(config.get_twister2_lib_dir(), "*")
    ]
    return jars


def task_scheduler_jars():
    '''
    Get the packing algorithm jars
    :return:
    '''
    jars = [
        os.path.join(config.get_twister2_lib_dir(), "*")
    ]
    return jars
