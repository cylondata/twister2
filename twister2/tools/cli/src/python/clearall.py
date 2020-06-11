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

''' clearall.py '''

import twister2.tools.cli.src.python.cli_helper as cli_helper

################################################################################
def create_parser(subparsers):
    '''
    :param subparsers:
    :return:
    '''
    helpMsg = 'clear leftover resources for all checkpointed jobs'
    return cli_helper.create_parser_without_jobid(subparsers, 'clearall', helpMsg)

################################################################################
# pylint: disable=dangerous-default-value
def run(command, parser, cl_args, unknown_args):
    '''
    :param command:
    :param parser:
    :param cl_args:
    :param unknown_args:
    :return:
    '''
    return cli_helper.run_without_jobid(command, cl_args, "clearall job")
