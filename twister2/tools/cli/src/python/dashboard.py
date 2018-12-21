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
''' cli_helper.py '''
import os
import twister2.tools.cli.src.python.args as args
import twister2.tools.cli.src.python.config as config
import twister2.tools.cli.src.python.execute as execute


################################################################################
def create_parser(subparsers):
    '''
    :param subparsers:
    :return:
    '''

    parser = subparsers.add_parser(
        'dash',
        help='Start dashboard process',
        usage="%(prog)s port",
        add_help=True
    )

    args.add_port(parser)

    parser.set_defaults(subcommand='dash')
    return parser


################################################################################
# pylint: disable=dangerous-default-value
def run(command, cl_args, action, extra_args=[], extra_lib_jars=[]):
    print("Starting dashboard...")

    result = execute.twister2_jar(
        os.path.join(config.get_twister2_lib_dir(), "twister2-dash-server_springboot.jar"),
        args=extra_args
    )

    err_msg = "Failed to start dashboard"
    succ_msg = "Successfully started dashboard"
    result.add_context(err_msg, succ_msg)
    return result
