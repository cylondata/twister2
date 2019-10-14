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
import twister2.tools.cli.src.python.submit as submit


################################################################################
def create_parser(subparsers):
    '''
    :param subparsers:
    :return:
    '''

    parser = subparsers.add_parser(
        'restart',
        help='Restart a job from a checkpoint',
        usage="%(prog)s job-id",
        add_help=True
    )

    args.add_cluster_role_env(parser)
    args.add_job_id(parser)
    args.add_debug(parser)

    parser.set_defaults(subcommand='restart')
    return parser


################################################################################
# pylint: disable=dangerous-default-value
def run(command, parser, command_args, unknown_args):
    print("Restarting JOB...")
    job_file_name = os.path.join(config.get_twister2_lib_dir(), "librestarter-java.jar")
    print(job_file_name)

    command_args["job-file-name"] = job_file_name
    command_args["job-type"] = "jar"
    command_args["job-class-name"] = "edu.iu.dsc.tws.restarter.CheckpointedJobRestarter"

    return submit.run(command, parser, command_args, [command_args['job-id']])
