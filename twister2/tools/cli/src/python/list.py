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
''' list.py '''
import os
import logging
import twister2.tools.cli.src.python.args as args
import twister2.tools.cli.src.python.config as config
import twister2.tools.cli.src.python.jars as jars
import twister2.tools.cli.src.python.execute as execute

from twister2.tools.cli.src.python.log import Log

def create_parser(subparsers):
    '''
    :param subparsers:
    :return:
    '''

    parser = subparsers.add_parser(
        'list',
        help='List jobs, or list workers of a job',
        usage="%(prog)s jobs/jobID",
        add_help=True
    )

    args.add_job_id(parser)

    parser.set_defaults(subcommand='list')
    return parser


# pylint: disable=unused-argument
def run(command, parser, args, unknown_args):
    '''
    :param command:
    :param parser:
    :param args:
    :param unknown_args:
    :return:
    '''

    job_id = args['job-id']
    conf_path = os.path.join(config.get_twister2_dir(), "conf/common")

    new_args = [
        "--twister2_home", config.get_twister2_dir(),
        "--config_path",conf_path,
        "--job_id", job_id,
        "--command", command,
    ]

    new_args += unknown_args
    lib_jars = config.get_twister2_libs(jars.resource_scheduler_jars())

    if Log.getEffectiveLevel() == logging.DEBUG:
        new_args.append("--verbose")

    java_defines = []
    conf_dir_common = config.get_twister2_cluster_conf_dir("common", config.get_twister2_conf_dir())
    java_defines.append("java.util.logging.config.file=" + conf_dir_common + "/logger.properties")

    # invoke the runtime manager to kill the job
    result = execute.twister2_class(
        'edu.iu.dsc.tws.rsched.job.ZKJobLister',
        lib_jars,
        extra_jars=[],
        args=new_args,
        java_defines=java_defines
    )

    err_msg = "Failed to %s %s" % (command, job_id)
    succ_msg = "Successfully %s %s" % (command, job_id)
    result.add_context(err_msg, succ_msg)

    return result
