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
''' submit.py '''
import glob
import logging
import os
import tempfile
import sys

from twister2.tools.cli.src.python.log import Log
from twister2.tools.cli.src.python.result import SimpleResult, Status
import twister2.tools.cli.src.python.args as cli_args
import twister2.tools.cli.src.python.execute as execute
import twister2.tools.cli.src.python.jars as jars
import twister2.tools.cli.src.python.result as result
import twister2.tools.cli.src.python.config as config

# pylint: disable=too-many-return-statements

################################################################################
def create_parser(subparsers):
    '''
    Create a subparser for the submit command
    :param subparsers:
    :return:
    '''
    parser = subparsers.add_parser(
        'submit',
        help='Submit a job',
        usage="%(prog)s [options] cluster " + \
              "job-type job-file-name job-class-name [job-args]",
        add_help=True
    )

    cli_args.add_titles(parser)
    cli_args.add_cluster_role_env(parser)
    cli_args.add_job_type(parser)
    cli_args.add_job_file(parser)
    cli_args.add_job_class(parser)
    cli_args.add_verbose(parser)

    parser.set_defaults(subcommand='submit')
    return parser

def setup_java_system_properties(cl_args):
    java_system_props = []
    twister2_home = config.get_twister2_dir()

    if os.environ.get('TWISTER2_HOME'):
        twister2_home = os.environ.get('TWISTER2_HOME')

    # lets set the system property
    java_system_props.append("twister2_home=" + twister2_home)
    # set the cluster name property
    java_system_props.append("cluster_type=" + cl_args["cluster"])
    # set the job file
    java_system_props.append("job_file=" + cl_args['job-file-name'])
    return java_system_props

################################################################################
def submit_fatjar(cl_args, unknown_args):
    '''
    We use the packer to make a package for the jar and dump it
    to a well-known location. We then run the main method of class
    with the specified arguments. We pass arguments as an environment variable TWISTER2_OPTIONS.

    This will run the jar file with the job_class_name. The submitter
    inside will write out the job defn file to a location that
    we specify. Then we write the job defn file to a well known
    location. We then write to appropriate places in zookeeper
    and launch the scheduler jobs
    :param cl_args:
    :param unknown_args:
    :param tmp_dir:
    :return:
    '''
    # set up the system properties
    java_system_props = setup_java_system_properties(cl_args)

    # execute main of the job to create the job definition
    job_file = cl_args['job-file-name']

    main_class = cl_args['job-class-name']

    res = execute.twister2_class(
        class_name=main_class,
        lib_jars=config.get_twister2_libs(jars.job_jars()),
        extra_jars=[job_file],
        args=tuple(unknown_args),
        java_defines=java_system_props)

    result.render(res)

    if not res.is_successful():
        err_context = ("Failed to create job definition " \
                       "Please check logs for more information")
        res.add_context(err_context)
        return res

    return res

################################################################################
# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
    '''
    Submits the job to the scheduler
      * Depending on the job file name extension, we treat the file as a
        fatjar (if the ext is .jar) or a tar file (if the ext is .tar/.tar.gz).
      * We upload the job file to the packer, update zookeeper and launch
        scheduler jobs representing that job
      * You can see your job in Twister2 UI
    :param command:
    :param parser:
    :param cl_args:
    :param unknown_args:
    :return:
    '''
    # get the job type
    job_type = cl_args['job-type']

    # get the job file name
    job_file = cl_args['job-file-name']

    # check to see if the job file exists
    if not os.path.isfile(job_file):
        err_context = "Topology file '%s' does not exist" % job_file
        return SimpleResult(Status.InvocationError, err_context)

    # check if it is a valid file type
    if not job_type or job_type not in ['jar']:
        err_context = "Unknown file type '%s'. Please use jar" \
                      % job_type
        return SimpleResult(Status.InvocationError, err_context)

    # check the extension of the file name to see if it is tar/jar file.
    if job_type == "jar":
        return submit_fatjar(cl_args, unknown_args)
    else:
        return False
