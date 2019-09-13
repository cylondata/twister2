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
''' execute.py '''
import contextlib
import os
import subprocess
import tarfile
import tempfile
import traceback
from zipfile import ZipFile
from os import listdir
from os.path import isfile, join

from twister2.tools.cli.src.python.log import Log
from twister2.tools.cli.src.python.result import SimpleResult, ProcessResult, Status
import twister2.tools.cli.src.python.opts as opts
import twister2.tools.cli.src.python.jars as jars
import twister2.tools.cli.src.python.config as config


################################################################################
def twister2_class(class_name, lib_jars, extra_jars=None, args=None, java_defines=None,
                   client_props=None):
    '''
    Execute a twister2 class given the args and the jars needed for class path
    :param class_name:
    :param lib_jars:
    :param extra_jars:
    :param args:
    :param java_defines:
    :return:
    '''
    # default optional params to empty list if not provided
    if extra_jars is None:
        extra_jars = []
    if args is None:
        args = []
    if java_defines is None:
        java_defines = []

    # Format all java -D options that need to be passed while running
    # the class locally.
    java_opts = ['-D' + opt for opt in java_defines]

    if client_props:
        if 'twister2.client.debug' in client_props:
            java_opts.append(client_props['twister2.client.debug'])

    # Construct the command line for the sub process to run
    # Because of the way Python execute works,
    # the java opts must be passed as part of the list
    all_args = [config.get_java_path(), "-client", "-Xmx1g"] + \
               java_opts + \
               ["-cp", config.get_classpath(extra_jars + lib_jars)]

    all_args += [class_name] + list(args)

    # set twister2_config environment variable
    twister2_env = os.environ.copy()
    twister2_env['TWISTER2_OPTIONS'] = opts.get_twister2_config()

    # print the verbose message
    Log.debug("Invoking class using command: ``%s''", ' '.join(all_args))
    Log.debug("Twister2 options: {%s}", str(twister2_env["TWISTER2_OPTIONS"]))

    # invoke the command with subprocess and print error message, if any
    proc = subprocess.Popen(all_args, env=twister2_env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, bufsize=1,
                            universal_newlines=True,)
    # stdout message has the information Java program sends back
    # stderr message has extra information, such as debugging message
    return ProcessResult(proc)


def twister2_tar(class_name, topology_tar, arguments, tmpdir_root, java_defines, client_props=None):
    '''
    :param class_name:
    :param topology_tar:
    :param arguments:
    :param tmpdir_root:
    :param java_defines:
    :return:
    '''
    # Extract tar to a tmp folder.
    tmpdir = tempfile.mkdtemp(dir=tmpdir_root, prefix='tmp')
    topology_jar = os.path.basename(topology_tar).replace(".zip", "")

    with ZipFile(topology_tar, 'r') as zipObj:
        print(topology_tar, tmpdir, topology_jar)
        # Extract all the contents of zip file in different directory
        zipObj.extractall(tmpdir)


    if java_defines is None:
        java_defines = []

    # Format all java -D options that need to be passed while running
    # the class locally.
    java_opts = ['-D' + opt for opt in java_defines]

    extra_jars = [
        os.path.join(tmpdir, topology_jar, "*.jar"),
        os.path.join(tmpdir, topology_jar, "libs/*.jar")
    ]

    for root, dirs, files in os.walk(os.path.join(tmpdir, topology_jar)):
        for file in files:
            if file.endswith('.jar'):
                extra_jars.append(os.path.abspath(os.path.join(root, file)))

    print(extra_jars)

    lib_jars = config.get_twister2_libs(jars.job_jars())

    print(lib_jars)

    if client_props:
        if 'twister2.client.debug' in client_props:
            java_opts.append(client_props['twister2.client.debug'])

    # Now execute the class
    return twister2_class(class_name, lib_jars, extra_jars, arguments, java_defines, client_props)


def twister2_jar(jar_path, args=None, jvm_args=[]):
    if args is None:
        args = []

    all_args = [config.get_java_path()]

    all_args += list(jvm_args)

    all_args += ["-jar", jar_path]

    all_args += list(args)

    print(all_args)

    proc = subprocess.Popen(all_args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, bufsize=1)

    return ProcessResult(proc)
