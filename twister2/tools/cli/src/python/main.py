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

# !/usr/bin/env python2.7
''' main.py '''
import argparse
import atexit
import os
import shutil
import sys
import time
import traceback

import twister2.tools.cli.src.python.log as log
import twister2.tools.cli.src.python.help as cli_help
import twister2.tools.cli.src.python.kill as kill
import twister2.tools.cli.src.python.config as config
import twister2.tools.cli.src.python.submit as submit
import twister2.tools.cli.src.python.result as result

Log = log.Log

HELP_EPILOG = '''Getting more help:
  twister2 help <command> Prints help and options for <command>'''


# pylint: disable=protected-access
class _HelpAction(argparse._HelpAction):
    def __call__(self, parser, namespace, values, option_string=None):
        parser.print_help()

        # retrieve subparsers from parser
        subparsers_actions = [
            action for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
            ]

        # there will probably only be one subparser_action,
        # but better save than sorry
        for subparsers_action in subparsers_actions:
            # get all subparsers and print help
            for choice, subparser in subparsers_action.choices.items():
                print "Subparser '{}'".format(choice)
                print subparser.format_help()
                return

def create_parser():
    '''
    Main parser
    :return:
    '''
    parser = argparse.ArgumentParser(
        prog='twister2',
        epilog=HELP_EPILOG,
        formatter_class=config.SubcommandHelpFormatter,
        add_help=True)

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')

    cli_help.create_parser(subparsers)
    kill.create_parser(subparsers)
    submit.create_parser(subparsers)

    return parser


def run(command, parser, command_args, unknown_args):
    '''
    Run the command
    :param command:
    :param parser:
    :param command_args:
    :param unknown_args:
    :return:
    '''
    runners = {
        'kill':kill,
        'submit':submit,
        'help':cli_help,
    }

    if command in runners:
        return runners[command].run(command, parser, command_args, unknown_args)
    else:
        err_context = 'Unknown subcommand: %s' % command
        return result.SimpleResult(result.Status.InvocationError, err_context)

def cleanup(files):
    '''
    :param files:
    :return:
    '''
    for cur_file in files:
        if os.path.isdir(cur_file):
            shutil.rmtree(cur_file)
        else:
            shutil.rmtree(os.path.dirname(cur_file))


def check_environment():
    '''
    Check whether the environment variables are set
    :return:
    '''
    if not config.check_java_home_set():
        sys.exit(1)


def main():
    '''
    Run the command
    :return:
    '''
    # verify if the environment variables are correctly set
    check_environment()

    # create the argument parser
    parser = create_parser()

    # if no argument is provided, print help and exit
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        return 0

    # insert the boolean values for some of the options
    sys.argv = config.insert_bool_values(sys.argv)

    try:
        # parse the args
        args, unknown_args = parser.parse_known_args()
        # print "args ", argparse, " unknown_args", unknown_args
    except ValueError as ex:
        Log.error("Error while parsing arguments: %s", str(ex))
        Log.debug(traceback.format_exc())
        sys.exit(1)

    command_line_args = vars(args)

    # command to be execute
    command = command_line_args['subcommand']

    # print the input parameters, if verbose is enabled
    Log.debug(command_line_args)

    start = time.time()
    results = run(command, parser, command_line_args, unknown_args)
    if command not in ('help', 'version'):
        result.render(results)
    end = time.time()

    if command not in ('help', 'version'):
        sys.stdout.flush()
        Log.info('Elapsed time: %.3fs.', (end - start))

    return 0 if result.isAllSuccessful(results) else 1

if __name__ == "__main__":
    sys.exit(main())
