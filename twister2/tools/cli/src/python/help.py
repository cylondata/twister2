from twister2.tools.cli.src.python.log import Log
from twister2.tools.cli.src.python.result import SimpleResult, Status
import twister2.tools.cli.src.python.config as config

def create_parser(subparsers):
    '''
    :param subparsers:
    :return:
    '''
    parser = subparsers.add_parser(
        'help',
        help='Prints help for commands',
        add_help=True)

    # pylint: disable=protected-access
    parser._positionals.title = "Required arguments"
    parser._optionals.title = "Optional arguments"

    parser.add_argument(
        'help-command',
        nargs='?',
        default='help',
        help='Provide help for a command')

    parser.set_defaults(subcommand='help')
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
    # get the command for detailed help
    command_help = args['help-command']

    # if no command is provided, just print main help
    if command_help == 'help':
        parser.print_help()
        return SimpleResult(Status.Ok)

    # get the subparser for the specific command
    subparser = config.get_subparser(parser, command_help)
    if subparser:
        print subparser.format_help()
        return SimpleResult(Status.Ok)
    else:
        Log.error("Unknown subcommand \'%s\'", command_help)
        return SimpleResult(Status.InvocationError)