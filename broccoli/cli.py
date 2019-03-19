import sys
import argparse
import importlib
from broccoli.types import App, Config
from broccoli.utils import default


def main():
    if '' not in sys.path:
        sys.path.insert(0, '')
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument('-A', '--app', default=None)
    subparsers = parser.add_subparsers(title='commands')

    for cmd in commands:
        cmdparser = subparsers.add_parser(cmd, add_help=False)
        cmdparser.set_defaults(_cmd=(cmd, commands[cmd]))

    args, rest = parser.parse_known_args()
    if not args.app or not hasattr(args, '_cmd'):
        parser.print_help()
        sys.exit(0)

    name, command = args._cmd
    cmdparser = subparsers.add_parser(name)
    bootstrap = command(cmdparser)
    args = vars(parser.parse_args())
    app = args.pop('app')

    def apply_config(config: Config):
        for k, v in args.items():
            if isinstance(v, default):
                if k not in config:
                    config[k] = v.value
            else:
                config[k] = v

    app_module = importlib.import_module(app)
    for inst in dir(app_module):
        app = getattr(app_module, inst)
        if isinstance(app, App):
            break
    else:
        msg = 'Application not found.'
        raise TypeError(msg)

    app.inject([apply_config] + bootstrap, cache=False)


def node(parser):
    from broccoli import worker
    worker.add_console_arguments(parser)
    return worker.bootstrap()


def beat(parser):
    from broccoli import beat
    beat.add_console_arguments(parser)
    return beat.bootstrap()


commands = {
    'node': node,
    'beat': beat
}


if __name__ == "__main__":
    main()
