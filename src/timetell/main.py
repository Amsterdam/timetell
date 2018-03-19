import argparse

from aiohttp import web

from . import application

parser = argparse.ArgumentParser(description='Timetell importer service')
parser.add_argument('--config-path', metavar='PATH', help='path to configuration file')


def main():
    args = parser.parse_args()
    config_path = args.config_path
    app = application.Application(config_path=config_path)
    web.run_app(app, host=app.bindaddr, port=app.bindport)


if __name__ == '__main__':
    main()
