import argparse

parser = argparse.ArgumentParser(description='Timetell importer service')
parser.add_argument('--config-path', metavar='PATH', help='path to configuration file')

def main():
    args = parser.parse_args()
    config_path = args.config_path
    from . import config
    print(config.load(config_path))

if __name__ == '__main__':
    main()
