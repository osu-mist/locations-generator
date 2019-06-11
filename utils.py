import argparse
import logging
import sys

import yaml


def parse_arguments():
    """
    Handler function for parsing command-line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        dest='config_path',
        help='Path to yaml formatted config file',
        required=True)
    parser.add_argument(
        '--debug',
        dest='debug',
        help='Enable debug logging mode',
        action='store_true')

    return parser.parse_args()


def load_yaml(file):
    """
    Handler function for loading config file
    """
    with open(file, 'r') as configuration:
        try:
            return yaml.safe_load(configuration)
        except yaml.YAMLError as error:
            logging.debug(error)
            sys.exit(f'Unable to parse {file}')
