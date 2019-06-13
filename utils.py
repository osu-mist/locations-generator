import argparse
import json
import logging
import sys

import yaml


def parse_arguments():
    """
    Helper function for parsing command-line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        dest='config',
        help='Path to yaml formatted config file',
        required=True)
    parser.add_argument(
        '--debug',
        dest='debug',
        help='Enable debug logging mode',
        action='store_true')

    return parser.parse_args()


def load_yaml(file_name):
    """
    Helper function for loading YAML file
    """
    with open(file_name, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as error:
            logging.debug(error)
            sys.exit(f'Unable to parse {file_name}')


def load_json(file_name):
    """
    Helper function for loading JSON file
    """
    with open(file_name, 'r') as file:
        try:
            return json.load(file)
        except json.decoder.JSONDecodeError as error:
            logging.debug(error)
            sys.exit(f'Unable to parse {file_name}')


def load_file(file_name):
    """
    Helper function for loading file
    """
    with open(file_name, 'r') as file:
        try:
            return file.read()
        except FileNotFoundError as error:
            logging.debug(error)
            sys.exit(f'File {file_name} not found')


def get_calendar_url(calendar_id):
    """
    Helper function for generating calendar URL
    """
    ical_url = load_yaml('configuration.yaml')['locations']['ical']['url']
    return ical_url.replace('calendar-id', calendar_id)
