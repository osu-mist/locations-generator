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


def load_extra_data():
    raw_extra_data = load_yaml('contrib/extra-data.yaml')
    extra_data = []
    for calendar in raw_extra_data['calendars']:
        service_location = {
            'concept_title': calendar.get('id'),
            'calendar_id': calendar.get('calendarId'),
            'merge': calendar.get('merge'),
            'parent': calendar.get('parent'),
            'tags': calendar.get('tags'),
            'type': calendar.get('type')
        }
        extra_data.append(service_location)

    return extra_data
