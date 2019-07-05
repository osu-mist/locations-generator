import argparse
from datetime import timezone
import hashlib
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


def format_library_hour(string):
    """
    Helper function to format the open hours of the library
    """
    string = string.strip()

    # e.g. 7:00am -> 07:00am
    return '0' + string if len(string) <= 7 else string


def to_utc(dt):
    """
    Helper function to convert the timezone of a datetime object to UTC
    """
    return dt.replace(tzinfo=timezone.utc)


def to_date(dt):
    """
    Helper function to stringify a datetime object to date string
    """
    return dt.strftime('%Y-%m-%d')


def to_utc_string(dt):
    """
    Helper function to stringify a datetime object to UTC datetime string
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def is_valid_field(field):
    """
    Helper function to determine if a field contains at least one
    non-whitespace character
    """
    return field and field.strip()


def get_md5_hash(string):
    """
    Helper function to return the MD5 hash in hexadecimal format
    """
    return hashlib.md5(string.encode('utf-8')).hexdigest()
