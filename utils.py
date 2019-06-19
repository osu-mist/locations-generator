import argparse
from datetime import timezone
import hashlib
import json
import logging
import sys

from pyproj import Proj
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


def to_utc(datetime):
    """
    Helper function to convert the timezone of a datetime object to UTC
    """
    return datetime.replace(tzinfo=timezone.utc)


def to_date(datetime):
    """
    Helper function to stringify a datetime object to date string
    """
    return datetime.strftime('%Y-%m-%d')


def to_utc_string(datetime):
    """
    Helper function to stringify a datetime object to UTC datetime string
    """
    return datetime.strftime('%Y-%m-%dT%H:%M:%SZ')


def is_valid_field(field):
    """
    Helper function to determine if a field is valid or not
    """
    return field and field.strip()


def get_md5_hash(string):
    """
    Helper function to return the MD5 hash in hexadecimal format
    """
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def create_geo_location(latitude, longitude):
    """
    Helper function to generate geo location object
    """
    if (latitude and longitude):
        return {
            'lat': float(latitude),
            'lon': float(longitude)
        }
