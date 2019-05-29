import logging
import xml.etree.ElementTree as ET

import requests

import utils


class LocationsGenerator:
    def __init__(self):
        arguments = utils.parse_arguments()

        # Setup logging level
        if arguments.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        self.config = utils.load_config()

    def get_arcGIS_locations(self):
        config = self.config['locations']['arcGISGenderInclusiveRR']

        response = requests.get(config['url'], params=config['params'])

        if response.status_code == 200:
            arcGIS_data = {}
            for feature in response.json()['features']:
                attributes = feature['attributes']
                arcGIS_data[attributes['BldID']] = {
                    'id': attributes['BldID'],
                    'name': attributes['BldNam'],
                    'abbreviation': attributes['BldNamAbr'],
                    'restroom_count': attributes['CntAll'],
                    'restroom_limit': attributes['Limits'],
                    'restroom_locations': attributes['LocaAll']
                }

        return arcGIS_data

    def get_campus_map_locations(self):
        config = self.config['locations']['campusMap']

        response = requests.get(config['url'])

        if response.status_code == 200:
            campus_map_data = {}
            for location in response.json():
                campus_map_data[location['id']] = location

        return campus_map_data

    def get_extention_locations(self):
        config = self.config['locations']['extension']

        response = requests.get(config['url'])

        if response.status_code == 200:
            extention_data = []
            root = ET.fromstring(response.content)

            for item in root:
                item_data = {}
                for attribute in item:
                    item_data[attribute.tag] = attribute.text
                extention_data.append(item_data)

        return extention_data


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    locationsGenerator.get_arcGIS_locations()
    locationsGenerator.get_campus_map_locations()
    locationsGenerator.get_extention_locations()
