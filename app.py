import logging

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
        arcGIS_config = self.config['locations']['arcGISGenderInclusiveRR']
        url = arcGIS_config['url']
        params = arcGIS_config['params']

        response = requests.get(url, params=params)

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


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    locationsGenerator.get_arcGIS_locations()
