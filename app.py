import logging
import xml.etree.ElementTree as et

import arrow
from icalendar import Calendar
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

        self.today = arrow.utcnow()
        self.config = utils.load_yaml('configuration.yaml')
        self.extra_data = utils.load_extra_data('contrib/extra-data.yaml')

    def get_arcGIS_locations(self):
        config = self.config['locations']['arcGISGenderInclusiveRR']

        response = requests.get(config['url'], params=config['params'])
        arcGIS_data = {}

        if response.status_code == 200:
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
        campus_map_data = {}

        if response.status_code == 200:
            for location in response.json():
                campus_map_data[location['id']] = location

        return campus_map_data

    def get_extention_locations(self):
        config = self.config['locations']['extension']

        response = requests.get(config['url'])
        extention_data = []

        if response.status_code == 200:
            root = et.fromstring(response.content)

            for item in root:
                item_data = {}
                for attribute in item:
                    item_data[attribute.tag] = attribute.text
                extention_data.append(item_data)

        return extention_data

    def get_dining_locations(self):
        config = self.config['locations']['uhds']
        calendar_url = f'{config["url"]}/{config["calendar"]}'
        week_menu_url = f'{config["url"]}/{config["weeklyMenu"]}'

        response = requests.get(calendar_url)
        diners_data = {}

        if response.status_code == 200:
            for raw_diner in response.json():
                if raw_diner['calendar_id'] not in diners_data:
                    diner = {
                        'concept_title': raw_diner['concept_title'],
                        'zone': raw_diner['zone'],
                        'calendar_id': raw_diner['calendar_id'],
                        'start': raw_diner['start'],
                        'end': raw_diner['end'],
                        'type': 'dining'
                    }

                    if raw_diner['concept_coord']:
                        coordinates = raw_diner['concept_coord'].split(',')
                        diner['latitude'] = coordinates[0].strip()
                        diner['longitude'] = coordinates[1].strip()

                    if raw_diner['loc_id']:
                        diner['weekly_menu'] = (
                            f'{week_menu_url}?loc={raw_diner["loc_id"]}'
                        )

                    diners_data[raw_diner['calendar_id']] = diner

            return diners_data

    # def get_extra_service_locations(self):
    #     extra_locations = []
    #     extra_services = []
    #     week_events = {}
    #     for day in range(7):
    #         week_day = self.today.replace(days=+day).format('YYYY-MM-DD')
    #         week_events[week_day] = []

    #     for extra_location in self.extra_data:
    #         url = (
    #             self.config['locations']['ical']['url']
    #                 .replace('calendar-id', extra_location['calendar_id'])
    #         )

    #         response = requests.get(url)
    #         calendar = Calendar.from_ical(response.text)

    #         for component in calendar.walk():
    #             if component.name == 'VEVENT':
    #                 print('-------------')
    #                 print(component.get('summary'))
    #                 print(component.get('uid'))
    #                 print(component.get('dtstart'))
    #                 print(component.get('dtend'))
    #                 print(component.get('dtstamp'))
    #                 print(component.get('sequence'))

            # for raw_event in calendar.events:
            #     print(raw_event)
            #     print('-------')
            #     event_day = raw_event.begin.format('YYYY-MM-DD')
                # if event_day in week_events:
                #     event = {
                #         'start': raw_event.begin,
                #         'end': raw_event.end,

                #     }

                #     week_events[event_day].append(event)

            # if 'services' in extra_location['tags']:
            #     extra_services.append(extra_location)
            # else:
            #     extra_locations.append(extra_location)

        # return extra_locations, extra_services


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    # locationsGenerator.get_arcGIS_locations()
    # locationsGenerator.get_campus_map_locations()
    # locationsGenerator.get_extention_locations()
    locationsGenerator.get_dining_locations()
