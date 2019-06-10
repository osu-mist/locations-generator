import asyncio
from datetime import datetime, timedelta
import logging
import xml.etree.ElementTree as et

from icalendar import Calendar
import grequests
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

        self.today = datetime.utcnow().date()
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

    async def get_dining_locations(self):
        config = self.config['locations']['uhds']
        calendar_url = f'{config["url"]}/{config["calendar"]}'
        week_menu_url = f'{config["url"]}/{config["weeklyMenu"]}'

        response = requests.get(calendar_url)
        diners_data = {}

        if response.status_code == 200:
            calendar_ids = []
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

                    calendar_ids.append(raw_diner['calendar_id'])
                    diners_data[raw_diner['calendar_id']] = diner

            diners_hours_responses = []
            for calendar_id in calendar_ids:
                url = (
                    self.config['locations']['ical']['url']
                        .replace('calendar-id', calendar_id)
                )
                diners_hours_responses.append(grequests.get(url))

            for calendar_id, response in zip(
                calendar_ids,
                grequests.map(diners_hours_responses)
            ):
                week_events = self.get_location_hours(response)
                diners_data[calendar_id]['open_hours'] = week_events

            return diners_data

    def get_location_hours(self, response):
        week_events = {}

        for day in range(7):
            week_day = self.today + timedelta(days=day)
            week_events[week_day.strftime('%Y-%m-%d')] = []

        if response.status_code == 200:
            calendar = Calendar.from_ical(response.text)

            for event in calendar.walk():
                if event.name == 'VEVENT':
                    event_day = event.get('dtstart').dt.strftime('%Y-%m-%d')
                    if event_day in week_events:
                        event_hours = {
                            'summary': event.get('summary'),
                            'uid': event.get('uid'),
                            'start': event.get('dtstart'),
                            'end': event.get('dtend'),
                            'dtstamp': event.get('dtstamp'),
                            'sequence': event.get('sequence'),
                            'recurrence_id': event.get('recurrenceId'),
                            'last_modified': event.get('lastModified')
                        }
                        week_events[event_day].append(event_hours)

            return week_events

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

    #         for event in calendar.walk():
    #             if event.name == 'VEVENT':
    #                 print('-------------')
    #                 print(event.get('summary'))
    #                 print(event.get('uid'))
    #                 print(event.get('dtstart'))
    #                 print(event.get('dtend'))
    #                 print(event.get('dtstamp'))
    #                 print(event.get('sequence'))

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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(locationsGenerator.get_dining_locations())
