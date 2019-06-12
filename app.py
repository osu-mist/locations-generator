import asyncio
from datetime import datetime, timedelta
import logging
import xml.etree.ElementTree as et

from cx_Oracle import connect
import grequests
from icalendar import Calendar
from pyproj import Proj
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
        self.extra_data = utils.load_yaml('contrib/extra-data.yaml')
        self.facil_query = utils.load_file('contrib/get_facil_locations.sql')

    def get_arcGIS_locations(self):
        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["genderInclusiveRR"]["endpoint"]}'
        params = config['genderInclusiveRR']['params']

        response = requests.get(url, params=params)
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

    def get_arcGIS_coordinates(self):
        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["buildingGeometries"]["endpoint"]}'
        params = config['buildingGeometries']['params']
        buildings_coordinates = self.get_converted_coordinates(url, params)

        arcGIS_coordinates = {}

        for feature in buildings_coordinates['features']:
            properties = feature['properties']
            arcGIS_location = {
                'id': properties['BldID'],
                'name': properties['BldNam'],
                'abbreviation': properties['BldNamAbr'],
                'latitude': properties['Cent_Lat'],
                'longitude': properties['Cent_Lon']
            }

            if feature['geometry']:
                geometry = feature['geometry']
                arcGIS_location['coordinates'] = geometry['coordinates']
                arcGIS_location['coordinates_type'] = geometry['type']

            arcGIS_coordinates[properties['BldID']] = arcGIS_location

        return arcGIS_coordinates

    def get_parking_locations(self):
        def __is_valid_field(field):
            return field and field.strip()

        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["parkingGeometries"]["endpoint"]}'
        params = config['parkingGeometries']['params']
        parkings_coordinates = self.get_converted_coordinates(url, params)

        parking_locations = []
        ignored_parkings = []

        for feature in parkings_coordinates['features']:
            properties = feature['properties']
            prop_id = properties['Prop_ID']
            parking_zone_group = properties['ZoneGroup']

            if (
                __is_valid_field(prop_id)
                and __is_valid_field(parking_zone_group)
            ):
                parking_location = {
                    'id': f'{prop_id}{parking_zone_group}',
                    'description': properties['AiM_Desc'],
                    'prop_id': prop_id,
                    'parking_zone_group': parking_zone_group,
                    'latitude': properties['Cent_Lat'],
                    'longitude': properties['Cent_Lon'],
                    'ada_parking_space_count': properties['ADA_Spc'],
                    'ev_parking_spaceCount': properties['EV_Spc'],
                    'motorcycle_parking_space_count': properties['MCycle_Spc']
                }

                if feature['geometry']:
                    geometry = feature['geometry']
                    parking_location['coordinates'] = geometry['coordinates']
                    parking_location['coordinates_type'] = geometry['type']

                parking_locations.append(parking_location)
            else:
                ignored_parkings.append(properties['OBJECTID'])

        return parking_locations

    def get_facil_locations(self):
        config = self.config['database']
        connection = connect(config['user'], config['password'], config['url'])
        cursor = connection.cursor()

        cursor.execute(self.facil_query)

        col_names = [row[0] for row in cursor.description]
        facil_locations = []

        for row in cursor:
            for index, col_name in enumerate(col_names):
                facil_location = {
                    col_name: row[index]
                }
            facil_locations.append(facil_location)

        return facil_locations

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
                calendar_id = raw_diner.get('calendar_id')
                if raw_diner['calendar_id'] not in diners_data:
                    diner = {
                        'concept_title': raw_diner.get('concept_title'),
                        'zone': raw_diner.get('zone'),
                        'calendar_id': calendar_id,
                        'start': raw_diner.get('start'),
                        'end': raw_diner.get('end'),
                        'type': 'dining'
                    }

                    if raw_diner.get('concept_coord'):
                        coordinates = raw_diner.get('concept_coord').split(',')
                        diner['latitude'] = coordinates[0].strip()
                        diner['longitude'] = coordinates[1].strip()

                    if raw_diner['loc_id']:
                        diner['weekly_menu'] = (
                            f'{week_menu_url}?loc={raw_diner["loc_id"]}'
                        )

                    calendar_ids.append(calendar_id)
                    diners_data[calendar_id] = diner

            diners_hours_responses = []
            for calendar_id in calendar_ids:
                url = utils.get_calendar_url(calendar_id)
                diners_hours_responses.append(grequests.get(url))

            for calendar_id, response in zip(
                calendar_ids,
                grequests.map(diners_hours_responses)
            ):
                open_hours = self.get_location_open_hours(response)
                diners_data[calendar_id]['open_hours'] = open_hours

            return diners_data

    async def get_extra_data(self):
        extra_locations = []
        extra_services = []
        extra_data = {}

        calendar_ids = []
        for calendar in self.extra_data['calendars']:
            calendar_id = calendar.get('calendarId')
            if calendar_id:
                service_location = {
                    'concept_title': calendar.get('id'),
                    'calendar_id': calendar_id,
                    'merge': calendar.get('merge'),
                    'parent': calendar.get('parent'),
                    'tags': calendar.get('tags'),
                    'type': calendar.get('type')
                }

                calendar_ids.append(calendar_id)
                extra_data[calendar_id] = service_location

        service_locations_hours_responses = []
        for calendar_id in calendar_ids:
            url = utils.get_calendar_url(calendar_id)
            service_locations_hours_responses.append(grequests.get(url))

        for calendar_id, response in zip(
            calendar_ids,
            grequests.map(service_locations_hours_responses)
        ):
            open_hours = self.get_location_open_hours(response)
            extra_data[calendar_id]['open_hours'] = open_hours

        for _, data in extra_data.items():
            if 'services' in data['tags']:
                extra_services.append(data)
            else:
                extra_locations.append(data)

        return extra_locations, extra_services

    def get_location_open_hours(self, response):
        open_hours = {}

        for day in range(7):
            week_day = self.today + timedelta(days=day)
            open_hours[week_day.strftime('%Y-%m-%d')] = []

        if response.status_code == 200:
            calendar = Calendar.from_ical(response.text)

            for event in calendar.walk():
                if event.name == 'VEVENT':
                    event_day = event.get('dtstart').dt.strftime('%Y-%m-%d')
                    if event_day in open_hours:
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
                        open_hours[event_day].append(event_hours)

            return open_hours

    def get_converted_coordinates(self, url, params):
        def __convert_polygon(polygon):
            coordinates = []
            for coordinate in polygon:
                pairs = []
                for pair in coordinate:
                    lon_lat = list(proj(pair[0], pair[1], inverse=True))
                    pair = lon_lat + pair[2:] if len(pair) >= 2 else lon_lat
                    pairs.append(pair)
                coordinates.append(pairs)
            return coordinates

        response = requests.get(url, params=params)

        if response.status_code == 200:
            response_json = response.json()
            proj = Proj(('+proj=lcc '
                         '+lat_0=43.66666666666666 '
                         '+lat_1=46 '
                         '+lat_2=44.33333333333334 '
                         '+lon_0=-120.5 '
                         '+x_0=2500000.0001424 '
                         '+y_0=0 +ellps=GRS80 '
                         '+towgs84=0,0,0,0,0,0,0 '
                         '+units=ft +no_defs'))

            for feature in response_json['features']:
                geometry = feature['geometry']
                geometry_type = geometry['type']

                coordinates = []
                if geometry_type == 'Polygon':
                    coordinates = __convert_polygon(geometry['coordinates'])
                elif geometry_type == 'MultiPolygon':
                    for polygon in geometry['coordinates']:
                        coordinates.append(__convert_polygon(polygon))
                else:
                    logging.warning((
                        f'Ignoring unknown geometry type: {geometry_type}. '
                        f'(id: {feature["id"]})'
                    ))

                feature['geometry']['coordinates'] = coordinates

        return response_json


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    locationsGenerator.get_arcGIS_locations()
    # locationsGenerator.get_converted_coordinates()
    locationsGenerator.get_arcGIS_coordinates()
    locationsGenerator.get_parking_locations()
    # locationsGenerator.get_facil_locations()
    # locationsGenerator.get_campus_map_locations()
    # locationsGenerator.get_extention_locations()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(locationsGenerator.get_dining_locations())
    # loop.run_until_complete(locationsGenerator.get_extra_data())
