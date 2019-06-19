import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import logging
import xml.etree.ElementTree as et

from cx_Oracle import connect
import grequests
from icalendar import Calendar
from pyproj import Proj
import requests

from locations.Locations import (
    ExtensionLocation,
    ExtraLocation,
    FacilLocation,
    ParkingLocation,
    ServiceLocation
)
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
        self.config = utils.load_yaml(arguments.config)
        self.extra_data = utils.load_yaml('contrib/extra-data.yaml')
        self.facil_query = utils.load_file('contrib/get_facil_locations.sql')

    def get_gender_inclusive_restrooms(self):
        """
        Get gender inclusing restrooms data via arcGIS API
        """
        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["genderInclusiveRR"]["endpoint"]}'
        params = config['genderInclusiveRR']['params']

        response = requests.get(url, params=params)
        gender_inclusive_restrooms = {}

        if response.status_code == 200:
            for feature in response.json()['features']:
                attributes = feature['attributes']

                gender_inclusive_restrooms[attributes['BldID']] = {
                    'abbreviation': attributes.get('BldNamAbr'),
                    'count': attributes.get('CntAll'),
                    'limit': attributes.get('Limits'),
                    'all': attributes.get('LocaAll')
                }

        return gender_inclusive_restrooms

    def get_arcGIS_geometries(self):
        """
        Get locations' geometry data via arcGIS API
        """
        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["buildingGeometries"]["endpoint"]}'
        params = config['buildingGeometries']['params']
        buildings_coordinates = self.get_converted_coordinates(url, params)

        arcGIS_coordinates = {}

        for feature in buildings_coordinates['features']:
            prop = feature['properties']

            arcGIS_location = {
                'abbreviation': prop.get('BldNamAbr'),
                'latitude': prop.get('Cent_Lat'),
                'longitude': prop.get('Cent_Lon'),
                'coordinates': None,
                'coordinatesType': None
            }

            if feature['geometry']:
                geometry = feature['geometry']
                arcGIS_location['coordinates'] = geometry.get('coordinates')
                arcGIS_location['coordinatesType'] = geometry.get('type')

            arcGIS_coordinates[prop['BldID']] = arcGIS_location

        return arcGIS_coordinates

    def get_parking_locations(self):
        """
        Get parking locations via arcGIS API
        """

        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["parkingGeometries"]["endpoint"]}'
        params = config['parkingGeometries']['params']
        parkings_coordinates = self.get_converted_coordinates(url, params)

        parking_locations = []
        ignored_parkings = []

        for feature in parkings_coordinates['features']:
            # Only fetch the location if Prop_ID and ZoneGroup are valid
            if (
                utils.is_valid_field(feature['properties']['Prop_ID'])
                and utils.is_valid_field(feature['properties']['ZoneGroup'])
            ):
                parking_location = ParkingLocation(feature)
                parking_locations.append(parking_location)
            else:
                ignored_parkings.append(feature['properties']['OBJECTID'])

        if ignored_parkings:
            logging.warning((
                f'These parking lot OBJECTID\'s were ignored because they '
                f'don\'t have a valid Prop_ID or ZoneGroup: {ignored_parkings}'
            ))

        return parking_locations

    def get_facil_locations(self):
        """
        Get facility locations via Banner
        """
        config = self.config['database']
        connection = connect(config['user'], config['password'], config['url'])
        cursor = connection.cursor()

        cursor.execute(self.facil_query)

        col_names = [row[0] for row in cursor.description]
        facil_locations = {}

        for row in cursor:
            facil_location = {}
            for index, col_name in enumerate(col_names):
                facil_location[col_name] = row[index]
            facil_locations[facil_location['id']] = facil_location

        return facil_locations

    def get_campus_map_locations(self):
        """
        Get campus map locations by parsing JSON file
        """
        config = self.config['locations']['campusMap']

        response = requests.get(config['url'])
        campus_map_data = {}

        if response.status_code == 200:
            for location in response.json():
                campus_map_data[location['id']] = location

        return campus_map_data

    def get_extension_locations(self):
        """
        Get extension locations by paring XML file
        """
        config = self.config['locations']['extension']

        response = requests.get(config['url'])
        extension_data = []

        if response.status_code == 200:
            root = et.fromstring(response.content)

            for item in root:
                raw_data = {}
                for attribute in item:
                    raw_data[attribute.tag] = attribute.text
                extension_location = ExtensionLocation(raw_data)
                extension_data.append(extension_location)

        return extension_data

    async def get_dining_locations(self):
        """
        An async function to get dining locations via UHDS
        """
        config = self.config['locations']['uhds']
        calendar_url = f'{config["url"]}/{config["calendar"]}'
        week_menu_url = f'{config["url"]}/{config["weeklyMenu"]}'

        response = requests.get(calendar_url)
        diners_data = {}

        if response.status_code == 200:
            calendar_ids = []
            for raw_diner in response.json():
                diner = ServiceLocation(
                    raw_diner,
                    location_type='dining',
                    week_menu_url=week_menu_url
                )
                calendar_id = diner.get_primary_id()

                if calendar_id and calendar_id not in diners_data:
                    calendar_ids.append(calendar_id)
                    diners_data[calendar_id] = diner

            # Create a set of unsent asynchronous requests
            diners_hours_responses = []
            for calendar_id in calendar_ids:
                url = utils.get_calendar_url(calendar_id)
                diners_hours_responses.append(grequests.get(url))

            # Send requests all at once
            for calendar_id, response in zip(
                calendar_ids,
                grequests.map(diners_hours_responses)
            ):
                open_hours = self.get_location_open_hours(response)
                diners_data[calendar_id].open_hours = open_hours

            return list(diners_data.values())

    def get_extra_locations(self):
        """
        A function to get extra location data
        """
        extra_locations = []

        for raw_location in self.extra_data['locations']:
            extra_location = ExtraLocation(raw_location)
            extra_locations.append(extra_location)

        return extra_locations

    async def get_extra_calendars(self):
        """
        An async function to get extra calendars data
        """
        extra_data = defaultdict(list)
        data = {}

        calendar_ids = []
        for raw_location in self.extra_data['calendars']:
            service_location = ServiceLocation(raw_location)
            calendar_id = service_location.get_primary_id()

            if calendar_id:
                calendar_ids.append(calendar_id)
                data[calendar_id] = service_location

        # Create a set of unsent asynchronous requests
        service_locations_hours_responses = []
        for calendar_id in calendar_ids:
            url = utils.get_calendar_url(calendar_id)
            service_locations_hours_responses.append(grequests.get(url))

        # Send requests all at once
        for calendar_id, response in zip(
            calendar_ids,
            grequests.map(service_locations_hours_responses)
        ):
            open_hours = self.get_location_open_hours(response)
            data[calendar_id].open_hours = open_hours

        for _, item in data.items():
            if 'services' in item.tags:
                extra_data['services'].append(item)
            else:
                extra_data['locations'].append(item)

        return extra_data

    def get_location_open_hours(self, response):
        """
        Get location open hour by parsing iCalendar files
        """
        open_hours = {}

        # Only fetch the events within a week
        for day in range(7):
            week_day = self.today + timedelta(days=day)
            open_hours[utils.to_date(week_day)] = []

        if response.status_code == 200:
            calendar = Calendar.from_ical(response.text)

            for event in calendar.walk():
                if event.name == 'VEVENT':
                    utc_start = utils.to_utc(event.get('dtstart').dt)
                    event_day = utils.to_date(utc_start)
                    if event_day in open_hours:
                        event_hours = {
                            'summary': str(event.get('summary')),
                            'uid': str(event.get('uid')),
                            'start': utils.to_utc_string(utc_start),
                            'end': utils.to_utc_string(event.get('dtend').dt),
                            'sequence': event.get('sequence'),
                            'recurrenceId': event.get('recurrenceId'),
                            'lastModified': event.get('lastModified')
                        }
                        open_hours[event_day].append(event_hours)

            return open_hours

    def get_converted_coordinates(self, url, params):
        """
        Convert ArcGIS coordinates to latitude and longitude
        """
        def __convert_polygon(polygon):
            """
            Helper function to convert a polygon location
            """
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

    def get_library_hours(self):
        """
        Get library open hours via library API
        """
        config = self.config['locations']['library']

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.kiosks.v1'
        }
        body = {'dates': []}

        for day in range(7):
            week_day = self.today + timedelta(days=day)
            body['dates'].append(utils.to_date(week_day))

        response = requests.post(config['url'], headers=headers, json=body)

        if response.status_code == 200:
            return response.json()

    def get_combined_data(self):
        facil_locations = self.get_facil_locations()
        gender_inclusive_restrooms = self.get_gender_inclusive_restrooms()
        arcGIS_geometries = self.get_arcGIS_geometries()
        locations = []

        for location_id, raw_facil in facil_locations.items():
            raw_gir = gender_inclusive_restrooms.get(location_id)
            raw_geo = arcGIS_geometries.get(location_id)
            facil_location = FacilLocation(raw_facil, raw_gir, raw_geo)

            locations.append(facil_location)

        # Send async calls and collect results
        concurrent_calls = asyncio.gather(
            self.get_dining_locations(),
            self.get_extra_calendars()
        )
        loop = asyncio.get_event_loop()
        concurrent_res = loop.run_until_complete(concurrent_calls)
        loop.close()

        # Concatenate locations
        locations += self.get_extra_locations()  # extra locations
        locations += self.get_extension_locations()  # extension locations
        locations += self.get_parking_locations()  # parking locations
        locations += concurrent_res[0]  # dining locations
        locations += concurrent_res[1]['locations']  # extra service locations

        combined_locations = {}

        return combined_locations


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    locationsGenerator.get_combined_data()
