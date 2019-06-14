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
        def __is_valid_field(field):
            """
            Helper function to check if the field is valid
            """
            return field and field.strip()

        config = self.config['locations']['arcGIS']
        url = f'{config["url"]}{config["parkingGeometries"]["endpoint"]}'
        params = config['parkingGeometries']['params']
        parkings_coordinates = self.get_converted_coordinates(url, params)

        parking_locations = []
        ignored_parkings = []

        for feature in parkings_coordinates['features']:
            prop = feature['properties']
            prop_id = prop['Prop_ID']
            parking_zone_group = prop['ZoneGroup']

            # Only fetch the location if Prop_ID and ZoneGroup are valid
            if (
                __is_valid_field(prop_id)
                and __is_valid_field(parking_zone_group)
            ):
                location = {
                    'id': f'{prop_id}{parking_zone_group}',
                    'description': prop.get('AiM_Desc'),
                    'propId': prop_id,
                    'parkingZoneGroup': parking_zone_group,
                    'latitude': prop.get('Cent_Lat'),
                    'longitude': prop.get('Cent_Lon'),
                    'adaParkingSpaceCount': prop.get('ADA_Spc'),
                    'evParkingSpaceCount': prop.get('EV_Spc'),
                    'motorcycleParkingSpaceCount': prop.get('MCycle_Spc'),
                    'coordinates': None,
                    'coordinatesType': None
                }

                if feature['geometry']:
                    geometry = feature['geometry']
                    location['coordinates'] = geometry.get('coordinates')
                    location['coordinatesType'] = geometry.get('type')

                parking_locations.append(location)
            else:
                ignored_parkings.append(prop['OBJECTID'])

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

    def get_extention_locations(self):
        """
        Get extention locations by paring XML file
        """
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
                calendar_id = raw_diner.get('calendar_id')
                if raw_diner['calendar_id'] not in diners_data:
                    diner = {
                        'conceptTitle': raw_diner.get('concept_title'),
                        'zone': raw_diner.get('zone'),
                        'calendarId': calendar_id,
                        'start': raw_diner.get('start'),
                        'end': raw_diner.get('end'),
                        'type': 'dining',
                        'latitude': None,
                        'longitude': None,
                        'weeklyMenu': None
                    }

                    if raw_diner.get('concept_coord'):
                        coordinates = raw_diner['concept_coord'].split(',')
                        diner['latitude'] = coordinates[0].strip()
                        diner['longitude'] = coordinates[1].strip()

                    if raw_diner.get('loc_id'):
                        diner['weeklyMenu'] = (
                            f'{week_menu_url}?loc={raw_diner["loc_id"]}'
                        )

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
                diners_data[calendar_id]['openHours'] = open_hours

            return list(diners_data.values())

    def get_extra_locations(self):
        """
        An async function to get extra location data
        """
        extra_locations = []

        for raw_location in self.extra_data['locations']:
            location = {
                'name': raw_location.get('name'),
                'buildingId': raw_location.get('bldgID'),
                'latitude': raw_location.get('latitude'),
                'longitude': raw_location.get('longitude'),
                'campus': raw_location.get('campus'),
                'type': raw_location.get('type'),
                'tags': raw_location.get('tags')
            }
            extra_locations.append(location)

        return extra_locations

    async def get_extra_calendars(self):
        """
        An async function to get extra calendars data
        """
        extra_locations = []
        extra_services = []
        extra_data = {}

        calendar_ids = []
        for calendar in self.extra_data['calendars']:
            calendar_id = calendar.get('calendarId')
            if calendar_id:
                service_location = {
                    'conceptTitle': calendar.get('id'),
                    'calendarId': calendar_id,
                    'merge': calendar.get('merge'),
                    'parent': calendar.get('parent'),
                    'tags': calendar.get('tags'),
                    'type': calendar.get('type')
                }

                calendar_ids.append(calendar_id)
                extra_data[calendar_id] = service_location

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
            extra_data[calendar_id]['openHours'] = open_hours

        for _, data in extra_data.items():
            if 'services' in data['tags']:
                extra_services.append(data)
            else:
                extra_locations.append(data)

        return extra_locations, extra_services

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
        """
        Merge Banner locations with the data of gender inclusive restrooms and
        geometries from ArcGIS
        """
        facil_locations = self.get_facil_locations()
        gender_inclusive_restrooms = self.get_gender_inclusive_restrooms()
        arcGIS_geometries = self.get_arcGIS_geometries()
        locations = []

        for location_id, raw_location in facil_locations.items():
            location = {}
            address1 = raw_location['address1']
            address2 = raw_location['address2']

            # Prettify campus string
            campus_dict = {
                'cascadescampus': 'Cascades',
                'osucampus': 'Corvallis',
                'hmsc': 'HMSC'
            }
            campus = None
            raw_campus = raw_location['campus'].lower()
            if raw_campus in campus_dict:
                campus = campus_dict[raw_campus]
            elif raw_campus:
                campus = 'Other'

            # The definition of merged location
            location = {
                'buildingId': raw_location['id'],
                'bannerAbbreviation': raw_location['abbreviation'],
                'name': raw_location['name'],
                'campus': campus,
                'address': f'{address1}\n{address2}' if address2 else address1,
                'city': raw_location['city'],
                'state': raw_location['state'],
                'zip': raw_location['zip'],
                'arcGISAbbreviation': None,
                'giRestroomCount': 0,
                'giRestroomLimit': None,
                'giRestroomLocations': None,
                'latitude': None,
                'longitude': None,
                'coordinates': None,
                'coordinatesType': None
            }

            # Merge gender inclusive restrooms
            if location_id in gender_inclusive_restrooms:
                gi_restroom = gender_inclusive_restrooms[location_id]
                location['arcGISAbbreviation'] = gi_restroom['abbreviation']
                location['giRestroomCount'] = gi_restroom['count']
                location['giRestroomLimit'] = gi_restroom['limit']
                location['giRestroomLocations'] = gi_restroom['all'].strip()

            # Merge ArcGIS geometries
            if location_id in arcGIS_geometries:
                geometry = arcGIS_geometries[location_id]
                location['arcGISAbbreviation'] = geometry['abbreviation']
                location['latitude'] = geometry['latitude']
                location['longitude'] = geometry['longitude']
                location['coordinates'] = geometry['coordinates']
                location['coordinatesType'] = geometry['coordinatesType']

            locations.append(location)

        locations += self.get_extra_locations()

        loop = asyncio.get_event_loop()
        locations += loop.run_until_complete(self.get_dining_locations())

        return locations


if __name__ == '__main__':
    locationsGenerator = LocationsGenerator()
    # locationsGenerator.get_extra_locations()
    # locationsGenerator.get_campus_map_locations()
    locationsGenerator.get_extention_locations()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(locationsGenerator.get_dining_locations())
    # locationsGenerator.get_library_hours()
    # locationsGenerator.get_combined_data()
