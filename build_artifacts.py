import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import os
import xml.etree.ElementTree as et

from cx_Oracle import connect
import grequests
from icalendar import Calendar
from pyproj import Proj
import requests
from tabulate import tabulate

from locations.Locations import (
    ExtensionLocation,
    ExtraLocation,
    FacilLocation,
    FieldLocation,
    ParkingLocation,
    PlaceLocation,
    ServiceLocation
)
import utils


class LocationsGenerator:
    def __init__(self, arguments):
        self.today = datetime.utcnow().date()
        self.config = utils.load_yaml(arguments.config)
        self.extra_data = utils.load_yaml('contrib/extra-data.yaml')
        self.facil_query = utils.load_file('contrib/get_facil_locations.sql')
        # NAD_1983_HARN_StatePlane_Oregon_North_FIPS_3601_Feet_Intl WKID: 2913
        self.proj_2913 = Proj(('+proj=lcc '
                               '+lat_0=43.66666666666666 '
                               '+lat_1=46 '
                               '+lat_2=44.33333333333334 '
                               '+lon_0=-120.5 '
                               '+x_0=2500000.0001424 '
                               '+y_0=0 '
                               '+ellps=GRS80 '
                               '+towgs84=0,0,0,0,0,0,0 '
                               '+units=ft '
                               '+no_defs'))
        # WGS_1984_Web_Mercator_Auxiliary_Sphere WKID: 3857
        self.proj_3857 = Proj(('+proj=merc '
                               '+a=6378137 '
                               '+b=6378137 '
                               '+lat_ts=0.0 '
                               '+lon_0=0.0 '
                               '+x_0=0.0 '
                               '+y_0=0 '
                               '+k=1.0 '
                               '+units=ft '
                               '+nadgrids=@null '
                               '+wktext '
                               '+no_defs'))

    def get_gender_inclusive_restrooms(self):
        """Get gender inclusive restrooms data via arcGIS API

        :returns: Gender inclusive restrooms
        :rtype: dict
        """
        config = self.config['locations']['arcGIS']
        url = f"{config['url']}{config['genderInclusiveRR']['endpoint']}"
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

    def get_fields(self):
        """Get fields data (e.g. grass fields, quads, etc.) via arcGIS API

        :returns: Field locations
        :rtype: dict
        """
        config = self.config['locations']['arcGIS']
        url = f"{config['url']}{config['fields']['endpoint']}"
        params = config['fields']['params']
        field_coordinates = self.get_converted_coordinates(
            url, params, self.proj_3857
        )

        field_locations = []
        ignored_fields = []

        for feature in field_coordinates['features']:
            attrs = feature['attributes']
            # Only fetch the location has a valid Prop_ID and Expose is 'Y'
            if (
                utils.is_valid_field(attrs['Prop_ID'])
                and attrs['Expose'] == 'Y'
            ):
                field_location = FieldLocation(feature)
                field_locations.append(field_location)
            else:
                ignored_fields.append(attrs['OBJECTID'])

        if ignored_fields:
            logger.warning((
                "These fields OBJECTID's were ignored because they don't have"
                f"a valid Prop_ID or shouldn't be exposed: {ignored_fields}\n"
            ))

        return field_locations

    def get_places(self):
        """Get places data (e.g. Welcome Center, Challenge Course, etc.) via arcGIS API

        :returns: Place locations
        :rtype: dict
        """
        config = self.config['locations']['arcGIS']
        url = f"{config['url']}{config['places']['endpoint']}"
        params = config['fields']['params']
        response = requests.get(url, params=params)

        place_locations = []
        ignored_places = []

        if response.status_code == 200:
            for feature in response.json()['features']:
                attrs = feature['attributes']
                # Only fetch the location if Prop_ID and uID are valid
                if (
                    utils.is_valid_field(attrs['Prop_ID'])
                    and utils.is_valid_field(attrs['uID'])
                ):
                    place_location = PlaceLocation(feature)
                    place_locations.append(place_location)
                else:
                    place_locations.append(attrs['OBJECTID'])

            if ignored_places:
                logger.warning((
                    "These places OBJECTID's were ignored because they don't "
                    "have a valid Prop_ID or shouldn't be exposed: "
                    f"{ignored_places}\n"
                ))

        return place_locations

    def get_arcgis_geometries(self):
        """Get locations' geometry data via arcGIS API

        :returns: Locations arcGIS coordinates
        :rtype: dict
        """
        config = self.config['locations']['arcGIS']
        url = f"{config['url']}{config['buildingGeometries']['endpoint']}"
        params = config['buildingGeometries']['params']
        buildings_coordinates = self.get_converted_coordinates(
            url, params, self.proj_2913
        )

        arcgis_coordinates = {}

        for feature in buildings_coordinates['features']:
            prop = feature['properties']

            arcgis_location = {
                'abbreviation': prop.get('BldNamAbr'),
                'latitude': prop.get('Cent_Lat'),
                'longitude': prop.get('Cent_Lon'),
                'coordinates': None,
                'coordinatesType': None
            }

            if feature['geometry']:
                geometry = feature['geometry']
                arcgis_location['coordinates'] = geometry.get('coordinates')
                arcgis_location['coordinatesType'] = geometry.get('type')

            arcgis_coordinates[prop['BldID']] = arcgis_location

        return arcgis_coordinates

    def get_parking_locations(self):
        """Get parking locations via arcGIS API

        :returns: Parking locations
        :rtype: list
        """

        config = self.config['locations']['arcGIS']
        url = f"{config['url']}{config['parkingGeometries']['endpoint']}"
        params = config['parkingGeometries']['params']
        parkings_coordinates = self.get_converted_coordinates(
            url, params, self.proj_2913
        )

        parking_locations = []
        ignored_parkings = []

        for feature in parkings_coordinates['features']:
            props = feature['properties']
            # Only fetch the location if Prop_ID and ZoneGroup are valid
            if (
                utils.is_valid_field(props['Prop_ID'])
                and utils.is_valid_field(props['ZoneGroup'])
            ):
                parking_location = ParkingLocation(feature)
                parking_locations.append(parking_location)
            else:
                ignored_parkings.append(props['OBJECTID'])

        if ignored_parkings:
            logger.warning((
                "These parking lot OBJECTID's were ignored because they don't "
                f"have a valid Prop_ID or ZoneGroup: {ignored_parkings}\n"
            ))

        return parking_locations

    def get_facil_locations(self):
        """Get facility locations via Banner

        :returns: Facil locations
        :rtype: dict
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

    def get_campus_map_data(self):
        """Get campus map data by parsing JSON file

        :returns: Campus map data
        :rtype: dict
        """
        config = self.config['locations']['campusMap']

        response = requests.get(config['url'])
        campus_map_data = {}

        if response.status_code == 200:
            for location in response.json():
                campus_map_data[location['id']] = location

        return campus_map_data

    def get_extension_locations(self):
        """Get extension locations by parsing XML file

        :returns: Extension locations
        :rtype: list
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
        """An async function to get dining locations via UHDS

        :returns: Dining locations
        :rtype: dict
        """
        config = self.config['locations']['uhds']
        calendar_url = f"{config['url']}/{config['calendar']}"
        week_menu_url = f"{config['url']}/{config['weeklyMenu']}"

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
        """A function to get extra location data

        :returns: Extra locations
        :rtype: list
        """
        extra_locations = []

        for raw_location in self.extra_data['locations']:
            extra_location = ExtraLocation(raw_location)
            extra_locations.append(extra_location)

        return extra_locations

    async def get_extra_calendars(self):
        """An async function to get extra calendars data

        :returns: Extra calendars data
        :rtype: dict
        """
        extra_data = defaultdict(list)
        data = {}

        calendar_ids = []
        for raw_location in self.extra_data['calendars']:
            if 'services' in raw_location['tags']:
                service_location = ServiceLocation(
                    raw_location,
                    location_type='services'
                )
            else:
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

        for item in data.values():
            if item.type == 'services':
                extra_data['services'].append(item)
            else:
                extra_data['locations'].append(item)

        return extra_data

    def get_location_open_hours(self, response):
        """Get location open hour by parsing iCalendar files

        :returns: Locations open hours
        :rtype: dict
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

    def get_converted_coordinates(self, url, params, proj):
        """Convert ArcGIS coordinates to latitude and longitude

        :returns: Convert ArcGIS coordinates
        :rtype: dict
        """
        def _convert_polygon(polygon):
            """The helper function to convert a polygon location

            :param polygon: Polygon location to be converted
            :returns: Converted coordinates
            :rtype: list
            """
            coordinates = []
            for coordinate in polygon:
                pairs = []
                for pair in coordinate:
                    lon, lat = pair[0:2]
                    # Only convert the coordinates if not in decimal format
                    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                        lon_lat = list(proj(lon, lat, inverse=True))
                        pair = (
                            lon_lat + pair[2:] if len(pair) >= 2 else lon_lat
                        )
                    pairs.append(pair)
                coordinates.append(pairs)
            return coordinates

        response = requests.get(url, params=params)

        if response.status_code == 200:
            response_json = response.json()

            for feature in response_json['features']:
                geometry = feature['geometry']
                coordinates = []

                if 'type' in geometry:
                    geometry_type = geometry['type']

                    if geometry_type == 'Polygon':
                        coordinates = _convert_polygon(geometry['coordinates'])
                    elif geometry_type == 'MultiPolygon':
                        for polygon in geometry['coordinates']:
                            coordinates.append(_convert_polygon(polygon))
                    else:
                        logger.warning((
                            f'Ignoring unknown geometry type: {geometry_type}.'
                            f' (id: {feature["id"]})'
                        ))
                elif 'rings' in geometry:
                    coordinates = _convert_polygon(geometry['rings'])
                    feature['geometry']['type'] = 'rings'
                feature['geometry']['coordinates'] = coordinates

        return response_json

    def get_library_hours(self):
        """Get library open hours via library API

        :returns: Library open hours
        :rtype: dict
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
            open_hours = {}

            for day in range(7):
                week_day = self.today + timedelta(days=day)
                open_hours[utils.to_date(week_day)] = []

            for value in response.json().values():
                datetime_format = '%Y-%m-%d %I:%M%p'
                date_format = '%Y-%m-%d'

                date = value['sortable_date']
                begin = utils.format_library_hour(value['open'])
                close = utils.format_library_hour(value['close'])

                start_format = datetime_format if begin else date_format
                end_format = datetime_format if begin else date_format

                raw_start = f'{date} {begin}'.strip()
                raw_end = f'{date} {close}'.strip()

                start = datetime.strptime(raw_start, start_format)
                end = datetime.strptime(raw_end, end_format)

                open_hours[date] = {
                    'summary': None,
                    'uid': None,
                    'start': utils.to_utc_string(start),
                    'end': utils.to_utc_string(end),
                    'sequence': None,
                    'recurrenceId': None,
                    'lastModified': None
                }
            return open_hours

    def generate_json_resources(self):
        """
        Generate resources and write to JSON files
        """
        base_url = self.config['locationsApi']['url']
        facil_locations = self.get_facil_locations()
        gender_inclusive_restrooms = self.get_gender_inclusive_restrooms()
        arcgis_geometries = self.get_arcgis_geometries()
        locations = []

        # Merge facil locations, gender inclusive restrooms and geometry data
        for location_id, raw_facil in facil_locations.items():
            raw_gir = gender_inclusive_restrooms.get(location_id)
            raw_geo = arcgis_geometries.get(location_id)
            facil_location = FacilLocation(
                raw_facil, raw_gir, raw_geo, self.proj_2913
            )

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
        locations += self.get_fields()  # field locations
        locations += self.get_places()  # place locations
        locations += concurrent_res[0]  # dining locations
        locations += concurrent_res[1]['locations']  # extra service locations

        extra_services = concurrent_res[1]['services']
        campus_map_data = self.get_campus_map_data()
        combined_locations = []
        merge_data = []
        for location in locations:
            resource_id = location.calculate_hash_id()

            # Merge with campus map data
            if resource_id in campus_map_data:
                campus_resource = campus_map_data[resource_id]
                location.address = campus_resource['address']
                location.description = campus_resource['description']
                location.descriptionHtml = campus_resource['descriptionHTML']
                location.images = campus_resource['images']
                location.thumbnails = campus_resource['thumbnail']
                location.website = campus_resource['mapUrl']
                location.synonyms = campus_resource['synonyms']

                # Add open hours to The Valley Library (Building ID: 0036)
                if location.bldg_id == '0036':
                    location.open_hours = self.get_library_hours()

            if location.merge:
                merge_data.append(location)
            else:
                combined_locations.append(location)

        # Merge data with the original locations
        for data in merge_data:
            for orig in combined_locations:
                if (
                    orig.bldg_id == data.concept_title
                    and not orig.merge
                ):
                    self.open_hours = data.open_hours
                    self.tags = orig.tags + data.tags

        # Append service relationships to each location
        for service in extra_services:
            for orig in combined_locations:
                if (
                    orig.bldg_id == service.parent
                    and not service.merge
                ):
                    orig.relationships['services']['data'].append({
                        'id': service.calculate_hash_id(),
                        'type': service.type
                    })

        # Build location resources
        combined_resources = []
        summary = defaultdict(int)
        for location in combined_locations:
            summary[location.source] += 1
            resource = location.build_resource(base_url)
            combined_resources.append(resource)

        total_number = 0
        summary_table = []
        for location_type, number in summary.items():
            summary_table.append([location_type, number])
            total_number += number
        summary_table.append(['total', total_number])
        table_output = tabulate(
            summary_table,
            headers=['Location Type', 'Number'],
            tablefmt='fancy_grid'
        )
        logger.info(f"\n{table_output}")

        output_folder = 'build'
        # Write location data to output file
        locations_output = f'{output_folder}/locations-combined.json'
        os.makedirs(os.path.dirname(locations_output), exist_ok=True)
        with open(locations_output, 'w') as file:
            json.dump(combined_resources, file)

        # Build service resources
        services = []
        for service in extra_services:
            resource = service.build_resource(base_url)
            services.append(resource)

        # Write services data to output file
        services_output = f'{output_folder}/services.json'
        os.makedirs(os.path.dirname(services_output), exist_ok=True)
        with open(services_output, 'w') as file:
            json.dump(services, file)


if __name__ == '__main__':
    arguments = utils.parse_arguments()

    # Setup logging level
    logging.basicConfig(
        level=(logging.DEBUG if arguments.debug else logging.INFO)
    )
    logger = logging.getLogger(__name__)

    locations_generator = LocationsGenerator(arguments)
    locations_generator.generate_json_resources()
