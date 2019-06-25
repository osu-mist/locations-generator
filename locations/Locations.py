import logging
import re

from overrides import overrides

from utils import get_md5_hash


class Location:
    """
    Base location type
    """
    def __init__(self):
        # default attributes of a location
        self.attr = {
            'name': None,
            'tags': [],
            'open_hours': [],
            'type': None,
            'parents': None,
            'location_id': None,
            'merge': False,
            'abbreviation': None,
            'geo_location': None,
            'geometry': None,
            'summary': None,
            'description': None,
            'description_html': None,
            'address': None,
            'city': None,
            'state': None,
            'zip': None,
            'county': None,
            'telephone': None,
            'fax': None,
            'thumbnails': [],
            'images': [],
            'departments': [],
            'website': None,
            'sqft': None,
            'calendar': None,
            'campus': None,
            'gir_count': None,
            'gir_limit': None,
            'gir_locations': None,
            'synonyms': [],
            'bldg_id': None,
            'parking_zone_group': None,
            'prop_id': None,
            'ada_parking_space_count': None,
            'motorcycle_parking_space_count': None,
            'ev_parking_space_count': None,
            'weekly_menu': None
        }

    def get_primary_id(self):
        """
        The function to get location's primary ID and needs to be overrode
        """
        pass

    def set_attributes(self):
        """
        The function to set location's attributes and needs to be overrode
        """
        pass

    def _create_geo_location(self, longitude, latitude):
        """
        A helper function to generate geo location object
        """
        if longitude and latitude:
            return {
                'longitude': longitude,
                'latitude': latitude
            }

    def _create_geometry(self, coordinates_type, coordinates):
        """
        A helper function to generate geometry object
        """
        if coordinates_type and coordinates:
            return {
                'type': type,
                'coordinates': coordinates
            }

    def build_resource(self, api_base_url):
        """
        The function to generate geo location object
        """
        self.set_attributes()
        resource_id = get_md5_hash(f'{self.type}{self.get_primary_id()}')

        return {
            'id': resource_id,
            'type': 'locations',
            'attributes': self.attr,
            'links': {
                'self': f'{api_base_url}/locations/{resource_id}'
            }
        }


class ExtraLocation(Location):
    """
    The location type for extra locations
    """
    def __init__(self, raw):
        self.name = raw.get('name')
        self.bldg_id = raw.get('bldgID')
        self.campus = raw.get('campus')
        self.type = raw.get('type')
        self.tags = raw.get('tags')
        self.geo_location = self._create_geo_location(
            raw.get('latitude'),
            raw.get('longitude')
        )

    @overrides
    def get_primary_id(self):
        return self.bldg_id or self.name

    @overrides
    def set_attributes(self):
        self.attr['name'] = self.name
        self.attr['bldg_id'] = self.bldg_id
        self.attr['geo_location'] = self.geo_location
        self.attr['type'] = self.type
        self.attr['campus'] = self.campus


class ExtensionLocation:
    """
    The location type for extension locations
    """
    def __init__(self, raw):
        self.guid = raw['GUID']
        self.type = 'building'
        self.campus = 'Extension'
        self.geo_location = self._create_geo_location(raw.get('GeoLocation'))
        self.group_name = raw.get('GroupName')
        self.street_address = raw.get('StreetAddress')
        self.city = raw.get('City')
        self.state = raw.get('State')
        self.zip = raw.get('ZIPCode')
        self.fax = raw.get('fax')
        self.telephone = raw.get('tel')
        self.county = raw.get('country')
        self.location_url = raw.get('location_url')

    @overrides
    def _create_geo_location(self, geo_location):
        if geo_location:
            search = re.search(r'-?\d+(\.\d+)?', geo_location)
            return {
                'longitude': search.group(1),
                'latitude': search.group(2)
            }

    @overrides
    def get_primary_id(self):
        return self.guid

    @overrides
    def set_attributes(self):
        self.attr['name'] = self.group_name
        self.attr['geo_location'] = self.geo_location
        self.attr['address'] = self.street_address
        self.attr['city'] = self.city
        self.attr['state'] = self.state
        self.attr['zip'] = self.zip
        self.attr['telephone'] = self.telephone
        self.attr['fax'] = self.fax
        self.attr['county'] = self.county
        self.attr['website'] = self.location_url
        self.attr['type'] = self.type
        self.attr['campus'] = self.campus


class FacilLocation:
    """
    The location type for facil locations
    """
    def __init__(self, raw_facil, raw_gir, raw_geo):
        """
        Merge Banner locations with the data of gender inclusive restrooms and
        geometries from ArcGIS
        """
        address1 = raw_facil.get('address1')
        address2 = raw_facil.get('address2')

        self.bldg_id = raw_facil['id']
        self.type = 'building'
        self.abbreviation = raw_facil.get('abbreviation')
        self.name = raw_facil.get('name')
        self.campus = self._get_pretty_campus(raw_facil.get('campus'))
        self.address = self._get_address(address1, address2)
        self.city = raw_facil.get('city')
        self.state = raw_facil.get('state')
        self.zip = raw_facil.get('zip')
        self.geo_location = self._create_geo_location(
            raw_geo['longitude'] if raw_geo else None,
            raw_geo['latitude'] if raw_geo else None
        )
        self.geometry = self._create_geometry(
            raw_geo['coordinatesType'] if raw_geo else None,
            raw_geo['coordinates'] if raw_geo else None
        )
        self.gir_count = raw_gir['count'] if raw_gir else 0
        self.gir_limit = raw_gir['limit'] if raw_gir else None
        self.gir_locations = raw_gir['all'].strip() if raw_gir else None
        self.arcGIS_abbreviation = (
            (raw_geo.get('abbreviation') if raw_geo else None)
            or (raw_gir.get('abbreviation') if raw_gir else None)
        )

    def _get_pretty_campus(self, raw_campus):
        campus_dict = {
            'cascadescampus': 'Cascades',
            'osucampus': 'Corvallis',
            'hmsc': 'HMSC'
        }
        campus = None
        raw_campus = raw_campus.lower()
        if raw_campus in campus_dict:
            campus = campus_dict[raw_campus]
        elif raw_campus:
            campus = 'Other'

        return campus

    def _get_address(self, address1, address2):
        return f'{address1}\n{address2}' if address2 else address1

    @overrides
    def get_primary_id(self):
        return self.bldg_id

    @overrides
    def set_attributes(self):
        self.attr['name'] = self.group_name
        self.attr['abbreviation'] = self.abbreviation
        self.attr['geo_location'] = self.geo_location
        self.attr['geometry'] = self.geometry
        self.attr['type'] = self.type
        self.attr['campus'] = self.campus
        self.attr['address'] = self.street_address
        self.attr['city'] = self.city
        self.attr['state'] = self.state
        self.attr['zip'] = self.zip
        self.attr['gir_count'] = self.gir_count
        self.attr['gir_limit'] = self.gir_limit
        self.attr['gir_locations'] = self.gir_locations
        self.attr['bldg_id'] = self.bldg_id


class ParkingLocation:
    """
    The location type for parking locations
    """
    def __init__(self, raw):
        properties = raw['properties']
        geometry = raw.get('geometry')

        self.prop_id = properties['Prop_ID']
        self.parking_zone_group = properties['ZoneGroup']
        self.type = 'parking'
        self.campus = 'Corvallis'
        self.description = properties.get('AiM_Desc')
        self.ada_parking_count = properties.get('ADA_Spc')
        self.moto_parking_count = properties.get('MCycle_Spc')
        self.ev_parking_count = properties.get('EV_Spc')
        self.lat = properties.get('Cent_Lat')
        self.lon = properties.get('Cent_Lon')
        self.geo_location = self._create_geo_location(self.lon, self.lat)
        self.geometry = self._create_geometry(
            geometry.get('type') if geometry else None,
            geometry.get('coordinates') if geometry else None
        )

    @overrides
    def get_primary_id(self):
        return f'{self.prop_id}{self.parking_zone_group}'

    @overrides
    def set_attributes(self):
        self.attr['name'] = self.group_name
        self.attr['parking_zone_group'] = self.parking_zone_group
        self.attr['geometry'] = self.geometry
        self.attr['type'] = self.type
        self.attr['campus'] = self.campus
        self.attr['prop_id'] = self.prop_id
        self.attr['ada_parking_space_count'] = self.ada_parking_count
        self.attr['motorcycle_parking_space_count'] = self.moto_parking_count
        self.attr['ev_parking_space_count'] = self.ev_parking_count
        self.attr['geo_location'] = self.geo_location


class ServiceLocation:
    """
    The location type for dining locations and the locations from extra data
    """
    def __init__(self, raw, location_type='other', week_menu_url=None):
        latitude, longitude, weekly_menu = None, None, None

        if raw.get('concept_coord'):
            coordinates = raw['concept_coord'].split(',')
            latitude = coordinates[0].strip()
            longitude = coordinates[1].strip()

        if raw.get('loc_id'):
            weekly_menu = f'{week_menu_url}?loc={raw["loc_id"]}'

        self.calendar_id = raw.get('calendar_id') or raw['calendarId']
        self.type = location_type
        self.campus = 'Corvallis'
        self.concept_title = raw.get('concept_title') or raw.get('id')
        self.zone = raw.get('zone')
        self.summary = f'Zone: {raw.get("zone")}' if raw.get('zone') else ''
        self.lat = latitude
        self.lon = longitude
        self.geo_location = self._create_geo_location(longitude, latitude)
        self.weekly_menu = weekly_menu
        self.start = raw.get('start')
        self.end = raw.get('end')
        self.tags = raw.get('tags')
        self.parent = raw.get('parent')
        self.merge = False
        self.open_hours = None

    @overrides
    def get_primary_id(self):
        return self.calendar_id

    @overrides
    def set_attributes(self):
        self.attr['name'] = self.concept_title
        self.attr['geo_location'] = self.geo_location
        self.attr['summary'] = self.summary
        self.attr['type'] = self.type
        self.attr['campus'] = self.campus
        self.attr['open_hours'] = self.open_hours
        self.attr['merge'] = self.merge
        self.attr['tags'] = self.tags
        self.attr['parent'] = self.parent
        self.attr['weekly_menu'] = self.weekly_menu
