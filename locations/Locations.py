from abc import ABC, abstractmethod
import re

from utils import get_md5_hash


class Location(ABC):
    """
    Base location type
    """
    def _init_attributes(self):
        """
        Default attributes of a location. All fields in attributes should be
        camelCase since it will be used to generate a JSON resource object.
        """
        self.attr = {
            'name': None,
            'tags': [],
            'openHours': {},
            'type': None,
            'parent': None,
            'locationId': None,
            'bannerAbbreviation': None,
            'arcgisAbbreviation': None,
            'geoLocation': None,
            'geometry': None,
            'summary': None,
            'description': None,
            'descriptionHtml': None,
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
            'girCount': None,
            'girLimit': False,
            'girLocations': None,
            'synonyms': [],
            'bldgId': None,
            'parkingZoneGroup': None,
            'propId': None,
            'adaParkingSpaceCount': None,
            'motorcycleParkingSpaceCount': None,
            'evParkingSpaceCount': None,
            'weeklyMenu': None,
            'notes': None,
            'labels': {},
            'steward': None,
            'shape': {}
        }

    @abstractmethod
    def get_primary_id(self):
        """
        The function to get location's primary ID and needs to be overrode
        """

    @abstractmethod
    def _set_attributes(self):
        """
        The function to set location's attributes and needs to be overrode
        """

    def update_attributes(self, key, value):
        """The helper function update attribute value

        :param key: Attribute key
        :param value: Attribute value
        """
        if key not in self.attr:
            raise KeyError(f'{key} is not defined in location attributes.')
        else:
            self.attr[key] = value

    def _create_geo_location(self, longitude, latitude):
        """The helper function to generate geo location object

        :param longitude: Longitude
        :param latitude: Latitude
        :returns: Geolocation object
        :rtype: dict
        """
        if longitude and latitude:
            return {
                'longitude': longitude,
                'latitude': latitude
            }

    def _create_geometry(self, coordinates_type, coordinates):
        """The helper function to generate geometry object

        :param coordinates_type: Coordinates type
        :param coordinates: Coordinates
        :returns: Geometry object
        :rtype: dict
        """
        if coordinates_type and coordinates:
            return {
                'type': coordinates_type,
                'coordinates': coordinates
            }

    def calculate_hash_id(self):
        """The function to calculate location's hash ID

        :returns: MD5 hash ID string
        :rtype: str
        """
        return get_md5_hash(f'{self.type}{self.get_primary_id()}')

    def build_resource(self, api_base_url):
        """The function to build location resource

        :param api_base_url: API base URL
        :returns: Location resource adhere to JSONAPI convention
        :rtype: dict
        """
        self._set_attributes()
        resource_id = self.calculate_hash_id()

        return {
            'id': resource_id,
            'type': 'services' if self.type == 'services' else 'locations',
            'attributes': self.attr,
            'links': {
                'self': f'{api_base_url}/locations/{resource_id}'
            },
            'relationships': self.relationships
        }


class ExtraLocation(Location):
    """
    The location type for extra locations
    """
    def __init__(self, raw):
        self._init_attributes()
        self.source = 'extra-location'
        self.name = raw.get('name')
        self.bldg_id = raw.get('bldgID')
        self.campus = raw.get('campus')
        self.type = raw.get('type')
        self.tags = raw.get('tags')
        self.geo_location = self._create_geo_location(
            raw.get('latitude'),
            raw.get('longitude')
        )
        self.relationships = {'services': {'data': []}}
        self.merge = False

    def get_primary_id(self):
        return self.bldg_id or self.name

    def _set_attributes(self):
        attributes = {
            'name': self.name,
            'bldgId': self.bldg_id,
            'geoLocation': self.geo_location,
            'type': self.type,
            'campus': self.campus
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class ExtensionLocation(Location):
    """
    The location type for extension locations
    """
    def __init__(self, raw):
        self._init_attributes()
        self.source = 'extension-location'
        self.guid = raw['GUID']
        self.type = 'other'
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
        self.relationships = {'services': {'data': []}}
        self.merge = False
        self.bldg_id = None

    def _create_geo_location(self, geo_location):
        if geo_location:
            search = re.search(r'-?\d+(\.\d+)?', geo_location)
            return {
                'longitude': search.group(1),
                'latitude': search.group(2)
            }

    def get_primary_id(self):
        return self.guid

    def _set_attributes(self):
        attributes = {
            'name': self.group_name,
            'geoLocation': self.geo_location,
            'address': self.street_address,
            'city': self.city,
            'state': self.state,
            'zip': self.zip,
            'telephone': self.telephone,
            'fax': self.fax,
            'county': self.county,
            'website': self.location_url,
            'type': self.type,
            'campus': self.campus
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class FacilLocation(Location):
    """
    The location type for facil locations
    """
    def __init__(self, raw_facil, raw_gir, raw_geo, proj):
        """Merge Banner locations with the data of gender inclusive restrooms and
        geometries from ArcGIS

        :param raw_facil: Raw facil locations to be merged
        :param raw_gir: Raw gender inclusive restrooms to be merged
        :param raw_geo: Raw geometry data to be merged
        :param proj: PROJ object to transform coordinates
        """
        address1 = raw_facil.get('address1')
        address2 = raw_facil.get('address2')

        lon_lat = None
        if raw_geo:
            lon_lat = proj(
                raw_geo['longitude'],
                raw_geo['latitude'],
                inverse=True
            )

        self._init_attributes()
        self.source = 'facil-location'
        self.bldg_id = raw_facil['id']
        self.type = 'building'
        self.tags = []
        self.banner_abbreviation = raw_facil.get('abbreviation')
        self.name = raw_facil.get('name')
        self.campus = self._get_pretty_campus(raw_facil.get('campus'))
        self.address = self._get_address(address1, address2)
        self.city = raw_facil.get('city')
        self.state = raw_facil.get('state')
        self.zip = raw_facil.get('zip')
        self.geo_location = self._create_geo_location(
            lon_lat[0] if lon_lat else None,
            lon_lat[1] if lon_lat else None
        )
        self.geometry = self._create_geometry(
            raw_geo['coordinatesType'] if raw_geo else None,
            raw_geo['coordinates'] if raw_geo else None
        )
        self.gir_count = raw_gir['count'] if raw_gir else 0
        self.gir_limit = bool(raw_gir['limit'].strip()) if raw_gir else None
        self.gir_locations = raw_gir['all'].strip() if raw_gir else None
        self.arcgis_abbreviation = (
            (raw_geo.get('abbreviation') if raw_geo else None)
            or (raw_gir.get('abbreviation') if raw_gir else None)
        )
        self.relationships = {'services': {'data': []}}
        self.merge = False
        self.open_hours = {}
        self.description = None
        self.descriptionHtml = None
        self.images = None
        self.thumbnails = []
        self.website = None
        self.synonyms = None

    def _get_pretty_campus(self, raw_campus):
        """The helper function to generate pretty campus string

        :param raw_campus: Raw campus string
        :returns: Pretty campus string
        :rtype: str
        """
        campus_dict = {
            'cascadescampus': 'Cascades',
            'osucorvallis': 'Corvallis',
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
        """The helper function to generate full address

        :param address1: Address 1 from raw data
        :param address2: Address 2 from raw data
        :returns: Pretty campus string
        :rtype: str
        """
        return f'{address1}\n{address2}' if address2 else address1

    def get_primary_id(self):
        return self.bldg_id

    def _set_attributes(self):
        attributes = {
            'name': self.name,
            'bannerAbbreviation': self.banner_abbreviation,
            'arcgisAbbreviation': self.arcgis_abbreviation,
            'geoLocation': self.geo_location,
            'geometry': self.geometry,
            'type': self.type,
            'campus': self.campus,
            'address': self.address,
            'city': self.city,
            'state': self.state,
            'zip': self.zip,
            'girCount': self.gir_count,
            'girLimit': self.gir_limit,
            'girLocations': self.gir_locations,
            'bldgId': self.bldg_id,
            'openHours': self.open_hours,
            'description': self.description,
            'descriptionHtml': self.descriptionHtml,
            'images': self.images,
            'thumbnails': self.thumbnails,
            'website': self.website,
            'synonyms': self.synonyms
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class FieldLocation(Location):
    """
    The location type for field locations
    """
    def __init__(self, raw):
        attributes = raw['attributes']
        geometry = raw.get('geometry')

        self._init_attributes()
        self.source = 'field-location'
        self.prop_id = attributes['Prop_ID']
        self.type = 'field'
        self.name = attributes['Field_Nam']
        self.description = attributes['Description']
        self.notes = attributes['Notes']
        self.label_1 = attributes['Label_1']
        self.label_2 = attributes['Label_2']
        self.expose = attributes['Expose']
        self.steward = attributes['Steward']
        self.image = attributes['Image']
        self.shape_area = attributes['Shape__Area']
        self.shape_length = attributes['Shape__Length']
        self.shape_acres = attributes['Shape_Acres']
        self.geometry = self._create_geometry(
            geometry.get('type') if geometry else None,
            geometry.get('coordinates') if geometry else None
        )
        self.merge = False
        self.bldg_id = None
        self.relationships = {'services': {'data': []}}

    def get_primary_id(self):
        return f'{self.prop_id}'

    def _set_attributes(self):
        attributes = {
            'name': self.name,
            'description': self.description,
            'geometry': self.geometry,
            'type': self.type,
            'propId': self.prop_id,
            'notes': self.notes,
            'labels': {
                1: self.label_1,
                2: self.label_2
            },
            'steward': self.steward,
            'images': [self.image],
            'shape': {
                'area': self.shape_area,
                'length': self.shape_length,
                'acres': self.shape_acres,
            }
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class ParkingLocation(Location):
    """
    The location type for parking locations
    """
    def __init__(self, raw):
        properties = raw['properties']
        geometry = raw.get('geometry')

        self._init_attributes()
        self.source = 'parking-location'
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
        self.relationships = {'services': {'data': []}}
        self.merge = False
        self.bldg_id = None

    def get_primary_id(self):
        return f'{self.prop_id}{self.parking_zone_group}'

    def _set_attributes(self):
        attributes = {
            'name': self.description,
            'parkingZoneGroup': self.parking_zone_group,
            'geometry': self.geometry,
            'type': self.type,
            'campus': self.campus,
            'propId': self.prop_id,
            'adaParkingSpaceCount': self.ada_parking_count,
            'motorcycleParkingSpaceCount': self.moto_parking_count,
            'evParkingSpaceCount': self.ev_parking_count,
            'geoLocation': self.geo_location
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class PlaceLocation(Location):
    """
    The location type for place locations
    """
    def __init__(self, raw):
        attributes = raw['attributes']

        self._init_attributes()
        self.source = 'place-location'
        self.prop_id = attributes['Prop_ID']
        self.uid = attributes['uID']
        self.type = 'place'
        self.name = attributes['Name']
        self.address = attributes['Loca']
        self.description = attributes.get('Desc_')
        self.website = attributes['URL_Home']
        self.lat = attributes.get('Cent_Lat')
        self.lon = attributes.get('Cent_Lon')
        self.geo_location = self._create_geo_location(self.lon, self.lat)
        self.merge = False
        self.bldg_id = None
        self.relationships = {'services': {'data': []}}

    def get_primary_id(self):
        return f'{self.prop_id}{self.uid}'

    def _set_attributes(self):
        attributes = {
            'name': self.name,
            'description': self.description,
            'address': self.address,
            'type': self.type,
            'propId': self.prop_id,
            'geoLocation': self.geo_location,
            'website': self.website
        }

        for key, value in attributes.items():
            self.update_attributes(key, value)


class ServiceLocation(Location):
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

        self._init_attributes()
        self.source = 'service-location'
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
        self.merge = True if raw.get('merge') else False
        self.open_hours = None
        self.relationships = {'services': {'data': []}}
        self.bldg_id = None

    def get_primary_id(self):
        return self.calendar_id

    def _set_attributes(self):
        if self.type == 'services':
            self.relationships = {'location': {'data': [
                {
                    'id': get_md5_hash(f'building{self.parent}'),
                    'type': 'locations'
                }
            ]}}
            self.attr = {
                'name': self.concept_title,
                'type': self.type,
                'openHours': self.open_hours,
                'tags': self.tags,
                'parent': self.parent
            }
        else:
            attributes = {
                'name': self.concept_title,
                'geoLocation': self.geo_location,
                'summary': self.summary,
                'type': self.type,
                'campus': self.campus,
                'openHours': self.open_hours,
                'tags': self.tags,
                'parent': self.parent,
                'weeklyMenu': self.weekly_menu
            }

            for key, value in attributes.items():
                self.update_attributes(key, value)
