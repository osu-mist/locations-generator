import re


class ExtraLocation:
    """
    The location type for extra locations
    """
    def __init__(self, raw):
        self.name = raw.get('name')
        self.bldg_id = raw.get('bldgID')
        self.longitude = raw.get('latitude')
        self.latitude = raw.get('longitude')
        self.campus = raw.get('campus')
        self.type = raw.get('type')
        self.tags = raw.get('tags')

    def get_primary_id(self):
        return self.bldg_id or self.name


class ExtensionLocation:
    """
    The location type for extension locations
    """
    def __init__(self, raw):
        self.guid = raw['GUID']
        self.type = 'building'
        self.campus = 'Extension'
        self.geo_location = self.__create_geo_location(raw.get('GeoLocation'))
        self.group_name = raw.get('GroupName')
        self.street_address = raw.get('StreetAddress')
        self.city = raw.get('City')
        self.state = raw.get('State')
        self.zip_code = raw.get('ZIPCode')
        self.fax = raw.get('fax')
        self.tel = raw.get('tel')
        self.county = raw.get('country')
        self.location_url = raw.get('location_url')

    def __create_geo_location(self, geo_location):
        if geo_location:
            search = re.search(r'-?\d+(\.\d+)?', geo_location)
            return {
                'longitude': search.group(1),
                'latitude': search.group(2)
            }

    def get_primary_id(self):
        return self.guid


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
        self.campus = self.__get_pretty_campus(raw_facil.get('campus'))
        self.address = self.__get_address(address1, address2)
        self.city = raw_facil.get('city')
        self.state = raw_facil.get('state')
        self.zip = raw_facil.get('zip')
        self.latitude = raw_geo['latitude'] if raw_geo else None
        self.longitude = raw_geo['longitude'] if raw_geo else None
        self.coordinates = raw_geo['coordinates'] if raw_geo else None
        self.coordinates_type = raw_geo['coordinatesType'] if raw_geo else None
        self.gir_count = raw_gir['count'] if raw_gir else 0
        self.gir_limit = raw_gir['limit'] if raw_gir else None
        self.gir_locations = raw_gir['all'].strip() if raw_gir else None
        self.arcGIS_abbreviation = (
            (raw_geo.get('abbreviation') if raw_geo else None)
            or (raw_gir.get('abbreviation') if raw_gir else None)
        )

    def __get_pretty_campus(self, raw_campus):
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

    def __get_address(self, address1, address2):
        return f'{address1}\n{address2}' if address2 else address1

    def get_primary_id(self):
        return self.bldg_id


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
        self.description = properties.get('AiM_Desc')
        self.ada_parking_spaceCount = properties.get('ADA_Spc')
        self.motorcycle_parking_space_count = properties.get('MCycle_Spc')
        self.ev_parking_space_count = properties.get('EV_Spc')
        self.latitude = properties.get('Cent_Lat')
        self.longitude = properties.get('Cent_Lon')
        self.coordinates = geometry.get('coordinates') if geometry else None
        self.coordinates_type = geometry.get('type') if geometry else None

    def get_primary_id(self):
        return f'{self.prop_id}{self.parking_zone_group}'


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
        self.concept_title = raw.get('concept_title') or raw.get('id')
        self.zone = raw.get('zone')
        self.latitude = latitude
        self.longitude = longitude
        self.weekly_menu = weekly_menu
        self.start = raw.get('start')
        self.end = raw.get('end')
        self.tags = raw.get('tags')
        self.parent = raw.get('parent')
        self.merge = False
        self.open_hours = None

    def get_primary_id(self):
        return self.calendar_id
