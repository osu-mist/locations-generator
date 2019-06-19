class ServiceLocation:
    def __init__(self, raw, week_menu_url=None):
        latitude, longitude, weekly_menu = None, None, None

        if raw.get('concept_coord'):
            coordinates = raw['concept_coord'].split(',')
            latitude = coordinates[0].strip()
            longitude = coordinates[1].strip()

        if raw.get('loc_id'):
            weekly_menu = f'{week_menu_url}?loc={raw["loc_id"]}'

        self.concept_title = raw.get('concept_title') or raw.get('id')
        self.zone = raw.get('zone')
        self.calendar_id = raw.get('calendar_id') or raw.get('calendarId')
        self.latitude = latitude
        self.longitude = longitude
        self.weekly_menu = weekly_menu
        self.start = raw.get('start')
        self.end = raw.get('end')
        self.tags = raw.get('tags')
        self.parent = raw.get('parent')
        self.merge = False
        self.type = 'Other'
        self.open_hours = None

    def get_primary_id(self):
        return self.calendar_id
