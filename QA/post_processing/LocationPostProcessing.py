import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester


class LocationPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
            'rawlocation': {'id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'country_transformed': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'location_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'location_id_transformed': {'data_type': 'varchar', 'null_allowed': True,
                                                        'category': False},
                            'city': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'state': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'country': {'data_type': 'varchar', 'null_allowed': True, 'category': False}}}
        self.entity_table = 'rawlocation'
        self.disambiguated_id = 'location_id'
        self.disambiguation_table = 'location'
        self.waypoint_nodes = [{'table': 'rawassignee', 'id': 'rawlocation_id'},
                               {'table': 'rawinventor', 'id': 'rawlocation_id'}]
