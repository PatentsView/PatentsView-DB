import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester


class AssigneePostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
            'rawassignee': {'patent_id': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'name_last': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'assignee_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'organization': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'rawlocation_id': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                            'type': {'data_type': 'varchar', 'null_allowed': True, 'category': False},
                            'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                            'name_first': {'data_type': 'varchar', 'null_allowed': True, 'category': False}}}
        self.entity_table = 'rawassignee'
        self.disambiguated_id = 'assignee_id'
        self.disambiguation_table = 'assignee'
