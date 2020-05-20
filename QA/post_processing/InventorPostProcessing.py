import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester


class InventorPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {'rawinventor': {'uuid': {'data_type': 'varchar', 'null_allowed': False, 'category': False},
                                             'name_last': {'data_type': 'varchar', 'null_allowed': True,
                                                           'category': False},
                                             'patent_id': {'data_type': 'varchar', 'null_allowed': False,
                                                           'category': False},
                                             'sequence': {'data_type': 'int', 'null_allowed': False, 'category': False},
                                             'inventor_id': {'data_type': 'varchar', 'null_allowed': True,
                                                             'category': False},
                                             'rule_47': {'data_type': 'varchar', 'null_allowed': True,
                                                         'category': False},
                                             'rawlocation_id': {'data_type': 'varchar', 'null_allowed': True,
                                                                'category': False},
                                             'deceased': {'data_type': 'varchar', 'null_allowed': True,
                                                          'category': False},
                                             'name_first': {'data_type': 'varchar', 'null_allowed': True,
                                                            'category': False}}}
        self.entity_table = 'rawinventor'
        self.disambiguated_id = 'inventor_id'
        self.disambiguation_table = 'inventor'

