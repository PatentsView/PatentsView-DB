import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_config


class LawyerPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
                'rawlawyer':        {
                        'fields':           {
                                'name_last':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':         {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'organization': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'lawyer_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'country':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':     {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_first':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'lawyer':           {
                        "fields":           {
                                "id":           {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "country":      {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     True
                                        },
                                "name_first":   {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "name_last":    {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        },
                                "organization": {
                                        "data_type":    "varchar",
                                        "null_allowed": True,
                                        "category":     False
                                        }
                                },
                        "related_entities": [{
                                'table':          'patent_lawyer',
                                'source_id':      'id',
                                'destination_id': 'lawyer_id'
                                },{
                                'table':          'rawlawyer',
                                'source_id':      'id',
                                'destination_id': 'lawyer_id'
                                }]
                        },
                'patent_lawyer':    {
                        "fields": {
                                "patent_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "lawyer_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        }
                                }
                        },
                "related_entities": [{
                        'table':          'lawyer',
                        'source_id':      'lawyer_id',
                        'destination_id': 'id'
                        }]
                }

        self.entity_table = 'rawlawyer'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'lawyer_id'
        self.disambiguated_table = 'lawyer'
        self.disambiguated_data_fields = ['name_last', 'name_first', "organization", "country"]
        self.patent_exclusion_list.extend(['lawyer'])


if __name__ == '__main__':
    config = get_config()
    qc = LawyerPostProcessingQC(config)
    qc.runTests()
