import datetime

import pandas as pd

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config


class AssigneePostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'UPDATE_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
                'rawassignee':     {
                        'fields': {
                                'patent_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_last':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'assignee_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'organization':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'rawlocation_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'sequence':       {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'type':           {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_first':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'assignee':        {
                        "fields":           {
                                "id":           {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "type":         {
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
                        "related_entities": [
                                {
                                        'table':          'patent_assignee',
                                        'source_id':      'id',
                                        'destination_id': 'assignee_id'
                                        }, {
                                        'table':          'rawassignee',
                                        'source_id':      'id',
                                        'destination_id': 'assignee_id'
                                        }]
                        },
                'patent_assignee': {
                        "fields": {
                                "patent_id":   {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "assignee_id": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        }
                                }
                        },

                }
        self.entity_table = 'rawassignee'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'assignee_id'
        self.disambiguated_table = 'assignee'
        self.disambiguated_data_fields = ['name_last', 'name_first', 'organization']
        self.patent_exclusion_list.extend(['assignee'])
        self.add_persistent_table_to_config()

    def assert_name_or_organization(self):
        query = 'SELECT count(1) from assignee where name_first is null and name_last is null and organization is null'
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            empties = cursor.fetchall()[0][0]
            if empties > 0:
                print(query)
                raise Exception(
                        "There are {x} entries in assignee table with NULL name and NULL organization".format(
                                x=empties))

    def add_persistent_table_to_config(self):
        columns_query = """
            select COLUMN_NAME,
               case
                   when DATA_TYPE in ('varchar', 'tinytext', 'text', 'mediumtext', 'longtext') then 'varchar'
                   else 'int' end                                           data_type,
               case when COLUMN_KEY = 'PRI' then 'False' else 'True' end as null_allowed,
               'False'                                                   as category    
            from information_schema.COLUMNS
            where TABLE_NAME = 'persistent_assignee_disambig'
              and TABLE_SCHEMA = '{db}'
              and column_name not in ('updated_date','created_date', 'version_indicator');
        """.format(db=self.config['PATENTSVIEW_DATABASES']['UPDATE_DB'])
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as crsr:
            crsr.execute(columns_query)
            column_data = pd.DataFrame.from_records(
                    crsr.fetchall(),
                    columns=['column', 'data_type', 'null_allowed', 'category'])
            table_config = {
                    'persistent_assignee_disambig': {
                            'fields': column_data.set_index('column').to_dict(orient='index')
                            }
                    }
            self.table_config.update(table_config)

    def runTests(self):
        self.assert_name_or_organization()
        super(AssigneePostProcessingQC, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })
    print({section: dict(config[section]) for section in config.sections()})
    qc = AssigneePostProcessingQC(config)
    qc.runTests()
