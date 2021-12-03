import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_current_config


class CPCTest(PatentDatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
                'cpc_current':    {
                        'fields': {
                                'uuid':          {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':     {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':      {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'section_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'subsection_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'group_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'subgroup_id':   {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'category':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        }
                                }
                        },
                'wipo':           {
                        'fields': {
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':  {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'field_id':  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        }
                                }
                        },

                'wipo_field':     {
                        'fields': {
                                'id':            {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'section_title': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'field_title':   {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                },
                        },
                'cpc_group':      {
                        'fields':           {
                                "id":    {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "title": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        }
                                },
                        'related_entities': [
                                {
                                        'table':          'cpc_current',
                                        'source_id':      'id',
                                        'destination_id': 'group_id'
                                        }]
                        },
                'cpc_subgroup':   {
                        'fields':           {
                                "id":    {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "title": {
                                        "data_type":    "mediumtext",
                                        "null_allowed": False,
                                        "category":     False
                                        }
                                },
                        'related_entities': [
                                {
                                        'table':          'cpc_current',
                                        'source_id':      'id',
                                        'destination_id': 'subgroup_id'
                                        }]
                        },
                'cpc_subsection': {
                        'fields':           {
                                "id":    {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        },
                                "title": {
                                        "data_type":    "varchar",
                                        "null_allowed": False,
                                        "category":     False
                                        }
                                },
                        'related_entities': [
                                {
                                        'table':          'cpc_current',
                                        'source_id':      'id',
                                        'destination_id': 'subsection_id'
                                        }]
                        }
                }
        self.patent_exclusion_list.extend(['cpc_group', 'cpc_subgroup', 'cpc_subsection','wipo_field'])

    def test_yearly_count(self, table, strict=True):
        start_date = datetime.datetime.strptime(self.config['DATES']['START_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(self.config['DATES']['END_DATE'], '%Y%m%d')
        start_date_string = start_date.strftime('%Y-%m-%d')
        end_date_string = end_date.strftime('%Y-%m-%d')
        if table in ['cpc_current', 'wipo']:
            if not self.connection.open:
                self.connection.connect()

            with self.connection.cursor() as count_cursor:
                in_between_query = "SELECT count(1) as new_count from {table} t join patent p on p.id =t.patent_id " \
                                   "and p.date  between '{start_dt}' and '{end_dt}'".format(
                        table=table, start_dt=start_date_string, end_dt=end_date_string)
                count_cursor.execute(in_between_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value < 1:
                    raise AssertionError(
                            "Table doesn't not have new data : {table}, date range '{start_dt}' to '{end_dt}' ".format(
                                    table=table, start_dt=start_date_string, end_dt=end_date_string))
            super().test_yearly_count(table)


if __name__ == '__main__':
    qc = CPCTest(get_current_config(**{
            "execution_date": datetime.date(2020, 12, 29)
            }))
    qc.runTests()
