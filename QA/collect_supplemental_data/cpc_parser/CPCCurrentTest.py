import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester


class CPCTest(PatentDatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'NEW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {'cpc_current': {'uuid': {'data_type': 'varchar', 'null_allowed': False},
                                             'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'sequence': {'data_type': 'int', 'null_allowed': False},
                                             'section_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'subsection_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'group_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'subgroup_id': {'data_type': 'varchar', 'null_allowed': False},
                                             'category': {'data_type': 'varchar', 'null_allowed': False}},
                             'wipo': {'patent_id': {'data_type': 'varchar', 'null_allowed': False},
                                      'sequence': {'data_type': 'int', 'null_allowed': False},
                                      'field_id': {'data_type': 'varchar', 'null_allowed': False}},
                             'mainclass_current': {'id': {'data_type': 'varchar', 'null_allowed': False},
                                                   'title': {'data_type': 'int', 'null_allowed': False}},
                             'subclass_current': {'id': {'data_type': 'varchar', 'null_allowed': False},
                                                  'title': {'data_type': 'int', 'null_allowed': False}},
                             'mainclass': {'id': {'data_type': 'varchar', 'null_allowed': False}},
                             'subclass': {'id': {'data_type': 'varchar', 'null_allowed': False}}}

    def test_yearly_count(self):
        start_date = datetime.datetime.strptime(self.config['DATES']['START_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(self.config['DATES']['END_DATE'], '%Y%m%d')
        start_date_string = start_date.strftime('%Y-%m-%d')
        end_date_string = end_date.strftime('%Y-%m-%d')
        for table in ['cpc_current', 'wipo']:
            if not self.connection.open:
                self.connection.connect()

            with self.connection.cursor() as count_cursor:
                in_between_query = "SELECT count(1) as new_count from {table} t join patent p on p.id =t.patent_id and p.date  between '{start_dt}' and '{end_dt}'".format(
                    table=table, start_dt=start_date_string, end_dt=end_date_string)
                count_cursor.execute(in_between_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value < 1:
                    raise AssertionError(
                        "Table doesn not have new data : {table}, date range '{start_dt}' to '{end_dt}' ".format(
                            table=table, start_dt=start_date_string, end_dt=end_date_string))
