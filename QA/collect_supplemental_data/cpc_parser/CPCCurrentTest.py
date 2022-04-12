import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class CPCTest(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

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

    def runTests(self):
        for table in self.table_config:
                self.test_yearly_count(table)
        super(CPCTest, self).runTests()


if __name__ == '__main__':
    qc = CPCTest(get_current_config(**{
            "execution_date": datetime.date(2021, 10, 1)
            }))
    qc.runTests()
