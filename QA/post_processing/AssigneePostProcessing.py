import datetime

import pandas as pd

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config


class AssigneePostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def assert_name_or_organization(self, table):
        if table not in self.exclusion_list:
            print(f"Testing Table counts for {table} in {self.database_section}")
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

    def runTests(self):
        for table in self.table_config:
            self.assert_name_or_organization(table)
        super(AssigneePostProcessingQC, self).runTests()
        super(DisambiguationTester, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule='quarterly', **{
            "execution_date": datetime.date(2021, 10, 1)
            })
    # print({section: dict(config[section]) for section in config.sections()})
    qc = AssigneePostProcessingQC(config)
    qc.runTests()