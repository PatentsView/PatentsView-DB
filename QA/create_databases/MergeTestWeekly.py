import datetime
import json
import os

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class MergeTestWeekly(DatabaseTester):

    def __init__(self, config, run_id):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def init_qa_dict(self):
        super().init_qa_dict()

    def runTests(self):
        self.init_qa_dict()
        # skiplist = []
        for table in self.table_config:
            # if table[:2] >= 'pu':
            print(" -------------------------------------------------- ")
            print(f"BEGINNING TESTS FOR {self.database_section}.{table}")
            print(" -------------------------------------------------- ")
            self.test_blank_count(table, self.table_config[table], where_vi=True)
            self.test_related_floating_entities(table, table_config=self.table_config[table], where_vi=True)
            self.load_nulls(table, self.table_config[table], where_vi=True)
            for field in self.table_config[table]["fields"]:
                print(f"\tBeginning tests for {field} in {table}")
                if (field != 'uuid') and (self.table_config[table]['fields'][field]['data_type'] in ['varchar', 'text', 'mediumtext', 'longtext']):
                    self.test_null_byte(table, field, where_vi=True)
                if "date_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["date_field"]:
                    self.test_zero_dates(table, field, where_vi=True)
            if table == 'patent':
                self.test_patent_abstract_null(table, where_vi=True)
            print(" -------------------------------------------------- ")
            print(f"FINISHED TABLE: {table}")
            print(" -------------------------------------------------- ")


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2021, 10, 5)
    })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    run_id = "scheduled__2021-12-11T09:00:00+00:00"
    mc = MergeTestWeekly(config, run_id)
    mc.runTests()
