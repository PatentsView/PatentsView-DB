import datetime
import json
import os

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class MergeTestWeekly(DatabaseTester):

    def __init__(self, config, run_id):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def runTests(self):
        # self.test_merge_status()
        skiplist = ['patent']
        for table in self.table_config:
            if table[:1] >= 'r':
                print(f"Beginning Test for {table} in {self.database_section}")
                if table in skiplist:
                    continue
                self.test_blank_count(table, self.table_config[table])
                self.test_related_floating_entities(table, table_config=self.table_config[table])
                self.load_nulls(table, self.table_config[table])
                for field in self.table_config[table]["fields"]:
                    print(f"\tBeginning tests for {field} in {table}")
                    self.test_null_byte(table, field)
                    if "date_field" in self.table_config[table]["fields"][field] and \
                            self.table_config[table]["fields"][field]["date_field"]:
                        self.test_zero_dates(table, field)
                if table == 'patent':
                    self.test_patent_abstract_null(table)
                print(f"Finished With Table: {table}")
            #     self.current_status[str(self.run_id)][table] = 1
            # with open(self.status_file, 'a', encoding="utf-8") as file:
            #     json.dump(self.current_status, file)

    # def test_merge_status(self):
    #     status_folder = os.path.join(self.project_home, "updater", 'create_databases')
    #     self.status_file = os.path.join(status_folder, 'merge_status.json')
    #     self.current_status = json.load(open(self.status_file))
    #     os.remove(self.status_file)
    #     if str(self.run_id) in self.current_status.keys():
    #         current_run_status = self.current_status[str(self.run_id)]
    #         if sum(current_run_status.values()) == 0:
    #             print("Json Loaded with Tables for Current Run But Tests haven't Run. Running Now ....")
    #         elif sum(current_run_status.values()) < len(self.table_config):
    #             raise Exception("Some tables were not loaded {lst}".format(
    #                 lst=set(self.table_config.keys()).difference(current_run_status.keys())))
    #     else:
    #         d = {}
    #         for i in self.table_config.keys():
    #             d[i] = 0
    #         self.current_status[str(self.run_id)] = d


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2021, 10, 5)
    })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    # print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    # print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    # print(config['PATENTSVIEW_DATABASES']["TEXT_DB"])
    run_id = "scheduled__2021-12-11T09:00:00+00:00"
    mc = MergeTestWeekly(config, run_id)
    mc.runTests()
