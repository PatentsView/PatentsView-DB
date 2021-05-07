import datetime
import json
import os

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_current_config


class MergeTest(PatentDatabaseTester):

    def runTests(self):
        self.test_merge_status()
        super(MergeTest, self).runTests()

    def __init__(self, config, run_id):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        table_keys = ['application', 'botanic', 'figures', 'foreigncitation', 'foreign_priority',
                    'government_interest', 'ipcr', 'mainclass', 'non_inventor_applicant',
                    'otherreference', 'patent', 'pct_data', 'rawassignee', 'rawinventor',
                    'rawlawyer', 'rawexaminer', 'rawlocation', 'rel_app_text', 'subclass',
                    'usapplicationcitation', 'uspatentcitation',
                    'uspc', 'usreldoc', 'us_term_of_grant']

        self.table_config = {key: value for key, value in self.table_config.items() if key in table_keys}

        self.config = config

        self.project_home = os.environ['PACKAGE_HOME']
        self.run_id = run_id

    def test_merge_status(self):
        status_folder = os.path.join(self.project_home, "updater", 'create_databases')
        status_file = os.path.join(status_folder, 'merge_status.json')
        current_status = json.load(open(status_file))
        current_run_status = current_status[str(self.run_id)]
        if sum(current_run_status.values()) < len(self.table_config):
            raise Exception("Some tables were not loaded {lst}".format(
                    lst=set(self.table_config.keys()).difference(current_run_status.keys())))

    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        count_query = "SELECT count(*) from {tbl} where abstract is null and type!='design' and type!='reissue'".format(
                tbl=table)
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                        "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. "
                        "Count: {count}".format(
                                database=self.database_section, table=table,
                                count=count_value))


if __name__ == '__main__':
    #config = get_config()
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
    })

    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"

    mc = MergeTest(config,run_id)

    mc.runTests()




