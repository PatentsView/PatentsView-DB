import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class MergeTestQuarterly(DatabaseTester):

    def __init__(self, config, run_id):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)

    def test_merge_status(self):
        status_folder = os.path.join(self.project_home, "updater", 'create_databases')
        self.status_file = os.path.join(status_folder, 'merge_status.json')
        self.current_status = json.load(open(self.status_file))
        os.remove(self.status_file)
        if str(self.run_id) in self.current_status.keys():
            current_run_status = self.current_status[str(self.run_id)]
            if sum(current_run_status.values()) == 0:
                print("Json Loaded with Tables for Current Run But Tests haven't Run. Running Now ....")
            elif sum(current_run_status.values()) < len(self.table_config):
                raise Exception("Some tables were not loaded {lst}".format(
                    lst=set(self.table_config.keys()).difference(current_run_status.keys())))
        else:
            d = {}
            for i in self.table_config.keys():
                d[i] = 0
            self.current_status[str(self.run_id)] = d


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
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2020, 12, 29)
    })
    breakpoint()
    # fill with correct run_id
    run_id = "scheduled__2021-12-11T09:00:00+00:00"
    mcq = MergeTestQuarterly(config, run_id)
    mcq.runTests()
