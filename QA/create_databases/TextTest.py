import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config

class TextMergeTest(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        brf_key = "brf_sum_text_{year}".format(year=end_date.year)
        clm_key = "claims_{year}".format(year=end_date.year)
        ddr_key = "draw_desc_text_{year}".format(year=end_date.year)
        ddt_key = "detail_desc_text_{year}".format(year=end_date.year)
        super().__init__(config, config['PATENTSVIEW_DATABASES']["TEXT"], datetime.date(year=1976, month=1, day=1), end_date)
        # self.patent_db_prefix = "`{db}`".format(db=self.config['PATENTSVIEW_DATABASES']['RAW_DB'])
        # self.database_type = 'patent'
        # self.version = end_date.strftime('%Y%m%d')


class TextUploadTest(DatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)
        brf_key = "brf_sum_text_{year}".format(year=end_date.year)
        clm_key = "claims_{year}".format(year=end_date.year)
        ddr_key = "draw_desc_text_{year}".format(year=end_date.year)
        ddt_key = "detail_desc_text_{year}".format(year=end_date.year)
        keep_tables = []
        for i in self.table_config.keys():
            if i in [brf_key, clm_key, ddr_key, ddt_key]:
                keep_tables.append(i)
        self.table_config = self.with_keys(self.table_config, keep_tables)
        print(f"The following list of tables are run for {self.__class__.__name__}:")
        print(self.table_config.keys())

    def with_keys(self, d, keys):
        return {x: d[x] for x in d if x in keys}

    def runTests(self):
        skiplist = []
        for table in self.table_config:
            if table in skiplist:
                continue
            print("Beginning Test for {table_name} in {db}".format(table_name=table,
                                                                   db=self.config["PATENTSVIEW_DATABASES"][
                                                                       self.database_section]))
            self.load_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.load_nulls(table, self.table_config[table])

            for field in self.table_config[table]["fields"]:
                print("\tBeginning tests for {field} in {table_name}".format(field=field, table_name=table))
                if "date_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["date_field"]:
                    self.test_zero_dates(table, field)
                if self.table_config[table]["fields"][field]['category']:
                    self.load_category_counts(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
                    self.load_text_length(table, field)
                self.test_null_byte(table, field)
            print(f"Finished With Table: {table}")

if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })
    # tmt = TextMergeTest(config)
    # tmt.runTests()
    tut = TextUploadTest(config)
    tut.runTests()