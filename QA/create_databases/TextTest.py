import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config
from lib import utilities

class TextMergeTest(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        print(end_date)
        super().__init__(config, config['PATENTSVIEW_DATABASES']["TEXT_DB"], datetime.date(year=1976, month=1, day=1), end_date)
        brf_key = "brf_sum_text_{year}".format(year=end_date.year)
        if config['PATENTSVIEW_DATABASES']["TEXT_DB"] == 'patent_text':
            clm_key = "claims_{year}".format(year=end_date.year)
        else:
            clm_key = "claim_{year}".format(year=end_date.year)
        ddr_key = "draw_desc_text_{year}".format(year=end_date.year)
        ddt_key = "detail_desc_text_{year}".format(year=end_date.year)
        keep_tables = []
        for i in self.table_config.keys():
            print(i)
            if i in [brf_key, clm_key, ddr_key, ddt_key]:
                keep_tables.append(i)
        self.table_config = utilities.with_keys(self.table_config, keep_tables)
        print(f"The following list of tables are run for {self.__class__.__name__}:")
        print(self.table_config.keys())


class TextUploadTest(DatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"], start_date, end_date)
        brf_key = "brf_sum_text_{year}".format(year=end_date.year)
        if config['PATENTSVIEW_DATABASES']["TEXT_DB"] == 'patent_text':
            clm_key = "claims_{year}".format(year=end_date.year)
        else:
            clm_key = "claim_{year}".format(year=end_date.year)
        ddr_key = "draw_desc_text_{year}".format(year=end_date.year)
        ddt_key = "detail_desc_text_{year}".format(year=end_date.year)
        keep_tables = []
        for i in self.table_config.keys():
            print(i)
            if i in [brf_key, clm_key, ddr_key, ddt_key]:
                keep_tables.append(i)
        self.table_config = utilities.with_keys(self.table_config, keep_tables)
        print(f"The following list of tables are run for {self.__class__.__name__}:")
        print(self.table_config.keys())


if __name__ == '__main__':
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 5)
    # })
    config = get_current_config('pgpubs', **{
        "execution_date": datetime.date(2021, 12, 2)
    })
    # config = get_current_config('pgpubs', **kwargs)
    tmt = TextMergeTest(config)
    tmt.runTests()
    # tut = TextUploadTest(config)
    # tut.runTests()