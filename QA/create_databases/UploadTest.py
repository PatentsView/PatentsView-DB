import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_current_config


class UploadTest(PatentDatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)
        table_keys = ["application", "botanic", "figures", "foreigncitation", "foreign_priority",
                      "government_interest", "ipcr", "mainclass", "nber", "nber_category", "nber_subcategory",
                      "non_inventor_applicant", "otherreference", "patent", "pct_data", "rawassignee",
                      "rawinventor", "rawlawyer", "rawlocation", "rel_app_text", "subclass",
                      "usapplicationcitation", "uspatentcitation", "uspc", "uspc_current",
                      "usreldoc", "us_term_of_grant"]
        self.table_config = {key: value for key, value in self.table_config.items() if key in table_keys}


    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        count_query = "SELECT count(*) as null_abstract_count from {tbl} where abstract is null and type!='design' " \
                      "and type!='reissue'".format(
                tbl=table)
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                        "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. "
                        "Count: {count}".format(
                                database=self.config['PATENTSVIEW_DATABASES'][self.database_section], table=table,
                                count=count_value))

    def runTests(self):
        self.test_patent_abstract_null(table='patent')
        self.save_qa_data()
        self.init_qa_dict()
        super(UploadTest, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })

    test_obj = UploadTest(config)

    test_obj.runTests()
