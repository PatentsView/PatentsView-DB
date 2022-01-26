import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class UploadTest(DatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)

    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        if self.central_entity == 'patent':
            count_query = f"""
            SELECT count(*) as null_abstract_count 
            from {self.central_entity} 
            where abstract is null and type!='design' and type!='reissue' 
                and id not in ['4820515', '4885173', '6095757', '6363330', '6571026', '6601394', '6602488', '6602501', '6602630', '6602899', '6603179', '6615064', '6744569', 'H002199', 'H002200', 'H002203', 'H002204', 'H002217', 'H002235']
            """
        elif self.central_entity == 'publication':
            count_query = f"""
            SELECT count(*) as null_abstract_count 
            from {self.central_entity} p
                left join application a on p.document_number=a.document_number 
            where invention_abstract is null """
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                        "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. "
                        "Count: {count}".format(
                                database=self.config['PATENTSVIEW_DATABASES'][self.database_section], table=table,
                                count=count_value))

    # def runTests(self):
    #     self.test_patent_abstract_null(table='patent')
    #     super(UploadTest, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })
    upt = UploadTest(config)
    upt.runTests()