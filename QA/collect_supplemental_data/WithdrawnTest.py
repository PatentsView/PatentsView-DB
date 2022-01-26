import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine

from lib import xml_helpers
from lib.configuration import get_config, get_connection_string, get_current_config


class WithdrawnTest:
    def __init__(self, config):
        self.config = config
        self.qa_connection_string = get_connection_string(self.config, 'QA_DATABASE', connection='QA_DATABASE_SETUP')
        self.connection = pymysql.connect(host=self.config['DATABASE_SETUP']['HOST'],
                                          user=self.config['DATABASE_SETUP']['USERNAME'],
                                          password=self.config['DATABASE_SETUP']['PASSWORD'],
                                          db=self.config['PATENTSVIEW_DATABASES']['RAW_DB'],
                                          charset='utf8mb4', cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        self.qa_data = {
                "DataMonitor_patentwithdrawncount": []
                }
        self.database_type = 'patent'
        self.version = config['DATES']['END_DATE']

    def runTests(self):
        self.test_withdrawn_uploaded()
        self.save_qa_data()

    def test_withdrawn_uploaded(self):
        file_withdrawn_patents = []
        withdrawn_folder = '{}/withdrawn'.format(self.config['FOLDERS']['WORKING_FOLDER'])
        withdrawn_file = '{}/withdrawn.txt'.format(withdrawn_folder)
        with open(withdrawn_file, 'r') as f:
            for line in f.readlines():
                if len(line.strip())>0:
                    file_withdrawn_patents.append(xml_helpers.process_patent_numbers(line.strip('\n')))
        withdrawn_patent_query = "SELECT id from patent where withdrawn=1"
        if not self.connection.open:
            self.connection.connect()
        with self.connection.cursor() as withdrawn_cursor:
            withdrawn_cursor.execute(withdrawn_patent_query)
            withdrawn_patents = [x[0] for x in withdrawn_cursor.fetchall()]
            missing_withdrawn = [True if x not in file_withdrawn_patents else False for x in withdrawn_patents]
            if any(missing_withdrawn):
                raise AssertionError(
                        "Some of the patents marked withdrawn in {db} are not in the withdrawn file, count: {cnt}".format(
                                cnt=sum(missing_withdrawn), db=self.config["PATENTSVIEW_DATABASES"]["NEW_DB"]))

            self.qa_data['DataMonitor_patentwithdrawncount'].append(
                    {
                            "database_type":          self.database_type,
                            'update_version':         self.version,
                            'withdrawn_patent_count': len(withdrawn_patents)
                            })

    def save_qa_data(self):
        qa_engine = create_engine(self.qa_connection_string)
        for qa_table in self.qa_data:
            qa_table_data = self.qa_data[qa_table]
            table_frame = pd.DataFrame(qa_table_data)
            table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)


if __name__ == '__main__':
    # config = get_config()
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })
    qc = WithdrawnTest(config)
    qc.runTests()
