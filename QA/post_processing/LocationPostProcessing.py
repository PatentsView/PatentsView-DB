import datetime
import time

from QA.post_processing.DisambiguationTester import DisambiguationTester
from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_current_config


class LocationPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def init_qa_dict_loc(self):
        self.qa_data = {
            "DataMonitor_regioncount": [],
        }

    def save_qa_data(self):
        super(LocationPostProcessingQC, self).save_qa_data()

    def records_by_region(self, config, table_name):
        e_date = config["DATES"]["END_DATE"]
        s_date = config["DATES"]["START_DATE"]
        region_types = ['region','sub-region']
        for region_type in region_types:
            query = f"""
    select `{region_type}`, count(*)
    from patent.{table_name}  a
        inner join geo_data.country_codes b on a.country=b.`alpha-2`
    where a.version_indicator >= '{s_date}' and  a.version_indicator <= '{e_date}'
    group by 1
            """
            print(query)
            if not self.connection.open:
                self.connection.connect()
            with self.connection.cursor() as top_cursor:
                top_cursor.execute(query)
                for top_n_data_row in top_cursor:
                    self.qa_data['DataMonitor_regioncount'].append(
                        {
                            "database_type": self.database_type,
                            "update_version": self.version,
                            'table_name': table_name,
                            "region_type": region_type,
                            'region': top_n_data_row[0],
                            'count': top_n_data_row[1],
                            'quarter': self.quarter
                        })


    def runTests(self, config):
        super(LocationPostProcessingQC, self).runTests()
        super(DisambiguationTester, self).runDisambiguationTests()
        self.init_qa_dict_loc()
        for table in self.table_config:
            print(table)
            if table in ['rawlocation', 'location']:
                self.records_by_region(config, table,)
                self.save_qa_data()
                self.init_qa_dict_loc()


if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule='quarterly', **{
            "execution_date": datetime.date(2023, 1, 1)
            })
    lc = LocationPostProcessingQC(config)
    lc.runTests(config)