from abc import ABC

import pandas as pd
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from QA.PatentsViewDatabaseTester import PatentsViewDatabaseTester
from lib.configuration import get_connection_string
from lib.configuration import get_current_config
import datetime
import json


class PregrantDatabaseTester(PatentsViewDatabaseTester):
    def __init__(self, config, database_section, start_date, end_date):
        """
        Do not instantiate. Overriden class constructor
        :param config: Configparser object containing update parameters
        :param database_section: Section in config that indicates database to use. RAW_DB or TEMP_UPLOAD_DB
        :param start_date: Database Update start date
        :param end_date: Database Update end date
        """
        # Tables that do not directly link to publication table
        super().__init__(config, database_section, start_date, end_date)
        self.exclusion_list = ['rawlocation']
        self.central_entity = 'publication'
        self.table_config = json.load(open("{}".format(self.config["FOLDERS"]["resources_folder"] + "/" + self.config["FILES"]["table_config_pgpubs"]),))

    def temp_save_qa_data(self, table_dict):
        table_dict[self.qa_data['DataMonitor_count'][0]['table_name']] = self.qa_data
        return table_dict

    def runTests(self):
        skiplist = []
        qa_dict = {}
        for table in self.table_config:
            if table in skiplist:
                continue
            print("Beginning Test for {table_name} in {db}".format(table_name=table,
                                                                   db=self.config["PATENTSVIEW_DATABASES"][
                                                                       self.database_section]))
            self.test_floating_entities(table)
            self.test_yearly_count(table, strict=False)
            self.test_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.test_nulls(table, self.table_config[table])
            self.test_related_floating_entities(table_name=table, table_config=self.table_config[table])
            self.load_floating_patent_count(table, self.table_config[table])
            self.load_prefix_counts(table)
            for field in self.table_config[table]["fields"]:
                print("\tBeginning tests for {field} in {table_name}".format(field=field, table_name=table))
                if "date_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["date_field"]:
                    self.assert_zero_dates(table, field)
                if self.table_config[table]["fields"][field]['category']:
                    self.load_category_counts(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
                    self.test_text_length(table, field)
                if "location_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["location_field"]:
                    self.test_counts_by_location(table, field)
                self.test_null_byte(table, field)
            self.save_qa_data()
            self.init_qa_dict()


if __name__ == '__main__':
    #config = get_config()
    config = get_current_config('pgpubs', **{
        "execution_date": datetime.date(2020, 12, 29)
    })

    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"

    pt = PregrantDatabaseTester(config,"PGPUBS_DATABASE", datetime.date(2020, 1, 1), datetime.date(2020, 12, 31))

    pt.runTests()