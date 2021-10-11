import datetime
import json
from abc import ABC

import pandas as pd
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_connection_string, get_current_config


class PatentsViewDatabaseTester(PatentDatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'REPORTING_DATABASE', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = json.load(open("../../Automated QA/reporting_config.json"))
        self.config = config
        self.project_home = ''
        self.patent_exclusion_list = ["cpc_subsection", "uspc_current_mainclass_application_year",
                                      "inventor_cpc_subsection", "inventor", "inventor_cpc_group", "inventor_year",
                                      "inventor_uspc_mainclass", "cpc_current_subsection_patent_year",
                                      "location_nber_subcategory", "wipo_field", "location_year",
                                      "nber_subcategory_patent_year", "uspatentcitation", "location", "cpc_subgroup",
                                      "inventor_nber_subcategory", "uspc_mainclass", "cpc_current_group_patent_year",
                                      "nber_category", "assignee_nber_subcategory", "location_cpc_group",
                                      "assignee_uspc_mainclass", "examiner", "location_cpc_subsection",
                                      "usapplicationcitation", "location_uspc_mainclass", "cpc_group",
                                      "cpc_current_group_application_year", "nber_subcategory",
                                      "government_organization", "uspc_subclass", "assignee_year",
                                      "uspc_current_mainclass_patent_year", "assignee_cpc_group", "lawyer",
                                      "assignee_inventor", "inventor_coinventor", "location_assignee", "assignee",
                                      "location_inventor", "assignee_cpc_subsection"]

    def load_category_counts(self, table, field):
        pass

    def test_related_floating_entities(self, table_name, table_config):
        pass

    def test_floating_entities(self, table_name):
        pass

    def load_floating_patent_count(self, table, table_config):
        pass

    def test_yearly_count(self, table_name, strict=True, patent_id_field=None):
        super(PatentsViewDatabaseTester, self).test_yearly_count(table_name, strict, patent_id_field='patent_id')

    def load_prefix_counts(self, table_name, patent_id_field=None):
        super(PatentsViewDatabaseTester, self).load_prefix_counts(table_name, patent_id_field='patent_id')


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2021, 6, 22)
    })

    mc = PatentsViewDatabaseTester(config)

    mc.runTests()
