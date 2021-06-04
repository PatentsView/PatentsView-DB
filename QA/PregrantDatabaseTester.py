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



if __name__ == '__main__':
    #config = get_config()
    config = get_current_config('pgpubs', **{
        "execution_date": datetime.date(2020, 12, 29)
    })

    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"

    pt = PregrantDatabaseTester(config,"PGPUBS_DATABASE", datetime.date(2020, 1, 1), datetime.date(2020, 12, 31))

    pt.runTests()