from QA.PatentsViewDatabaseTester import PatentsViewDatabaseTester
from lib.configuration import get_current_config
import datetime
import json


class DatabaseTester(PatentsViewDatabaseTester):
    def __init__(self, config, database_section, start_date, end_date, exclusion_list, central_entity):
        """
        Do not instantiate. Overriden class constructor
        :param config: Configparser object containing update parameters
        :param database_section: Section in config that indicates database to use. RAW_DB or TEMP_UPLOAD_DB
        :param start_date: Database Update start date
        :param end_date: Database Update end date
        """
        super().__init__(config, database_section, start_date, end_date, exclusion_list, central_entity)
        # Tables that do not directly link to patent table
        self.exclusion_list = exclusion_list
        if database_section == "RAW_DB":
            self.exclusion_list = ['assignee',
                                   'cpc_group',
                                   'cpc_subgroup',
                                   'cpc_subsection',
                                   'government_organization',
                                   'inventor',
                                   'lawyer',
                                   'location',
                                   'location_assignee',
                                   'location_inventor',
                                   'location_nber_subcategory',
                                   'mainclass',
                                   'nber_category',
                                   'nber_subcategory',
                                   'rawlocation',
                                   'subclass',
                                   'usapplicationcitation',
                                   'uspatentcitation',
                                   'wipo_field']
            self.central_entity = 'patent'
            self.table_config = json.load(open("{}".format(
                self.config["FOLDERS"]["resources_folder"] + "/" + self.config["FILES"]["table_config_granted"]), ))
        elif database_section == "PGPUBS_DATABASE":
            # TABLES WITHOUT DOCUMENT_NUMBER ARE EXCLUDED FROM THE TABLE CONFIG
            self.exclusion_list = ['granted_patent_crosswalk_archive']
            self.central_entity = 'publication'
            self.table_config = json.load(open("{}".format(
                self.config["FOLDERS"]["resources_folder"] + "/" + self.config["FILES"]["table_config_pgpubs"]), ))
        else:
            raise Exception


if __name__ == '__main__':
    # config = get_config()
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2020, 12, 29)
    })
    print(config)
    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"
    # databases = ["RAW_DB", "PGPUBS_DATABASE"]
    databases = ["PGPUBS_DATABASE"]
    for d in databases:
        pt = DatabaseTester(config, d, datetime.date(2021, 12, 7), datetime.date(2022, 1, 8), exclusion_list=[],
                            central_entity="")
        pt.runTests()
