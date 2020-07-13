import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester


class TextMergeTest(PatentDatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        brf_key = "brf_sum_text_{year}".format(year=end_date.year)
        clm_key = "claim_{year}".format(year=end_date.year)
        ddr_key = "detail_desc_text_{year}".format(year=end_date.year)
        ddt_key = "draw_desc_text_{year}".format(year=end_date.year)

        super().__init__(config, 'TEXT_DATABASE', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {
            brf_key: {'fields': {"uuid": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                                 "version_indicator": {"null_allowed": False,
                                                       "data_type": "varchar", "category": False}}},
            clm_key: {'fields': {"uuid": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "exemplary": {"null_allowed": False, "data_type": "int", "category": False},
                                 "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "version_indicator": {"null_allowed": False, "data_type": "varchar",
                                                       "category": False},
                                 "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                                 "dependent": {"null_allowed": True, "data_type": "varchar", "category": False},
                                 "sequence": {"null_allowed": True, "data_type": "int", "category": False}}},
            ddr_key: {
                'fields': {"uuid": {"null_allowed": detail_desc_text_2020False, "data_type": "varchar", "category": False},
                           "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False},
                           "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                           "sequence": {"null_allowed": False, "data_type": "int", "category": False},
                           "version_indicator": {"null_allowed": True, "data_type": "varchar", "category": True}}},
            ddt_key: {'fields': {"uuid": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False},
                                 "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                                 "length": {"null_allowed": False, "data_type": "int", "category": False},
                                 "version_indicator": {"null_allowed": True, "data_type": "varchar",
                                                       "category": True}}}}

        self.patent_db_prefix = "`{db}`".format(db=self.config['DATABASE']['NEW_DB'])

    def test_yearly_count(self, table_name, strict=False):
        super().test_yearly_count(table_name, strict)


class TextUploadTest(PatentDatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)
        self.table_config = {
            "temp_brf_sum_text": {
                'fields': {"filename": {"null_allowed": False, "data_type": "varchar", "category": False},
                           "id": {"null_allowed": False, "data_type": "varchar", "category": False},
                           "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False}},
                "text": {"null_allowed": False, "data_type": "mediumtext", "category": False}},
            "temp_claim": {'fields': {"id": {"null_allowed": False, "data_type": "varchar", "category": False},
                                      "dependent": {"null_allowed": True, "data_type": "varchar", "category": False},
                                      "patent_id": {"null_allowed": False, "data_type": "varchar",
                                                    "category": False},
                                      "filename": {"null_allowed": False, "data_type": "varchar", "category": False},
                                      "num": {"null_allowed": False, "data_type": "varchar", "category": False},
                                      "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                                      "sequence": {"null_allowed": False, "data_type": "int", "category": False}}},
            "temp_detail_desc_text": {
                'fields': {"id": {"null_allowed": False, "data_type": "varchar", "category": False},
                           "patent_id": {"null_allowed": False, "data_type": "varchar", "category": False},
                           "text": {"null_allowed": False, "data_type": "mediumtext", "category": False},
                           "length": {"null_allowed": True, "data_type": "bigint", "category": False},
                           "filename": {"null_allowed": False, "data_type": "varchar", "category": False}}}}
