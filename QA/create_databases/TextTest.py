import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester


class TextMergeTest(PatentDatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEXT_DATABASE', datetime.date(year=1976, month=1, day=1), end_date)
        self.table_config = {"brf_sum_text": {"uuid": {"null_allowed": False, "data_type": "varchar"},
                                              "patent_id": {"null_allowed": False, "data_type": "varchar"},
                                              "text": {"null_allowed": False, "data_type": "mediumtext"},
                                              "version_indicator": {"null_allowed": False, "data_type": "varchar"}},
                             "claim": {"uuid": {"null_allowed": False, "data_type": "varchar"},
                                       "exemplary": {"null_allowed": True, "data_type": "varchar"},
                                       "patent_id": {"null_allowed": False, "data_type": "varchar"},
                                       "version_indicator": {"null_allowed": False, "data_type": "varchar"},
                                       "text": {"null_allowed": False, "data_type": "mediumtext"},
                                       "dependent": {"null_allowed": True, "data_type": "varchar"},
                                       "sequence": {"null_allowed": True, "data_type": "int"}}}

        self.count_data = []
        self.floating_entities = []
        self.floating_patent = []

    def test_yearly_count(self):
        pass


class TextUploadTest(PatentDatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)
        self.table_config = {"temp_brf_sum_text": {"filename": {"null_allowed": False, "data_type": "varchar"},
                                                   "id": {"null_allowed": False, "data_type": "varchar"},
                                                   "patent_number": {"null_allowed": False, "data_type": "varchar"},
                                                   "text": {"null_allowed": False, "data_type": "mediumtext"}},
                             "temp_claim": {"id": {"null_allowed": False, "data_type": "varchar"},
                                            "dependent": {"null_allowed": True, "data_type": "varchar"},
                                            "patent_number": {"null_allowed": False, "data_type": "varchar"},
                                            "filename": {"null_allowed": False, "data_type": "varchar"},
                                            "num": {"null_allowed": False, "data_type": "varchar"},
                                            "text": {"null_allowed": False, "data_type": "mediumtext"},
                                            "sequence": {"null_allowed": False, "data_type": "int"}},
                             "temp_detail_desc_text": {"id": {"null_allowed": False, "data_type": "varchar"},
                                                       "patent_number": {"null_allowed": False, "data_type": "varchar"},
                                                       "text": {"null_allowed": False, "data_type": "mediumtext"},
                                                       "length": {"null_allowed": True, "data_type": "bigint"},
                                                       "filename": {"null_allowed": False, "data_type": "varchar"}}}

        self.count_data = []
        self.floating_entities = []
        self.floating_patent = []

    def test_yearly_count(self):
        pass
