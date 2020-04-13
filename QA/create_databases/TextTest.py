from QA.PatentDatabaseTester import PatentDatabaseTester


class TextMergeTest(PatentDatabaseTester):
    def __init__(self, config):
        super().__init__(config, 'TEXT_DATABASE')
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


class TextUploadTest(PatentDatabaseTester):
    def __init__(self, config):
        super().__init__(config, 'TEMP_UPLOAD_DB')
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
