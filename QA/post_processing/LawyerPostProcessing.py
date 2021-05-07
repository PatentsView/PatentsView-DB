import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config


class LawyerPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'RAW_DB', datetime.date(year=1976, month=1, day=1), end_date)
        table_keys = ["rawlawyer", "lawyer", "patent_lawyer", "related_entities"]
        self.table_config = {key: value for key, value in self.table_config.items() if key in table_keys}

        self.entity_table = 'rawlawyer'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'lawyer_id'
        self.disambiguated_table = 'lawyer'
        self.disambiguated_data_fields = ['name_last', 'name_first', "organization", "country"]
        self.exclusion_list.extend(['lawyer'])


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
                    "execution_date": datetime.date(2020, 12, 31)
                                })
    print({section: dict(config[section]) for section in config.sections()})
    qc = LawyerPostProcessingQC(config)
    qc.runTests()
