class CPCParserTest:
    def __init__(self, config):
        self.config = config

    def test_cpc_parser_output(self):
        output_directory = '{}/{}'.format(self.config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')
        import pandas as pd

        granted_patent_data = pd.read_csv(output_directory + '/grants_classes.csv')
        if granted_patent_data.shape[0] < 1:
            raise ("Empty granted patent class data")

        app_data = pd.read_csv(output_directory + '/applications_classes.csv')
        if app_data.shape[0] < 1:
            raise ("Empty application class data")

    def runTests(self):
        self.test_cpc_parser_output()
