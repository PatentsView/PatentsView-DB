import pandas as pd


class CPCClassParserTest:
    def __init__(self, config):
        self.config = config

    def assert_csv_size(self, filename):
        filedata = pd.read_csv(filename)
        if filedata.shape[0] < 1:
            raise AssertionError("{Fname} does not contain any data".format(Fname=filename))

    def test_parse_output(self):
        output_directory = '{}/{}'.format(self.config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')
        files_to_check = ['/cpc_subsection.csv', '/cpc_group.csv', '/cpc_subgroup.csv']
        for file in files_to_check:
            self.assert_csv_size(output_directory + file)

    def runTests(self):
        self.test_parse_output()
