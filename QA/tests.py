class XMLTest:
    def __init__(self, update_config, project_home):
        output_folder = '{project_home}{timestamp}/raw_data'.format(project_home=project_home,
                                                                    timestamp=update_config['FOLDERS'][
                                                                        'WORKING_FOLDER'])
        import glob
        xml_glob = "{folder}/*.xml".format(folder=output_folder)
        self.xml_files_count = 0
        self.filenames = []
        for fname in glob.glob(xml_glob):
            self.filenames.append(fname)
            self.xml_files_count += 1

    def runTest(self, config):
        self.test_all_tuesdays(config)
        self.test_file_sizes()

    def test_all_tuesdays(self, config):
        from lib.utilities import weekday_count
        import datetime
        start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        week_day_counts = weekday_count(start_date, end_date)
        assert week_day_counts['Tuesday'] == self.get_file_count()

    def test_file_sizes(self):
        import os
        for filename in self.filenames:
            assert os.stat(filename).st_size > 0

    def get_file_count(self):
        return self.xml_files_count

    def get_file_size(self):
        import os
        sz = 0
        for filename in self.filenames:
            sz += os.stat(filename).st_size
        return sz
