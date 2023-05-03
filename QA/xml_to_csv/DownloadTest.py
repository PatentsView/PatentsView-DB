import re

class DownloadTest:
    def __init__(self, update_config):
        output_folder = '{working_folder}/raw_data'.format(working_folder=update_config['FOLDERS'][
                                                                        'WORKING_FOLDER'])
        import glob
        xml_glob = "{folder}/*.xml".format(folder=output_folder)
        self.xml_files_count = 0
        self.filenames = []
        self.dates = set()
        for fname in glob.glob(xml_glob):
            self.filenames.append(fname)
            self.xml_files_count += 1
            filedate = re.match(r".*([0-9]{6})(_r\d)?", fname).group(1)
            self.dates.add(filedate)

    def runTest(self, config):
        self.test_all_tuesdays(config)
        self.test_file_sizes()

    def test_all_tuesdays(self, config):
        from lib.utilities import weekday_count
        import datetime
        start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        week_day_counts = weekday_count(start_date, end_date)
        assert week_day_counts['Tuesday'] == self.get_date_count()

    def test_file_sizes(self):
        import os
        for filename in self.filenames:
            assert os.stat(filename).st_size > 0

    def get_file_count(self):
        return self.xml_files_count
    
    def get_date_count(self):
        return(len(self.dates))

    def get_file_size(self):
        import os
        sz = 0
        for filename in self.filenames:
            sz += os.stat(filename).st_size
        return sz
