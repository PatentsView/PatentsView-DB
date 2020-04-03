import os
import pandas as pd
import re
import logging
logger = logging.getLogger("airflow.task")

class ParserTest:
    def __init__(self, update_config):
        import glob
        self.expected_entities = ['application', 'botanic', 'brf_sum_text', 'claim',
                                  'detail_desc_text', 'draw_desc_text', 'figures', 'foreign_priority',
                                  'foreigncitation', 'government_interest', 'ipcr', 'mainclass',
                                  'non_inventor_applicant', 'otherreference', 'patent', 'pct_data', 'rawassignee',
                                  'rawinventor', 'rawlawyer', 'rawlocation', 'rel_app_text', 'subclass',
                                  'us_term_of_grant', 'usapplicationcitation', 'uspatentcitation', 'uspc', 'usreldoc']

        input_folder = '{working_folder}/raw_data'.format(working_folder=update_config['FOLDERS'][
            'WORKING_FOLDER'])
        xml_glob = "{folder}/*.xml".format(folder=input_folder)
        self.xml_files_count = 0
        self.ip_filenames = []
        for fname in glob.glob(xml_glob):
            self.ip_filenames.append(fname)
            self.xml_files_count += 1

    def runTests(self, update_config):
        self.test_all_entities(update_config)

    def test_all_entities(self, update_config):
        output_folder = '{working_folder}/parsed_data'.format(working_folder=update_config['FOLDERS']['WORKING_FOLDER'])
        for fname in self.ip_filenames:
            filename = os.path.basename(fname)
            name, ext = os.path.splitext(filename)
            tstamp_match = re.match('.*([0-9]{6}).*', name)
            if tstamp_match is None:
                raise AssertionError("Non patent file found in input folder")
            folder_name = tstamp_match.group(1)
            for entity in self.expected_entities:
                entity_file = "{top_folder}/{tfolder}/{entity}.csv".format(top_folder=output_folder,
                                                                           tfolder=folder_name, entity=entity)
                self.test_file(entity_file, folder_name, entity)

    def test_file(self, file_path, folder_name, entity):
        from pathlib import Path
        import pandas as pd
        my_file = Path(file_path)
        logger.info(file_path)
        assert my_file.exists()
        try:
            df = pd.read_csv(file_path, sep="\t")
        except:
            raise AssertionError("Unable to read CSV file")

        assert df.shape[0] > 0

    def get_file_shapes(self, update_config):
        shapes = []
        output_folder = '{working_folder}/parsed_data'.format(working_folder=update_config['FOLDERS']['WORKING_FOLDER'])
        logger.info(self.ip_filenames)
        for fname in self.ip_filenames:
            filename = os.path.basename(fname)
            name, ext = os.path.splitext(filename)
            tstamp_match = re.match('.*([0-9]{6}).*', name)
            if tstamp_match is None:
                raise AssertionError("Non patent file found in input folder")
            folder_name = tstamp_match.group(1)
            counts = {}
            for entity in self.expected_entities:
                entity_file = "{top_folder}/{tfolder}/{entity}.tsv".format(top_folder=output_folder,
                                                                           tfolder=folder_name, entity=entity)
                df = pd.read_csv(entity_file, sep='\t')
                counts[entity] = df.shape[0]
            shapes.append(counts)
        return pd.DataFrame(shapes)
