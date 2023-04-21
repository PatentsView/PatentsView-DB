import datetime
import logging
import os
import re

import pandas as pd

from lib.configuration import get_current_config, get_parsed_tables_dict
from lib.utilities import rds_free_space

logger = logging.getLogger("airflow.task")


class ParserTest:
    def __init__(self, update_config):
        self.update_config = update_config
        import glob
        tables_dict = get_parsed_tables_dict(update_config)
        self.expected_entities = tables_dict.keys()

        input_folder = '{working_folder}/raw_data'.format(working_folder=update_config['FOLDERS'][
            'WORKING_FOLDER'])
        xml_glob = "{folder}/*.xml".format(folder=input_folder)
        self.xml_files_count = 0
        self.ip_filenames = []
        for fname in glob.glob(xml_glob):
            self.ip_filenames.append(fname)
            self.xml_files_count += 1

    def runTests(self):
        self.test_aws_rds_space()
        self.test_all_entities()

    def test_all_entities(self):
        output_folder = '{working_folder}/parsed_data'.format(
                working_folder=self.update_config['FOLDERS']['WORKING_FOLDER'])
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
        print(file_path)
        assert my_file.exists()
        try:
            df = pd.read_csv(file_path, sep="\t")
        except:
            raise AssertionError("Unable to read CSV file")

    def get_file_shapes(self):
        shapes = {
                'timestamp': [],
                'entity':    [],
                'count':     []
                }
        output_folder = '{working_folder}/parsed_data'.format(
                working_folder=self.update_config['FOLDERS']['WORKING_FOLDER'])
        logger.info(self.ip_filenames)
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
                df = pd.read_csv(entity_file, sep='\t')
                shapes['timestamp'].append(folder_name)
                shapes['entity'].append(entity)
                shapes['count'].append(df.shape[0])

        return pd.DataFrame(shapes)

    def test_aws_rds_space(self):
        free_space_in_bytes = rds_free_space(
                self.update_config,
                self.update_config['PATENTSVIEW_DATABASES']['identifier'])
        if free_space_in_bytes / (1024 * 1024 * 1024) < 30:
            raise Exception("Free space less than 30G in RDS, stopping the process")


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })

    test_obj = ParserTest(config)

    test_obj.runTests()
