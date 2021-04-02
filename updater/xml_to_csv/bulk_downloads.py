import datetime
import logging
import os

from QA.xml_to_csv.DownloadTest import DownloadTest
from lib.utilities import download_xml_files

logger = logging.getLogger("Bulk Downloads")



def bulk_download(**kwargs):
    from lib.configuration import get_current_config
    update_config = get_current_config('granted_patent', **kwargs)
    download_xml_files(update_config,'granted_patent')
    try:
        post_download(update_config)
    except AssertionError as e:
        raise AssertionError("Error when validating downloaded XMLs")


def post_download(update_config):
    qc_step = DownloadTest(update_config)
    qc_step.runTest(update_config)


if __name__ == '__main__':
    from lib.configuration import get_current_config

    update_config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 7, 7)
            })
    update_config['DATES'] = {
            "START_DATE": '20201006',
            "END_DATE":   '20201229'
            }
    download_xml_files(update_config, 'granted_patent')

