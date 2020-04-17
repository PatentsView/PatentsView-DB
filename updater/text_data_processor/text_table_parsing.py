import sys
import os

from QA.create_databases.TextTest import TextUploadTest
from lib.configuration import get_config


def begin_text_parsing(config):
    project_home = os.environ['PACKAGE_HOME']
    sys.path.append(project_home + '/updater/text_parser/')
    from updater.text_parser.app_parser import queue_parsers

    queue_parsers(config)


def post_text_parsing(config):
    tpt = TextUploadTest(config)
    tpt.runTests()


if __name__ == '__main__':
    begin_text_parsing(config=get_config())
