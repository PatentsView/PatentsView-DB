
import logging

import os
import datetime

from QA.xml_to_csv.XMLTest import XMLTest
from lib.configuration import get_today_dict


def clean_single_file(raw_xml_file, new_xml_file):
    logger = logging.getLogger("airflow.task")
    '''
    This is a little hacky but solves the files-are-not-provided-as-valid-xml problem
    '''
    with open(raw_xml_file, 'r+') as f:

        with open(new_xml_file, 'w', encoding='utf8') as new_f:
            new_f.write('<?xml version="1.0" encoding="UTF-8"?>' + "\n")
            new_f.write('<root>' + '\n')
            for line in f.readlines():
                if not line.lstrip().startswith('<?xml') and not line.lstrip().startswith('<!DOCTYPE'):
                    new_f.write(line)
            new_f.write('</root>')

    logger.info("cleaning is done for current raw xml file: {fname}".format(fname=new_xml_file))


def check_schema(patent_xml):
    '''
    Used to determine if the core data containers have changed (as they sometimes do)
    Uses the patent xml data to generate a list of the highest level fields
    and the fields under 'us-bibliographic-data-grant' (the main category).
    :param patent_xml: the root object that represents the patent xml data
    :param expected_schema: the list of fields previously at those levels for comparison
    :returns null: Raises errors if the schema is unexpected
    '''
    logger = logging.getLogger("airflow.task")
    expected_high_level = ['abstract', 'claims', 'description', 'drawings', 'number',
                           'publication-reference', 'sequence-list-new-rules', 'table',
                           'us-bibliographic-data-grant', 'us-chemistry', 'us-claim-statement',
                           'us-math', 'us-sequence-list-doc']
    expected_main_fields = ['application-reference', 'assignees', 'classification-locarno', 'classification-national',
                            'classifications-cpc', 'classifications-ipcr', 'examiners', 'figures',
                            'hague-agreement-data', 'invention-title', 'number-of-claims',
                            'pct-or-regional-filing-data', 'pct-or-regional-publishing-data',
                            'priority-claims', 'publication-reference', 'rule-47-flag',
                            'us-application-series-code', 'us-botanic', 'us-exemplary-claim',
                            'us-field-of-classification-search', 'us-parties', 'us-references-cited',
                            'us-related-documents', 'us-term-of-grant']
    high_level = []
    main_fields = []
    without_us_bibliographic = 0
    for patent in patent_xml:
        high_level += [field.tag for field in patent]
        if patent.find('us-bibliographic-data-grant') is not None:
            main_fields += [field.tag for field in patent.find('us-bibliographic-data-grant')]
        else:
            without_us_bibliographic += 1
    high_level = sorted(list(set(high_level)))
    main_fields = sorted(list(set(main_fields)))

    if not high_level == expected_high_level:
        logger.error(high_level)
        raise Exception("The high level fields have changed ...check that it is not the ones we use.")
    if not main_fields == expected_main_fields:
        logger.error(main_fields)
        raise Exception(
                "The main fields in the us-bibliographic-grant-data have changed ...check that it is not the ones we use.")
    if without_us_bibliographic > 200:
        raise Exception("There are more patents missing the us-bibliographic-grant-data field than ussual ")


def begin_xml_cleaning(config):
    logger = logging.getLogger("airflow.task")
    infolder = '{}/raw_data'.format(config['FOLDERS']['WORKING_FOLDER'])
    outfolder = '{}/clean_data'.format(config['FOLDERS']['WORKING_FOLDER'])

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    in_files = ['{0}/{1}'.format(infolder, f) for f in os.listdir(infolder) if
                os.path.isfile(os.path.join(infolder, f))]
    out_files = ['{}/{}_clean.xml'.format(outfolder, os.path.splitext(os.path.split(raw_file)[-1])[0]) for raw_file in in_files]
    files = zip(in_files, out_files)

    for f in files:
        clean_single_file(f[0], f[1])

    logger.info("finished")
    # wait until all jobs finish processing to move on

    # delete the raw files so we don't run out of space
    # os.system('rm '+infolder+'/*')

    # After cleaning the files check that there haven't been schema changes
    # This is commented out because still needs work
    # check the schema has not changed
    # clean_example =xml_helpers.get_xml(out_files[-1]) #get the most recent data an an example
    # check_schema(clean_example, [expected_high_level, expected_main_fields])


def post_xml(update_config):
    qc_step = XMLTest(update_config)
    qc_step.runTest(update_config)


def preprocess_xml(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    begin_xml_cleaning(config)
    try:
        post_xml(config)
    except AssertionError as e:
        raise AssertionError("Error when cleaning XML files")


if __name__ == '__main__':
    from lib.configuration import get_current_config

    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)

            })

    config['DATES'] = {
            "START_DATE": '20201006',
            "END_DATE":   '20201229'
            }
    begin_xml_cleaning(config)