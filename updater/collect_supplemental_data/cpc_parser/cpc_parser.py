import csv
import os
import sys

from QA.collect_supplemental_data.cpc_parser.CPCParserTest import CPCParserTest
from lib.configuration import get_config
from lib.utilities import write_csv


def parse_and_write_cpc(inputdir, outputdir):
    """ Parse CPC Classifications """
    applications = []
    grants = []

    app_header = [['application_number', 'cpc_primary', 'cpc_additional']]
    writer = csv.writer(open(os.path.join(outputdir, 'applications_classes.csv'), 'w', encoding='utf-8'))
    writer.writerows(app_header)

    grant_header = [['patent_number', 'cpc_primary', 'cpc_additional']]
    writer = csv.writer(open(os.path.join(outputdir, 'grants_classes.csv'), 'w', encoding='utf-8'))
    writer.writerows(grant_header)

    for filename in os.listdir(inputdir):
        if (filename.startswith('US_PGPub_CPC_MCF_') and
                filename.endswith('.txt')):
            applications = parse_pgpub_file(os.path.join(inputdir, filename))
            writer = csv.writer(open(os.path.join(outputdir, 'applications_classes.csv'), 'a', encoding='utf-8'))
            writer.writerows(applications)

        elif (filename.startswith('US_Grant_CPC_MCF_') and
              filename.endswith('.txt')):
            grants = parse_grant_file(os.path.join(inputdir, filename))
            writer = csv.writer(open(os.path.join(outputdir, 'grants_classes.csv'), 'a', encoding='utf-8'))
            writer.writerows(grants)


def parse_pgpub_file(filepath):
    """ Extract CPC classification from ~35 million applications """
    with open(filepath) as f:
        input_rows = f.readlines()
        print("Parsing app file: {}; rows: {}".format(filepath, len(input_rows)))

    # Since applications are already sorted by app_number, we can check if the
    # current application has the same number as the last one seen.
    # Once we see a new application, save the classifications that have been
    # recorded and reset the lists of recorded classifications
    results = []

    # Initial values -- this will give us a first row with no data.
    last_application_seen = ''
    primary_classifications = []
    additional_classifications = []

    for row in input_rows:

        # Skip blank rows
        if row != '':
            app_number = row[10:14] + '/' + row[10:21]
            classification = strip_whitespace(row[21:36])

        # Save the classifications found to our results dataset
        if app_number != last_application_seen:
            results.append([last_application_seen,
                            "; ".join(primary_classifications),
                            "; ".join(additional_classifications)])

            # Start recording for a new application
            last_application_seen = app_number
            primary_classifications = []
            additional_classifications = []

        # There is a problematic line that is cut short; as a result, we don't
        # know whether it is primary or secondary; so, skip this classification
        if len(row) <= 45:
            continue

        # Primary classifications end with 'I';
        # All others are considered additional
        # Skip blank rows
        if row != '':
            if row[45] == 'I':
                primary_classifications.append(classification)
            else:
                additional_classifications.append(classification)

    # Return all except the first row, which had empty placeholder values
    return results[1:]


def strip_whitespace(s):
    """ Strip whitespace really fast:

        re.sub('\s+', '', s)           2.33 usec per loop
        ''.join(s.split())             0.47 usec per loop
        s.replace(' ','')              0.40 usec per loop

    """
    return ''.join(s.split())


def parse_grant_file(filepath):
    """ Extract CPC classification from ~43 million rows """
    with open(filepath) as f:
        input_rows = f.readlines()
        # input_rows = f.read().split('\r\n')
        print("Parsing grant file: {}; rows: {}".format(filepath, len(input_rows)))

    # Since patents are already sorted by patent_number, we can check if the
    # current patent has the same number as the last one seen.
    # Once we see a new patent, save the classifications that have been
    # recorded and reset the lists of recorded classifications
    results = []

    # Initial values -- this will give us a first row with no data.
    last_patent_seen = ''
    primary_classifications = []
    additional_classifications = []

    for row in input_rows:
        # print(row)
        # Skip blank rows
        if row != '':
            # patent_number = strip_whitespace(row[10:17]).zfill(7)
            patent_number = strip_whitespace(row[10:18]).zfill(7)
            # print(patent_number)
            classification = strip_whitespace(row[18:32])

        # Save the classifications found to our results dataset
        if patent_number != last_patent_seen:
            results.append([last_patent_seen,
                            "; ".join(primary_classifications),
                            "; ".join(additional_classifications)])

            # Start recording for a new patent
            last_patent_seen = patent_number
            primary_classifications = []
            additional_classifications = []

        # Primary classifications end with 'I';
        # All others are considered additional
        # Skip blank rows
        if row != '':
            if row[42] == 'I':
                primary_classifications.append(classification)
            else:
                additional_classifications.append(classification)

    # Return all except the first row, which had placeholder values
    return results[1:]


def start_cpc_parser(config):
    location_of_cpc_files = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    output_directory = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')
    parse_and_write_cpc(location_of_cpc_files, output_directory)


def post_cpc_parser(config):
    qc = CPCParserTest(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
#    start_cpc_parser(config)
    post_cpc_parser(config)
