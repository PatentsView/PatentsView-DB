import re
import os
import csv
from bs4 import BeautifulSoup as bs
import sys
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers


def parse_and_write_cpc_class(inputdir, outputdir):
    """
    Parse CPC Class information and write it to files. This is the function
    that drives parsing and writing of CPC Classification information.
    """

    # Parse CPC Information
    cpc_subsections, cpc_groups, cpc_subgroups = parse_cpc(inputdir, outputdir)


    # Write CPC Information
    general_helpers.write_csv(cpc_subsections, outputdir, 'cpc_subsection.csv')
    general_helpers.write_csv(cpc_groups, outputdir, 'cpc_group.csv')
    general_helpers.write_csv(cpc_subgroups, outputdir, 'cpc_subgroup.csv')


def parse_cpc(inputdir, outputdir):
    """
    Parse CPC information, including CPC subsections, groups, and subgroups.
    Return 2d arrays representing CPC subsection, group, and subgroup tables.
    """
    #make outputdir if does not exist
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    # Initialize arrays to store cpc information
    cpc_subsections, cpc_groups, cpc_subgroups = [], [], []

    # Loop over files in the input directory, parsing and appending CPC info
    input_filenames = os.listdir(inputdir)
    for filename in input_filenames:

        #print("Opening file: {}".format(filename))

        if re.search('-[A-Z].xml$', filename):
            # Parse subsections and groups from the ~9 CPC Section files
            input_file = open(os.path.join(inputdir, filename), 'rb').read()
            soup = bs(input_file, 'lxml', from_encoding='utf-8')

            cpc_subsections += parse_cpc_subsections(soup)
            cpc_groups += parse_cpc_groups(soup)
        elif re.search('-[A-Z]\d+[A-Z].xml$', filename):
            # Parse subgroups from the ~600+ CPC Subclass files
            input_file = open(os.path.join(inputdir, filename), 'rb').read()
            soup = bs(input_file, 'lxml', from_encoding='utf-8')
            cpc_subgroups += parse_cpc_subgroups(soup)
        else:
            pass
    # Print status messages
    print("Subsections found: {}".format(len(cpc_subsections)))
    print("Groups found: {}".format(len(cpc_groups)))
    print("Subgroups found: {}".format(len(cpc_subgroups)))

    return cpc_subsections, cpc_groups, cpc_subgroups


def parse_cpc_subsections(soup):
    """
    Parse CPC subsections from an xml file.

    A CPC subsection is represented as a symbol/description pair, such as:
        [
            "A01",
            "AGRICULTURE; FORESTRY; ANIMAL HUSBANDRY;
             HUNTING; TRAPPING; FISHING"
        ]
    """

    subsections = soup.findAll('classification-item', {'level': '4'})
    rows = []

    for subsection in subsections:

        # Classification Symbol
        symbol = subsection.find('classification-symbol').text

        # Classification Descriptions
        descriptions = parse_descriptions(subsection)

        # Append this row of information
        rows.append([symbol, descriptions])

    return rows


def parse_descriptions(classification_item):
    """
    Parse text fields related to a group/section/subsetion descriptions

    The best rule (so far) is to include all <text> fields within the
    object's <class-title>, but excluding those within the <reference> tags.
    """
    description_list = []

    for text_field in classification_item.find('class-title').findAll('text'):
        if text_field.find_parent('reference') is None:
            description_list.append(text_field.text)

    return '; '.join(description_list)


def parse_cpc_groups(soup):
    """
    Parse CPC subsections from an xml file.

    A CPC group is represented as a symbol/description pair, such as:
        [
            "A01C",
            "PLANTING; SOWING; FERTILISING"
        ]
    """
    groups = soup.findAll('classification-item', {'level': '5'})
    rows = []

    for group in groups:

        # Classification Symbol
        symbol = group.find('classification-symbol').text

        # Classification Descriptions
        descriptions = parse_descriptions(group)

        # Append this row of information
        rows.append([symbol, descriptions])         

    return rows


def parse_cpc_subgroups(soup):
    """
    Parse CPC subsections from an xml file.

    A CPC subgroup is represented as a symbol/description pair, such as:
        [
            "A01B1/02",
            "Hand tools -Spades; Shovels"
        ]

    Subgroups are nested, and each subgroup should contain all of its parents
    subgroup descriptions as well. To implement this, maintain a stack of
    descriptions that appends, swaps, or removes parts of the description
    depending on whether a subgroup is a child, sibling, or uncle of the
    last-parsed subgroup. For example,

    Source XML
    --------------------------------------
    A01B1/00 (level 7): "Hand tools"
        A01B1/02 (level 8): "Spades; Shovels"
            A01B1/04 (level 9): "with teeth"
        A01B1/06 (level 8): "Hoes; Hand cultivators"

    Output rows
    --------------------------------------
    ["A01B1/00", "Hand tools"]
    ["A01B1/02", "Hand tools -Spades; Shovels"]
    ["A01B1/04", "Hand tools -Spades; Shovels -with teeth"]
    ["A01B1/06", "Hand tools -Hoes; Hand cultivators"]
    """

    subgroups = soup.findAll('classification-item')
    rows = []

    # Maintain a stack of descriptions and keep track of the level of nesting
    full_descriptions_list = []
    current_depth = 0

    for subgroup in subgroups:

        # depth refers to how nested the subgroup is. Classification items
        # with "level" attribute 7 represent the first depth of nesting
        # of subgroups.
        depth = int(subgroup.attrs.get('level')) - 6

        # Subgroups begin at level 7; reset our subgroup information if
        # we reach something more broad than a subgroup
        if depth < 1:
            current_depth = 0
            full_descriptions_list = []
            continue

        # If the item is a nested subgroup, append the item description
        # Otherwise, remove items from the stack until we reach the
        # right depth of nesting to append the item description
        while depth <= current_depth:
            current_depth -= 1
            full_descriptions_list.pop()

        assert(depth == current_depth + 1)

        # Classification Symbol
        symbol = subgroup.find('classification-symbol').text

        # Classification Descriptions
        descriptions = parse_descriptions(subgroup)

        # Full Descriptions (including all parent descriptions)
        full_descriptions_list.append(descriptions)
        full_descriptions = '-'.join(full_descriptions_list)

        rows.append([symbol,full_descriptions])

        # Update the current depth
        current_depth = depth

    return rows



if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')

    location_of_cpc_files = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    output_directory = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')
    parse_and_write_cpc_class(location_of_cpc_files, output_directory)
