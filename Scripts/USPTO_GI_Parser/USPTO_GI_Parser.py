################################################################################
# Copyright (c) 2013, AMERICAN INSTITUTES FOR RESEARCH and UNITED STATES PATENT AND TRADEMARK OFFICE
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#    disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
#    following disclaimer in the documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
################################################################################
"""
USPTO_GI_Parser

This script was designed to extract Government Interests sections from United States Patent and Trademark Office
patent data files.  As of this writing, these files can be downloaded from:

  http://www.google.com/googlebooks/uspto-patents-grants-text.html

It is my understanding that this contract has been awarded to another company moving forward, so this location will
probably change in the not-too-distant future.

The original concept for this script was developed by Evgeny Klochikhin at American Institutes for Research.  This
newer version adds support for extracting data files from ZIP files, determines file type from evidence within the
file rather than using a strict date based approach, and provides copious logging.

It is important to note here that the approach used in this script precludes the needs to parse the entire file into a
database.  It is a rather crude method that treats the source data files as just text and extracts only what it needs.
Once files have been fully parsed into a database, this approach should probably be revisited in order to take
advantage of the already parsed data.

VERY IMPORTANT:  There are two sets of ZIPped data files available from Google for 2001.  The first is the tagged ASCII
format and the second is an SGML format.  Including both of these in the source data will cause 2001 to be double
counted.  As such, I have included a check that will skip the 2001 SGML files.  If you opt to remove the 2001 ASCII
files, you will need to make adjustments to this script as noted in the process_input_file function below.

To run, simply run this through your favorite Python interpreter... as long as it's 3.4.1 or newer.  There are some
3.4.1 specific commands/syntax used.

USAGE:

  Tweak INPUT_DIRECTORY below to the local location of your raw patent data files (zipped or otherwise) and run
  USPTO_GI_Parser.py through the Python interpreter.  In my case:

  \python34\python USPTO_GI_Parser.py

  By default, two subdirectories will be created under your current working directory (in Windows... not sure where
  they'll end up in other operating systems... sorry).  output will hold the results of the parse and a file to track
  which files have been processed.  temp should be empty unless the script crashes in which case it will hold the file
  it was working on at the time of the crash and, potentially, partial results from parsing that file.  The files in
  temp (or the entire temp directory) will need to be deleted before re-running.  The script should pick up where it
  left off.  No need to start over.

This has only been tested with Python 3.4.1 running under Windows 8.1 Pro.
"""

import os
import re
import csv
import sys
import html
import shutil
import zipfile
import datetime
import traceback


# Global "constants".  Really, you should only have to tweak INPUT_DIRECTORY to point to your data files and *POOF* it
# should magically work.  Note that we do not crawl subdirectories.  Only the top level of the INPUT_DIRECTORY
# will be examined.
SUPPORTED_PYTHON_VERSIONS = ["3.4.1"]  # Versions of Python against which this code has been tested.
CURRENT_PYTHON_VERSION = sys.version.split()[0]

CURRENT_WORKING_DIRECTORY = os.getcwd()
# restored previous line, which says input files are in "data" directory
# INPUT_DIRECTORY = "D:/Raw Data/USPTO/GrantedPatents"
INPUT_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, "data")  # Location of USPTO data files, zipped or otherwise.
OUTPUT_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, "output")  # Location for output files.
TEMP_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, "temp")  # Location for temporary working files.

OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIRECTORY, "government_interests.csv")  # Where the final results end up.
PROCESSED_FILE_LOG_PATH = os.path.join(OUTPUT_DIRECTORY, "processed_files.txt")  # List of successfully processed files.
                                                                                 # Allows us to continue after an error.
SUPPORTED_FILE_TYPES = [["HHHHHT", "parse_file_type_1"],
                        ["DTD ST.32 US PATENT GRANT V2.4", "parse_file_type_2"],
                        ["ST32-US-Grant-025xml.dtd", "parse_file_type_2"],
                        ["us-patent-grant-v40-2004-12-02.dtd", "parse_file_type_3"],
                        ["us-patent-grant-v41-2005-08-25.dtd", "parse_file_type_3"],  # Keywords found in data files
                        ["us-patent-grant-v42-2006-08-23.dtd", "parse_file_type_3"],  # that can be used to identify
                        ["us-patent-grant-v43-2012-12-04.dtd", "parse_file_type_3"],  # data file structure and the
                        ["us-patent-grant-v44-2013-05-16.dtd", "parse_file_type_3"]]  # functions that can parse them.


def write_log_entry(text):
    """Simplistic yet consistent form of logging information to the console"""
    print(str(datetime.datetime.now()) + ": " + text)


def log_constants():
    """May or may not help with debugging.  What do I know.  I like overkill."""
    write_log_entry("  SUPPORTED_PYTHON_VERSIONS=" + ", ".join(SUPPORTED_PYTHON_VERSIONS))
    write_log_entry("  CURRENT_PYTHON_VERSION=" + CURRENT_PYTHON_VERSION)
    write_log_entry("  CURRENT_WORKING_DIRECTORY=" + CURRENT_WORKING_DIRECTORY)
    write_log_entry("  INPUT_DIRECTORY=" + INPUT_DIRECTORY)
    write_log_entry("  OUTPUT_DIRECTORY=" + OUTPUT_DIRECTORY)
    write_log_entry("  TEMP_DIRECTORY=" + TEMP_DIRECTORY)
    write_log_entry("  OUTPUT_FILE_PATH=" + OUTPUT_FILE_PATH)
    write_log_entry("  PROCESSED_FILE_LOG_PATH=" + PROCESSED_FILE_LOG_PATH)
    write_log_entry("  SUPPORTED_FILE_TYPES:")
    for supported_file_type in SUPPORTED_FILE_TYPES:
        write_log_entry("    " + supported_file_type[0])


def get_input_file_names():
    """Returns a list of input files yet to be processed"""

    # First, grab the list of files that have already been processed.
    already_processed_file_names = []
    if os.path.isfile(PROCESSED_FILE_LOG_PATH):
        with open(PROCESSED_FILE_LOG_PATH, mode="r") as file:
            already_processed_file_names = file.read().splitlines()

    # Now check all the entries in the input_directory to see if a) they're actually a file (as opposed to a directory)
    # and b) are not in the list of files already processed.
    input_file_names = []
    entries = os.listdir(INPUT_DIRECTORY)
    for entry in entries:
        if os.path.isfile(os.path.join(INPUT_DIRECTORY, entry)):
            if entry in already_processed_file_names:
                write_log_entry("File '" + entry + "' has already been processed.  Skipping.")
            else:
                input_file_names.append(entry)

    return input_file_names


def record_processed_file(file_name):
    """Tacks the provided filename on the end of the 'already processed' file list"""
    with open(PROCESSED_FILE_LOG_PATH, mode="a", encoding="utf-8") as file:
        file.write(file_name + "\n")


def remove_file(file_path):
    """Remove file if it exists.  If remove fails, throw an exception."""
    if os.path.isfile(file_path):
        os.remove(file_path)
        if os.path.isfile(file_path):
            raise Exception("Unable to remove file '" + file_path + "'.  Please ensure that your account has proper "
                            "permissions to remove files in the directory indicated.")


def unzip_file(file_path):
    """The presumption here is that there is only one data file that we care about in the zip file.  Some zip files
    contain more than one data file, but they are duplicates.  Typically, when this happens, there is one SGML file and
    one XML file.  In these cases, we will discard the SGML file."""

    with zipfile.ZipFile(file_path, "r") as file:
        file_list = file.infolist()
        file_count = len(file_list)
        if file_count < 1:
            raise Exception("No data files found in archive '" + file_path + "'.")
        if file_count > 1:
            if file_count > 2:
                raise Exception("Found more than 2 data files in archive '" + file_path + "'.")
            if file_list[0].filename.lower().endswith(".xml") and file_list[1].filename.lower().endswith(".sgm"):
                extract_file = file_list[0]
            elif file_list[0].filename.lower().endswith(".sgm") and file_list[1].filename.lower().endswith(".xml"):
                extract_file = file_list[1]
            else:
                raise Exception("Found something other than 1 XML and 1 SGML file in archive '" + file_path + "'.")
        else:
            extract_file = file_list[0]
        extract_file_path = os.path.join(TEMP_DIRECTORY, os.path.basename(extract_file.filename))
        write_log_entry("  Extracting data file to '" + extract_file_path + "'.")
        with open(extract_file_path, mode="wb") as output_file:
            shutil.copyfileobj(file.open(extract_file, mode="r"), output_file)
    return extract_file_path


def ascertain_file_type(file_path):
    """We're going to scan through the first handful of lines looking for certain keywords.  What we find will
    determine how we process the file."""

    with open(file_path, mode="r") as file:
        counter = 0
        for line in file:
            lower_line = line.lower()
            for supported_file_type in SUPPORTED_FILE_TYPES:
                if supported_file_type[0].lower() in lower_line:
                    write_log_entry("  File type identified as '" + supported_file_type[0] + "'.")
                    write_log_entry("  Using '" + supported_file_type[1] + "' to process.")
                    return supported_file_type
            counter += 1
            if counter > 5:  # File time identification usually happens in the first two lines.
                break
    raise Exception("File type not recognized.")


def handle_continuations(process_file_contents, process_file_length, n):
    """Deals with line continuations."""
    text = process_file_contents[n][5:].strip()
    for i in range(n + 1, process_file_length):
        line = process_file_contents[i]
        if not line.startswith(" "):
            break
        text += " " + line[5:].strip()
    return text


def parse_file_type_1(process_file_path):
    """Parses from tagged ASCII files."""

    count = 0
    process_file_name = os.path.basename(process_file_path)
    temp_file_path = os.path.join(TEMP_DIRECTORY, process_file_name) + ".tmp"
    with open(process_file_path, mode="r") as input_file:
        process_file_contents = input_file.readlines()
    process_file_length = len(process_file_contents)
    with open(temp_file_path, mode="w", encoding="utf-8", newline="") as output_file:
        csv_output_file = csv.writer(output_file)
        patent_number = None
        application_number = None
        patent_date = None
        gi_header = ""
        gi_text = None
        for i in range(process_file_length):
            line = process_file_contents[i]
            if line.startswith("PATN"):
                patent_number = None
                application_number = None
                patent_date = None
                gi_header = ""
                gi_text = None
            elif line.startswith("WKU ") and not patent_number:
                # Remove checksum digit from patent number.
                patent_number = line[5:].strip()[:-1]
                # Patent numbers were forced to fit an 8 character pattern by the addition of a zero.  Remove it.
                patent_number = re.sub("^(\D*)0", r"\1", patent_number)
            elif line.startswith("APN ") and not application_number:
                # Remove checksum digit from application number.
                application_number = line[5:].strip()[:-1]
            elif line.startswith("ISD ") and not patent_date:
                patent_date = line[5:].strip()
            elif line.startswith("GOVT"):
                for n in range(i + 1, process_file_length):
                    line = process_file_contents[n]
                    # There are many variations on the paragraph tag; PAC, PAL, PA1, PAR, etc.  But we DON'T want PARN.
                    if line.startswith("PA") and line[3:4] == " ":
                        text = handle_continuations(process_file_contents, process_file_length, n)
                        if not gi_header and line.startswith("PAC "):
                            if text:
                                gi_header = text
                        else:
                            if not gi_text:
                                gi_text = text
                            elif text:
                                gi_text += " " + text
                    elif not line.startswith(" "):
                        break
                if not patent_number:
                    raise Exception("Patent number missing.")
                if not application_number:
                    raise Exception("Application number missing.")
                if not patent_date:
                    raise Exception("Issue date missing.")
                #if not gi_header:
                #    gi_header = "GOVERNMENT INTERESTS SECTION"
                csv_output_file.writerow([
                    patent_number,
                    datetime.datetime.strptime(patent_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    application_number,
                    gi_header,
                    gi_text,
                    process_file_name])
                count += 1

    write_log_entry("  " + str(count) + " government interests sections found.")
    return temp_file_path


def parse_file_type_2(process_file_path):
    """Older style SGML/XML files."""

    count = 0
    process_file_name = os.path.basename(process_file_path)
    temp_file_path = os.path.join(TEMP_DIRECTORY, process_file_name) + ".tmp"
    with open(process_file_path, mode="r") as input_file:
        with open(temp_file_path, mode="w", encoding="utf-8", newline="") as output_file:
            csv_output_file = csv.writer(output_file)
            patent_number = None
            application_number = None
            patent_date = None
            for line in input_file:
                if "<PATDOC" in line:
                    patent_number = None
                    application_number = None
                    patent_date = None
                elif "<B110>" in line:
                    patent_number = re.search("<PDAT>(.*?)</PDAT>", line).group(1)
                    # Patent numbers were forced to fit an 8 character pattern by the addition of a zero.  Remove it.
                    patent_number = re.sub("^(\D*)0", r"\1", patent_number)
                elif "<B210>" in line:
                    application_number = re.search("<PDAT>(.*?)</PDAT>", line).group(1)[2:]
                elif "<B140>" in line:
                    patent_date = re.search("<PDAT>(.*?)</PDAT>", line).group(1)
                elif "<GOVINT>" in line:
                    #gi_header = "GOVERNMENT INTERESTS SECTION"
                    gi_header = ""
                    gi_text = ""
                    for line in input_file:
                        if line.startswith("<H "):
                            gi_header_temp = re.search("<H .*?>(.*?)</H>", line).group(1)
                            if gi_header_temp:
                                gi_header = gi_header_temp
                        elif line.startswith("<PARA "):
                            if gi_text:
                                gi_text += " "
                            gi_text += re.search("<PARA.*?>(.*?)</PARA>", line).group(1)
                        elif "</GOVINT>" in line:
                            break
                    # Strip HTML tags from header and text.
                    gi_header = re.sub("<.*?>", "", gi_header)
                    gi_text = re.sub("<.*?>", "", gi_text)
                    if not patent_number:
                        raise Exception("Patent number missing.")
                    if not application_number:
                        raise Exception("Application number missing.")
                    if not patent_date:
                        raise Exception("Issue date missing.")
                    if not gi_text:
                        raise Exception("Government Interests text missing.")
                    #if not gi_header:
                    #    gi_header = "GOVERNMENT INTERESTS SECTION"
                    csv_output_file.writerow([
                        patent_number,
                        datetime.datetime.strptime(patent_date, "%Y%m%d").strftime("%Y-%m-%d"),
                        application_number,
                        html.unescape(gi_header),
                        html.unescape(gi_text),
                        process_file_name])
                    count += 1

    write_log_entry("  " + str(count) + " government interests sections found.")
    return temp_file_path


def parse_file_type_3(process_file_path):
    """Newer style XML files."""

    count = 0
    process_file_name = os.path.basename(process_file_path)
    temp_file_path = os.path.join(TEMP_DIRECTORY, process_file_name) + ".tmp"
    with open(process_file_path, mode="r", encoding="utf-8") as input_file:
        with open(temp_file_path, mode="w", encoding="utf-8", newline="") as output_file:
            csv_output_file = csv.writer(output_file)
            patent_number = None
            application_number = None
            patent_date = None
            for line in input_file:
                if "<us-patent-grant " in line:
                    patent_number = None
                    application_number = None
                    patent_date = None
                elif "<publication-reference>" in line:
                    for line in input_file:
                        if line.startswith("<doc-number>"):
                            patent_number = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                            # Patent numbers were forced to fit an 8 character pattern by the addition of a zero.  Remove it.
                            patent_number = re.sub("^(\D*)0", r"\1", patent_number)
                        elif line.startswith("<date>"):
                            patent_date = re.search("<date>(.*?)</date>", line).group(1)
                        elif "</publication-reference>" in line:
                            break
                elif "<application-reference" in line:
                    for line in input_file:
                        if line.startswith("<doc-number>"):
                            application_number = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                        elif "</application-reference>" in line:
                            break
                elif "<?GOVINT" in line and 'end="lead"' in line:
                    #gi_header = "GOVERNMENT INTERESTS SECTION"
                    gi_header = ""
                    gi_text = ""
                    for line in input_file:
                        if line.startswith("<heading "):
                            gi_header_temp = re.search("<heading.*?>(.*?)</heading>", line).group(1)
                            if gi_header_temp:
                                gi_header = gi_header_temp
                        elif line.startswith("<p "):
                            #gi_text += re.search("<p .*?>(.*?)</p>", line).group(1)
                            line = line.rstrip()
                            while True:
                                if gi_text:
                                    gi_text += " "
                                gi_text += line
                                if line.endswith("</p>"):
                                    break
                                line = input_file.readline().rstrip()
                        elif "<?GOVINT" in line and 'end="tail"' in line:
                            break
                    # Strip HTML tags from header and text.
                    gi_header = re.sub("<.*?>", "", gi_header)
                    gi_text = re.sub("<.*?>", "", gi_text)
                    if not patent_number:
                        raise Exception("Patent number missing.")
                    if not application_number:
                        raise Exception("Application number missing.")
                    if not patent_date:
                        raise Exception("Issue date missing.")
                    #if not gi_text:
                    #    raise Exception("Government Interests text missing.")
                    csv_output_file.writerow([
                        patent_number,
                        datetime.datetime.strptime(patent_date, "%Y%m%d").strftime("%Y-%m-%d"),
                        application_number,
                        html.unescape(gi_header),
                        html.unescape(gi_text),
                        process_file_name])
                    count += 1

    write_log_entry("  " + str(count) + " government interests sections found.")
    return temp_file_path


def append_file(source_file_path, destination_file_path):
    """Appends the temporary holding file to the final output file. Adds headers if necessary."""
    destination_file_exists = os.path.isfile(destination_file_path)
    with open(destination_file_path, mode="a", encoding="utf-8") as write_file:
        if not destination_file_exists:
            write_file.write("patent_number,date_issued,application_number,section_title,section_text,source_file\n")
        with open(source_file_path, mode="r", encoding="utf-8") as read_file:
            write_file.write(read_file.read())


def process_input_file(input_file_name):
    """Unzip the data file if necessary, figure out what kind of file we're dealing with, and call the appropriate
    parser"""

    write_log_entry("Processing started for '" + input_file_name + "'.")

    # Note that 2001 SGML files are duplicates of 2001 ASCII tagged files so we will skip them.  If you opt to remove
    # the ASCII tagged files, comment out or otherwise remove the check below.  The ASCII files will be named something
    # like 'pftaps20010102_wk01.zip' whereas the SGML files will be named something along the lines of 'pg010102.zip'.
    if input_file_name.startswith("pg01"):
        record_processed_file(input_file_name)
        write_log_entry("  This 2001 SGML file has equivalent ASCII tagged data file.  Skipping.")

    else:
        input_file_path = os.path.join(INPUT_DIRECTORY, input_file_name)
        if input_file_name.lower().endswith(".zip"):
            process_file_path = unzip_file(input_file_path)
        else:
            process_file_path = input_file_path

        file_type = ascertain_file_type(process_file_path)

        # Call the appropriate parser.
        temp_file_path = globals()[file_type[1]](process_file_path)

        append_file(temp_file_path, OUTPUT_FILE_PATH)
        record_processed_file(input_file_name)

        # Clean up.
        remove_file(temp_file_path)
        if process_file_path != input_file_path:
            remove_file(process_file_path)

    write_log_entry("Processing completed for '" + input_file_name + "'.")


def main():
    """The main function.  This is what drives everything."""

    # Verify that we're running under a supported version of Python.
    if CURRENT_PYTHON_VERSION in SUPPORTED_PYTHON_VERSIONS:

        # Make sure the input directory exists.
        if not os.path.isdir(INPUT_DIRECTORY):
            raise Exception("Input directory '" + INPUT_DIRECTORY + "' does not exist.")

        # The temp directory must be empty or must not exist in order to continue.  This is for safety reasons.  Don't
        # want to accidentally delete anything important.  Note that this script will clean up after itself, however,
        # if it crashes it will leave boogers in order to assist with debugging.
        if os.path.isdir(TEMP_DIRECTORY) and os.listdir(TEMP_DIRECTORY) != []:
            raise Exception("Temp directory '" + TEMP_DIRECTORY + "' is not empty.  "
                            "Please delete or empty before continuing (for safety reasons).")
        if not os.path.isdir(TEMP_DIRECTORY):
            write_log_entry("Creating temp directory '" + TEMP_DIRECTORY + "'.")
            os.makedirs(TEMP_DIRECTORY)
        else:
            write_log_entry("Temp directory '" + TEMP_DIRECTORY + "' already exists.")
        if not os.path.isdir(TEMP_DIRECTORY):
            raise Exception("Unable to create temp directory '" + TEMP_DIRECTORY + "'.")

        # Make sure the output directory exists.  Create it if not.
        if not os.path.isdir(OUTPUT_DIRECTORY):
            write_log_entry("Creating output directory '" + OUTPUT_DIRECTORY + "'.")
            os.makedirs(OUTPUT_DIRECTORY)
        else:
            write_log_entry("Output directory '" + OUTPUT_DIRECTORY + "' already exists.")
        if not os.path.isdir(OUTPUT_DIRECTORY):
            raise Exception("Unable to create output directory '" + OUTPUT_DIRECTORY + "'.")

        # Does the output file already exist?
        if os.path.isfile(OUTPUT_FILE_PATH):
            write_log_entry("Output file '" + OUTPUT_FILE_PATH + "' already exists.  New entries will be appended.")
        else:
            write_log_entry("Output file '" + OUTPUT_FILE_PATH + "' does not exist.  It will be created.")

        # Get the list of input files.
        input_file_names = get_input_file_names()
        input_file_count = len(input_file_names)
        write_log_entry(str(input_file_count) + " unprocessed input files found.")

        # Start processing files.
        for i in range(input_file_count):
            process_input_file(input_file_names[i])
            write_log_entry(str(i + 1) + " of " + str(input_file_count) + " files processed.  " +
                            str(input_file_count - i - 1) + " files remain.")

    else:

        raise Exception("The Government Interests Parser has only been tested against the following versions of "
                        "Python: [" + ", ".join(SUPPORTED_PYTHON_VERSIONS) + "]. Currently running unsupported "
                        "version " + CURRENT_PYTHON_VERSION + ".")


# Main function.  Should not do anything if called by another Python script.
if __name__ == "__main__":
    try:
        write_log_entry("Processing started for " + sys.argv[0])
        log_constants()
        main()
        write_log_entry("Processing finished successfully")
    except:
        for line in traceback.format_exc().splitlines():
            write_log_entry(line)
        sys.exit(1)
