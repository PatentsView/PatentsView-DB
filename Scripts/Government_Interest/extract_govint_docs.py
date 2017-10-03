#!/usr/bin/env python
"""
Extracts the government interest documents from the supplied concatenated-docs
filenames. Writes output files into the output directory, default=govintdocs.
"""

__author__ = 'rkimble'
#major edits by Sarah Kelley


from glob import glob
from gzip import GzipFile
from optparse import OptionParser
import os
from os import makedirs
from os.path import basename, isdir, isfile, join, splitext

from govint_common import separate_into_govint_docs_as_lines_ipg
from govint_common import separate_into_govint_docs_as_lines_pftaps
from govint_common import separate_into_govint_docs_as_lines_pg
from govint_common import format_count, time_stamp_message



def process_file(input_dir, in_file_name, out_file_name):
    """
    Uses the start of the in_file_name to determine what kind of documents it
    contains, and from that reads in the government interest documents and
    writes them to the output file.
    :param input_dir: directory where the input files are
    :param in_file_name: input file name.
    :param out_file_name: output file name.
    :return: Number of government interest documents found.
    """
    base_name = basename(in_file_name).lower()
    if base_name.startswith('ipg'):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_ipg
    elif base_name.startswith(('pftaps')):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pftaps
    elif base_name.startswith('pg'):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pg
    else:
        msg = 'Unable to determine file type of %s from name.' % (in_file_name,)
        raise ValueError(msg)
    out_file = GzipFile(out_file_name, 'w')
    gi_ct = 0
    for lines in separate_into_govint_docs_as_lines(input_dir + "/" + in_file_name):
        gi_ct += 1
        out_file.writelines(lines)
    return gi_ct


def main(input_dir, output_dir):
    """Main routine."""
    time_stamp_message('Starting.')

    in_file_names = os.listdir(input_dir)
    

    time_stamp_message('%s input files found.' % (format_count(len(
        in_file_names)),))

    processed_count = 0
    ignored_count = 0
    docs_written = 0
    os.mkdir(output_dir)
    for in_file_name in sorted(in_file_names):
        in_base_name = basename(in_file_name).lower()
        out_base_name = '%s.txt.gz' % (splitext(in_base_name)[0],)
        out_file_name = os.path.join(output_dir, out_base_name)
        if isfile(out_file_name):
            if not opts.overwrite:
                ignored_count += 1
                continue
        time_stamp_message('Processing %s.' % (in_file_name,))
        docs_count = process_file(input_dir, in_file_name, out_file_name)
        docs_written += docs_count
        time_stamp_message('- %s documents written to %s.' % (format_count(
            docs_count), out_file_name))
        processed_count += 1

    time_stamp_message('%s files processed, %s docs written.' % (
        format_count(processed_count), format_count(docs_written)))
    time_stamp_message('%s files ignored.' % (format_count(ignored_count),))
    time_stamp_message('Finished.')
    return


# if '__main__' == __name__:
#     main()
