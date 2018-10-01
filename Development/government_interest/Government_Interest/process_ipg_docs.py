#!/usr/bin/env python
"""
Reads the extracted XML docs from 2005 on and writes government interest
records.
"""

__author__ = 'rkimble'
#edited by skelley


from csv import DictWriter
from gzip import GzipFile
from glob import glob
from optparse import OptionParser
import os
from os import makedirs
from os.path import basename, isdir, isfile, join, splitext

from bs4 import BeautifulSoup

from govint_common import coalesce_strings, to_ascii
from govint_common import separate_into_govint_docs_as_lines_ipg
from govint_common import separate_into_govint_docs_as_lines_pftaps
from govint_common import separate_into_govint_docs_as_lines_pg
from govint_common import parse_pftaps_doc
from govint_common import format_count, time_stamp_message



def main(input_dir, output_folder):
    """Main routine."""
    time_stamp_message('Starting.')


    in_file_names = os.listdir(input_dir)
    time_stamp_message('%s input files found.' % (format_count(len(
        in_file_names)),))

    fields = 'pat_no,iss_dt,hdr,p'.split(',')
    os.mkdir(output_folder)
    for in_file_name in sorted(in_file_names):
        time_stamp_message('Processing %s.' % (in_file_name,))
        #in_file_pointer = GzipFile(in_file_name, 'r')
        #print in_file_pointer
        print in_file_name
        out_file_name = join(output_folder, basename(in_file_name).replace(
            '.txt.gz', '.csv'))
        out_file_pointer = open(out_file_name, 'wb')
        writer = DictWriter(out_file_pointer, fields)
        writer.writeheader()
        for doc_lines in separate_into_govint_docs_as_lines_ipg(
                input_dir + "/" + in_file_name, check_zip=True):
            lines = []
            in_govint = False
            for line in doc_lines:
                line = line.strip()
                if line.startswith('<?xml'):
                    lines.append(line)
                elif line.startswith(('<!DOCTYPE')):
                    lines.append(line)
                elif line.startswith('<us-patent-grant'):
                    lines.append(line)
                elif line.startswith('</us-patent-grant'):
                    lines.append(line)
                elif line.startswith('<?GOVINT'):
                    in_govint = not in_govint
                elif in_govint:
                    lines.append(line)

            soup = BeautifulSoup(''.join(lines), features='xml')
            records = []
            hdr = None
            p = []
            row = dict([(field, None) for field in fields])
            upg = soup.find_all('us-patent-grant')[0]
            pat_no = upg['file'].split('-')[0]
            row ['pat_no'] = pat_no
            iss_dt = upg['date-publ']
            row ['iss_dt'] = iss_dt
            for child in upg:
                if child.name == 'heading':
                    if hdr or p:
                        records.append((hdr, ' '.join(p).strip()))
                    hdr = to_ascii(coalesce_strings(child))
                elif child.name == 'p':
                    p.append(to_ascii(coalesce_strings(child)))
            if hdr or p:
                records.append((hdr, ' '.join(p).strip()))
            if records:
                for record in records:
                    row['hdr'], row['p'] = record
                    writer.writerow(row)
            else:
                print ''.join(lines)
                exit(-1)


    time_stamp_message('Finished.')
    return


# if '__main__' == __name__:
#     main()
