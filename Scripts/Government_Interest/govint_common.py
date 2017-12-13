#!/usr/bin/env python
"""Common routines for use in the govt_interest project."""

__author__ = 'rkimble'


from csv import DictReader, DictWriter, reader, writer
from locale import format, LC_ALL, setlocale
from os import linesep, makedirs
from os.path import isdir, join
from pprint import pprint
from re import compile as reCompile, split as reSplit
from string import digits, lowercase
from sys import stderr, stdout
from time import asctime
from zipfile import is_zipfile, ZipFile
import gzip

import nltk.corpus
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk import WordNetLemmatizer


#==============================================================================
#
# """Knuth-Morris-Pratt algorithm
#
# This is a direct transaltion of the KMP algorithm in the book
# "Introduction to Algorithms" by Cormen, Lieserson, and Rivest.  See that
# book for an explanation of why this algorithm works.  It's pretty cool.
#
# The only things I changed were some offsets, to cope with the fact that
# Python arrays are 0-offset.
#
# """
#
# __author__ = 'Neale Pickett <neale@woozle.org>'
#

def compute_prefix_function(p):
    """
    :param p: An iterable representing a pattern.
    :return:  A list representing the prefix function.
    Compute and return the prefix function for the pattern.
    """
    pi = [0] * m
    k = 0
    for q in range(1, m):
        while k > 0 and p[k] != p[q]:
            k = pi[k - 1]
        if p[k] == p[q]:
            k = k + 1
        pi[q] = k
    return pi

def kmp_matcher(t, p, pf=None):
    """
    :param t:  The "text" to be searched. Any iterable object will do.
    :param p:  The pattern to search for. An iterable compatible with t.
    :param pf: A precomputed prefix function, in case the function is to be
               numerous times. No sense in recomputing the prefix function
               each time.
    :return:   The index to the start of the first occurrence of the pattern,
               -1 if not found.
    Runs the Knuth-Morris-Pratt algorithm.
    """
    n = len(t)
    m = len(p)
    pi = pf
    if pi is None:
        pi = compute_prefix_function(p)
    q = 0
    for i in range(n):
        while q > 0 and p[q] != t[i]:
            q = pi[q - 1]
        if p[q] == t[i]:
            q = q + 1
        if q == m:
            return i - m + 1
    return -1

#==============================================================================


def time_stamp_message(msg, out=stdout):
    """Prints a timestamped message."""
    print >> out, asctime() + ': ' + msg
    return


setlocale(LC_ALL, '')
def format_count(count):
    """Returns a locale formatted version of the count."""
    return format('%d', count, grouping=True)


def natural_key(astr):
    """See http://www.codinghorror.com/blog/archives/001018.html"""
    nat_key = reSplit(r'(\d+)', astr.lower())
    nat_key[1::2] = map(int, nat_key[1::2])
    return nat_key

def naturally_sorted(alist):
    """Returns a naturally sorted copy of alist."""
    return sorted(alist, key=natural_key)


def separate_into_govint_docs_as_lines_pftaps(file_pointer, check_zip=True):
    """
    Reads the file given by file_pointer and yields the individual documents as
    lists of lines. Only the documents that contain a line starting with GOVT
    are returned.
    :param fname: name of file containing documents in "Green Book" format.
    :return: an iterator yielding one document at a time.
    """
    if check_zip and is_zipfile(file_pointer):
        zfile = ZipFile(file_pointer)
        read_pointer = zfile.open(zfile.namelist()[0], 'r')
    else:
        read_pointer = file_pointer
        read_pointer.seek(0)
    lines = []
    govint = False
    for line in read_pointer:
        if line.startswith('HHHHHT'):
            continue
        if line.startswith('PATN'):
            if govint and lines:
                yield lines
            lines = [line]
            govint = False
        else:
            lines.append(line)
        if line.startswith('GOVT'):
            govint = True
    if govint and lines:
        yield lines


def separate_into_govint_docs_as_lines_pg(file_pointer, check_zip=True):
    """
    Reads the file given by file_pointer and yields the individual documents as
    lists of lines. Only the documents that contain a line starting with <GOVINT>
    are returned.
    :param fname: name of file containing documents in "??? Book" format.
    :return: an iterator yielding one document at a time.
    """
    if check_zip and is_zipfile(file_pointer):
        zfile = ZipFile(file_pointer)
        read_pointer = zfile.open(zfile.namelist()[0], 'r')
    else:
        read_pointer = file_pointer
        read_pointer.seek(0)
    lines = []
    govint = False
    for line in read_pointer:
        if line.startswith('<?xml ') or line.startswith('<!DOCTYPE PATDOC PUBLIC'):
            if govint and lines:
                yield lines
            lines = [line]
            govint = False
        else:
            lines.append(line)
        if line.startswith('<GOVINT>'):
            govint = True
    if govint and lines:
        yield lines


def separate_into_govint_docs_as_lines_ipg(file_pointer, check_zip=True):
    """
    Reads the file given by file_pointer and yields the individual documents as
    lists of lines. Only the documents that contain a line starting with <GOVINT>
    are returned.
    :param fname: name of file containing documents in "??? Book" format.
    :return: an iterator yielding one document at a time.
    """
    #file_pointer is now going to be a file name
    if check_zip == True and file_pointer.endswith("gz"):
        read_pointer = gzip.open(file_pointer, 'r')
    else:
        read_pointer = open(file_pointer, 'r')
    lines = []
    govint = False
    for line in read_pointer:
        if line.startswith('<?xml '):
            if govint and lines:
                yield lines
            lines = [line]
            govint = False
        else:
            lines.append(line)
        if line.startswith('<?GOVINT'):
            govint = True
    if govint and lines:
        yield lines


def parse_pftaps_doc(doc_lines):
    """
    Parses a pftaps formatted doc_lines and returns a list of attribute-value pairs.
    The attributes are not unique.
    :param doc_lines: list of lines representing document.
    :return: list of 2-tuples.
    """
    parsed_doc = []
    for line in doc_lines:
        code = line[:4].strip()
        value = line[5:]
        if code:
            parsed_doc.append((code, value))
        else:
            try:
                last_code, last_value = parsed_doc[-1]
                parsed_doc[-1] = (last_code, last_value + value)
            except:
                pprint(parsed_doc[-1])
                print value
                raise
    return parsed_doc


def coalesce_strings(tag):
    """
    Returns a coalesced string of all the strings found inside that given tag
    object.
    :param tag: BeautifulSoup tag object.
    :return: A string representing all the contents of the tag object.
    """
    return ' '.join(' '.join(tag.strings).split())


def canon_pat_no(pat_no):
    """
    Returns the canonical 7 character patent number version of the input.
    :param pat_no:
    :return:
    """
    pat_no = pat_no.upper()
    if pat_no.startswith('US'):
        pat_no = pat_no[2:]
    if 8 == len(pat_no):
        if '0' in pat_no:
            idx = pat_no.find('0')
            pat_no = pat_no[:idx] + pat_no[1 + idx:]
    return pat_no


_bad_chars = set()
def to_ascii(u, replacement_char='?'):
    """Converts unicode string to ASCII safely."""
    try:
        return str(u)
    except:
        pass
    s = []
    for ch in u:
        try:
            s.append(str(ch))
        except:
            if u'\xa7' == ch or u'\xd7' == ch:
                s.append('(S)')
            elif u'\xa9' == ch:
                s.append('(c)')
            elif u'\xb0' == ch:
                s.append('(degrees)')
            elif u'\xd7' == ch:
                s.append(' X ')
            elif u'\xe4' == ch:
                s.append('ae')
            elif u'\xed' == ch:
                s.append('e')
            elif u'\xf1' == ch:
                s.append('n')
            elif u'\xf3' == ch:
                s.append('o')
            elif u'\xfc' == ch:
                s.append('u')
            else:
                _bad_chars.add(ch)
                s.append(replacement_char)
    return ''.join(s)


def fix_ascii(s, replacement_char='?'):
    """Replaces ASCII characters higher than 127 with replacement character."""
    s_chars = []
    for ch in s:
        if 128 <= ord(ch):
            s_chars.append(replacement_char)
        else:
            s_chars.append(ch)
    return ''.join(s_chars)


_wnl = WordNetLemmatizer()
_lemmatizer = {}
def lemmatize(word):
    """Looks up the lemma for a word and then remembers it."""
    if word in _lemmatizer:
        return _lemmatizer[word]
    lemma = _wnl.lemmatize(word)
    _lemmatizer[word] = lemma
    return lemma


def read_rollups(csv_fn):
    """
    Reads the data from the supplied filename and returns a dictionary whose
    keys are the individual organizations and whose values are all the parents
    associated with each org..
    """
    with open(csv_fn, 'rb') as csv_fp:
        csv_reader = reader(csv_fp)
        rollups = {}
        for row in csv_reader:
            org = row[0]
            rollups[org] = row
        return rollups


def orgs_sort_key(org):
    return -len(org['tokens']), org['tokens']


def read_orgs(csv_fn):
    """
    Reads the data from the supplied filename and returns a set of the lines
    read, one organization to a line.
    """
    with open(csv_fn, 'rb') as csv_fp:
        csv_reader = DictReader(csv_fp)
        orgs = []
        for row in csv_reader:
            row['tokens'] = [lemmatize(token) for token in
                word_tokenize(replace_punctuation(row['lower']))
                if not token in stop_words]
            row['prefix_function'] = compute_prefix_function(row['tokens'])
            orgs.append(row)
        orgs.sort(key=orgs_sort_key)
        return orgs


def replace_punctuation(text):
    for punct in set(text).intersection(r'\.,;:()[]{}"?/&$#@+*|!'):
        text = text.replace(punct, ' ')
    return ' '.join(text.split())


stop_words = set(stopwords.words('english'))
stop_words.discard('have')
stop_words.discard('no')
stop_words.discard('not')
stop_words.discard('with')
def standardize_sentence(sentence):
    sentence = sentence.strip().lower()
    if '' == sentence:
        return sentence
    for punct in '.,;:()[]{}*|':
        sentence = sentence.replace(punct, ' ')
    sentence = sentence.replace('u.s.', 'united states')
    sentence = sentence.replace('u. s.', 'united states')
    sentence = sentence.replace(' u s ', ' united states ')
    #sentence = sentence.replace(' may ', ' ')
    tokens = word_tokenize(sentence)
    tokens = [lemmatize(token) for token in tokens if not token in
        stop_words]
    return ' '.join(tokens)


def read_ignores(ignore_fn):
    """
    Reads the lines to ignore when writing to orgs_analysis.csv.
    :param ignore_fn: Name of file containing the lines to ignore.
    :return: Set of ignored lines.
    """
    with open(ignore_fn, 'rb') as ignore_fp:
        return set([line.strip() for line in ignore_fp.read().splitlines()])


def write_ignores(ignore_fn, ignores):
    """
    Writes the lines to ignore when writing to orgs_analysis.csv.
    :param ignore_fn: Name of file containing the lines to ignore.
    :param ignores: A set of the lines to ignore.
    :return: None
    """
    with open(ignore_fn, 'wb') as ignore_fp:
        print >> ignore_fp, linesep.join(list(sorted(ignores)))


sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')


s_digits = set(digits)
s_lowercase = set(lowercase)
def is_contract_number(word):
    if '-' in word and s_digits.intersection(word):
        return True
    elif s_lowercase.intersection(word) and s_digits.intersection(word):
        return True
    return False


def hyperlink(url, text):
    """Returns an expression that yields an Excel hyperlink."""
    url = url.replace('"', '""')
    text = text.replace('"', '""')
    text = text.encode('ascii', 'replace')
    parts = [('=HYPERLINK("%s", "%s")' % (url, text))]
    return ''.join(parts)


def get_fund_org_writer(org):
    """
    Returns a writer for the funding org:
    :param org: String containing organization name.
    :return: CSV writer instance along with its file pointer.
    """
    if not isdir('funding_orgs'):
        makedirs('funding_orgs')
    org_fn = join('funding_orgs', org.lower().replace(' ', '_') + '.csv')
    org_fp = open(org_fn, 'wb')
    org_writer = writer(org_fp)
    org_writer.writerow(['pat_no', 'rec_no', 'line_type', 'line'])
    return org_writer, org_fp


def get_fund_org_rollup_writer(org):
    """
    Returns a writer for the funding org:
    :param org: String containing organization name.
    :return: CSV writer instance along with its file pointer.
    """
    if not isdir('funding_orgs_rollup'):
        makedirs('funding_orgs_rollup')
    org_fn = join('funding_orgs_rollup', org.lower().replace(' ', '_') + '.csv')
    org_fp = open(org_fn, 'wb')
    org_writer = writer(org_fp)
    org_writer.writerow(['pat_no', 'rec_no', 'line_type', 'line'])
    return org_writer, org_fp


def read_canonical(csv_fn):
    rows = []
    with open(csv_fn, 'rb') as csv_fp:
        csv_reader = DictReader(csv_fp)
        orgs = []
        for row in csv_reader:
            orgs.append(row)
        rows.extend(orgs)
    return dict([(row['canonical'], row['belongs_to']) for row in rows])


def write_canonical(csv_fn, belongs_to):
    with open(csv_fn, 'wb') as csv_fp:
        can_writer = writer(csv_fp)
        can_writer.writerow('canonical,belongs_to'.split(','))
        can_writer.writerows(list(sorted(belongs_to.items())))


def main():
    """Main routine."""
    print >> stderr, 'This module is not intended to be run directly.'
    return


if '__main__' == __name__:
    main()
