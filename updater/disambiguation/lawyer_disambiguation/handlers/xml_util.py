#!/usr/bin/env python
"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""

"""
Collection of useful functions and tools for working with XML documents
"""

import re

from unicodedata import normalize
from cgi import escape


def flatten(ls_of_ls):
    """
    Takes in a list of lists, returns a new list of lists
    where list `i` contains the `i`th element from the original
    set of lists.
    """
    return list(map(list, list(zip(*ls_of_ls))))

def extend_padding(ls_of_ls, padding=''):
    """
    Takes in a lists of lists, returns a new list of lists
    where each list is padded up to the length of the longest
    list by [padding] (defaults to the empty string)
    """
    maxlen = max(list(map(len, ls_of_ls)))
    newls = []
    for ls in ls_of_ls:
        if len(ls) != maxlen:
            ls.extend([padding]*(maxlen - len(ls)))
        newls.append(ls)
    return newls

def escape_html_nosub(string):
    """
    Escapes html sequences (e.g. <b></b>) that are not the known idiom
    for subscript: <sub>...</sub>
    """
    lt = re.compile('<(?!/?sub>)',flags=re.I)
    gt = re.compile('(?=.)*(?<!sub)>',flags=re.I)
    amp = re.compile('&(?!(amp;|lt;|gt;))',flags=re.I)
    string = re.sub(amp,'&amp;',string)
    string = re.sub(lt,"&lt;",string)
    string = re.sub(gt,"&gt;",string)
    return string

def has_content(l):
    """
    Returns true if list [l] contains any non-null objects
    """
    return any([x for x in l if x])

def normalize_utf8(string):
    """
    Normalizes [string] to be UTF-8 encoded. Accepts both unicode and normal
    Python strings.
    """
    if isinstance(string, str):
        return normalize('NFC', string)
    else:
        return normalize('NFC', string.decode('utf-8'))

def remove_escape_sequences(string):
    """
    Replaces all contiguous instances of "\r\n\t\v\b\f\a " and replaces
    it with a single space. Preserves at most one space of surrounding whitespace
    """
    escape_seqs = r'[\r\n\t\v\b\f\a ]+'
    return re.sub(escape_seqs,' ', string)

def translate_underscore(string, lower=False):
    """
    Replaces the underscore HTML idiom <sub>&#x2014;</sub> with the literal
    underscore character _.
    """
    if lower:
        string = string.lower()
    return string.replace('<sub>&#x2014;</sub>','_').replace('<sub>-</sub>','_').replace('<sub>\u2014</sub>','_')


def escape_html(string):
    """
    Call cgi.escape on the string after applying translate_underscore
    """
    s = translate_underscore(string)
    return escape(s)

def normalize_document_identifier(identifier):
    """
    [identifier] is a string representing the document-id field from an XML document
    """
    # create splits on identifier
    if not identifier: return ''
    return re.sub(r'([A-Z]*)0?',r'\g<1>',identifier,1)

def associate_prefix(firstname, lastname):
    """
    Prepends everything after the first space-delineated word in [firstname] to
    [lastname].
    """
    if ' ' in firstname:
        name, prefix = firstname.split(' ',1) # split on first space
    else:
        name, prefix = firstname, ''
    space = ' '*(prefix is not '')
    last = prefix+space+lastname
    return name, last

def clean(string, upper=True):
    """
    Applies a subset of the above functions in the correct order
    and returns the string in all uppercase.

    Change &amp;
    """
    string = normalize_utf8(string)
    string = remove_escape_sequences(string)
    string = translate_underscore(string)
    string = escape_html(string)
    string = string.replace("&nbsp;", " ").replace("&amp;", "&")
    if upper:
        return string.upper()
    else:
        return string

def augment_class(string):
    """
    Given a [string] representing the contents of a <main-classification> tag
    (see USPTO XML Documentation 4.2 or later), realize the semantic meaning
    of the string and return a string of form recognized by USPTO:
    <main-class>/<sub-class>.<more-sub-class>
    """
    mainclass = string[:3]
    subclass1 = string[3:6]
    subclass2 = string[6:]
    if subclass2:
        return "{0}/{1}.{2}".format(mainclass, subclass1, subclass2)
    return "{0}/{1}".format(mainclass, subclass1)
