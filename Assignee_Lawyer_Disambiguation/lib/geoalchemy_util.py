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
import re
from bs4 import BeautifulSoup
import unicodedata
import Levenshtein as leven

#Return a ", " separated string of the location
def concatenate_location(city, state, country):
    location_list = []
    if(city and city!=''):
        location_list.append(city)
    if(state and state!=''):
        location_list.append(state)
    if(country and country!=''):
        location_list.append(country)
    location = ", ".join(location_list)
    return location

remove_eol_pattern = re.compile(ur'[\r\n]+')

#Many accent references are difficult to idenity programmatically.
#These are handled by manually replacing each entry.
#The replacements are stored in lib/manual_replacement_library.txt
#In the format: original_pattern|replacement
def generate_manual_patterns_and_replacements(library_file_name):
    manual_replacement_file = open(library_file_name, 'r')

    #first, generate the mappings from the file
    #mappings[i] contains (pattern, replacement) 
    mappings = list()
    for line in manual_replacement_file:
        #allow # to be a comment, allow empty lines
        if(line[0]=='#' or line=='\n' or line=='\r\n'):
            continue
        #Parse the substitution line
        line_without_newline = remove_eol_pattern.sub('',line)
        line_split = line_without_newline.split("|")
        mappings.append((line_split[0],line_split[1].decode('utf-8')))

    generated_patterns = list()
    generated_replacements = list()
    i=0
    length = len(mappings)
    while True:
        map_slice = mappings[i*100:(i+1)*100-1]
        generated_pattern_list = '|'.join('(%s)' % re.escape(p) for p, r in map_slice) 
        generated_patterns.append(re.compile(generated_pattern_list, re.UNICODE))
        replacement_list = [replacement for pattern, replacement in map_slice]
        generated_replacements.append(lambda m: replacement_list[m.lastindex-1])
        i+=1
        if (i+1)*100>=length:
            break
    """
    #multisub, but only done once for speed
    manual_pattern_0 = '|'.join('(%s)' % re.escape(p) for p, s in manual_mapping_0)
    substs_0 = [s for p, s in manual_mapping_0]
    manual_replacements_0 = lambda m: substs_0[m.lastindex - 1]
    manual_pattern_compiled_0 = re.compile(manual_pattern_0, re.UNICODE)
    
    manual_pattern_1 = '|'.join('(%s)' % re.escape(p) for p, s in manual_mapping_1)
    substs_1 = [s for p, s in manual_mapping_1]
    manual_replacement_1 = lambda n: substs_1[n.lastindex - 1]
    manual_pattern_compiled_1 = re.compile(manual_pattern_1, re.UNICODE)
    """    
    
    return generated_patterns, generated_replacements 
    
def get_chars_in_parentheses(text):
    text = text.group(0)
    #match text surrounded by parentheses
    pattern = re.compile(ur'(?<=\().*?(?=\))', re.UNICODE)
    match = pattern.search(text)
    try:
        match = match.group(0)
    except:
        #Happens if the match is empty
        match = ''
    #Return an empty string instead of a single space because the single space means an empty string is more appropriate
    if not (match==' '):
        return match
    return ''

#We don't have manual replacements for every accent yet
#These patterns perform quick fixes which let us parse the data

def generate_quickfix_patterns():
    curly_pattern = re.compile(ur'\{.*?\}',re.UNICODE)
    quickfix_slashes = re.compile(ur'/[a-zA-Z]/',re.UNICODE)
    return {'curly':curly_pattern, 'slashes':quickfix_slashes}

manual_patterns, manual_replacements = generate_manual_patterns_and_replacements("lib/manual_replacement_library.txt")
quickfix_patterns = generate_quickfix_patterns()
postal_pattern = re.compile(ur'(- )?[A-Z\-#\(]*\d+[\)A-Z]*', re.UNICODE)
foreign_postal_pattern = re.compile(ur'[A-Z\d]{3,4}[ ]?[A-Z\d]{3}', re.UNICODE)

separator_pattern = re.compile(ur'\|', re.UNICODE)

#Remove unnecessary symbols|whitespace in excess of one space|
#start of line symbols
unnecessary_symbols_pattern = re.compile(ur'[#\(?<!.\)]')
excess_whitespace_pattern = re.compile(ur'(?<= )( )+')
start_of_line_removal_pattern = re.compile(ur'^(late of|LATE OF)?[\-,/:;_& ]*', re.MULTILINE)
#A single letter followed by non-letters is unlikely to be useful information
#More likely, it is what remains of other removals, such as streets and addresses
start_of_line_letter_removal_pattern = re.compile(ur'^( )*[A-Za-z][\-, ]+', re.MULTILINE)
extra_commas_pattern = re.compile(ur'(( )*,( )*)+')

england_pattern = re.compile(ur', EN')
germany_pattern = re.compile(ur', DT')
japan_pattern = re.compile(ur', JA')
russia_pattern = re.compile(ur', SU')

#Input: a raw location from the parse of the patent data
def clean_raw_location(text):
    text = remove_eol_pattern.sub('', text)
    text = separator_pattern.sub(', ', text)
    #Perform all of the manual replacements
    i=0
    text = perform_replacements(manual_patterns, manual_replacements, text)
    #Perform all the quickfix replacements
    text = quickfix_patterns['curly'].sub(get_chars_in_parentheses, text)
    
    #Turn accents into unicode
    soup = BeautifulSoup(text)
    text = unicode(soup.get_text())
    text =  unicodedata.normalize('NFC', text)
    
    text = foreign_postal_pattern.sub('', text)
    text = postal_pattern.sub('', text)
    
    text = unnecessary_symbols_pattern.sub('', text)
    text = excess_whitespace_pattern.sub('', text)
    text = start_of_line_removal_pattern.sub('', text)
    text = start_of_line_letter_removal_pattern.sub('', text)
    text = extra_commas_pattern.sub(', ', text)
    #around commas
    text = england_pattern.sub(', GB', text)
    text = germany_pattern.sub(', DE', text)
    text = japan_pattern.sub(', JP', text)
    text = russia_pattern.sub(', RU', text)
    return text

def perform_replacements(generated_patterns, replacement_function, text):
    length = len(generated_patterns)
    for i in xrange(length):
        text = generated_patterns[i].sub(replacement_function[i],text)
    return text

def get_closest_match_leven(text, comparison_list, minimum_match_value):
    closest_match = ''
    closest_match_value=0
    for comparison_text in comparison_list:
        temp_match_value = leven.jaro(text, comparison_text)
        if temp_match_value>closest_match_value:
            closest_match = comparison_text
            closest_match_value = temp_match_value
    if closest_match_value>minimum_match_value:
        return closest_match
    else:
        return '' 
    
#Parse the country from a cleaned location
def get_country_from_cleaned(text):
    text_split = text.split(',')
    country = text_split[-1].strip()
    return country

capital_pattern = re.compile(r'[A-Z]')
def region_is_a_state(region):
    return capital_pattern.match(region)

#The code merely applies the replacements in the lib/state_abbreviations.txt
#It is intended to force US state names to use abbreviations.
def fix_state_abbreviations(locations):
    state_patterns, state_replacements = generate_manual_patterns_and_replacements('lib/state_abbreviations.txt')
    for location in locations:
        matching_location = location['matching_location']
        matching_location.region = perform_replacements(state_patterns, state_replacements, matching_location.region)
