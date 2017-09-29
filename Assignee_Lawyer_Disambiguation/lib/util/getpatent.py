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

import sys
import re
import time
import mechanize
from BeautifulSoup import BeautifulSoup

if len(sys.argv) < 2:
    print "Given a patent id number, will download the relevant zipfile"
    print "Usage: ./getpatent.py <patent id number>"
    print "Example: ./getpatent.py 7783348"
    sys.exit(0)

patent_name = sys.argv[1]

if patent_name[:2].upper() != 'US':
    patent_name = 'US'+patent_name

BASE_URL = 'http://www.google.com/patents/'
ZIP_BASE_URL = 'http://commondatastorage.googleapis.com/patents/grant_full_text/'
br = mechanize.Browser()
br.addheaders = [('User-agent', 'Feedfetcher-Google-iGoogleGadgets;\
 (+http://www.google.com/feedfetcher.html)')]
br.set_handle_robots(False)
html = br.open(BASE_URL+patent_name).read()

print 'Got HTML for patent page'

soup = BeautifulSoup(html)
sidebar = soup.find('div', {'class': 'patent_bibdata'})
text = str(sidebar.text)
date = re.search(r'(?<=Issue date: )[A-Za-z]{3} [0-9]{1,2}, [0-9]{4}', text).group()
date_struct = time.strptime(date, '%b %d, %Y')
year = str(date_struct.tm_year)[2:]
month = str(date_struct.tm_mon).zfill(2)
day = str(date_struct.tm_mday).zfill(2)

zipfile = 'ipg{0}{1}{2}.zip'.format(year,month,day)

zipurl = '{0}{1}/{2}'.format(ZIP_BASE_URL,date_struct.tm_year,zipfile)

print 'Downloading ZIP file: ',zipurl

res = br.retrieve(zipurl, zipfile)
print res

print 'Finished downloading'
