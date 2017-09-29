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
Creates the invpat file as seen and constructed in the Harvard DVN project
"""
from lib import alchemy
import pandas as pd

session_generator = alchemy.session_generator
session = session_generator()

#res = session.execute('select rawinventor.name_first, rawinventor.name_last, rawlocation.city, rawlocation.state, \
#                rawlocation.country, rawinventor.sequence, patent.id, \
#                year(application.date), year(patent.date), rawassignee.organization, uspc.mainclass_id, inventor.id \
#                from rawinventor left join patent on patent.id = rawinventor.patent_id \
#                left join application on application.patent_id = patent.id \
#                left join rawlocation on rawlocation.id = rawinventor.rawlocation_id \
#                left join rawassignee on rawassignee.patent_id = patent.id \
#                left join uspc on uspc.patent_id = patent.id \
#                left join inventor on inventor.id = rawinventor.inventor_id \
#                where uspc.sequence = 0;')
res = session.execute('select rawinventor.name_first, rawinventor.name_last, location.city, location.state, \
                location.country, rawinventor.sequence, patent.id, year(application.date), \
                year(patent.date), rawassignee.organization, uspc.mainclass_id, inventor.id \
                from rawinventor, rawlocation, patent, application, rawassignee, uspc, inventor,location \
                where rawinventor.patent_id = patent.id and \
                application.patent_id = patent.id and \
                rawlocation.id = rawinventor.rawlocation_id and \
                location.id = rawlocation.location_id and \
                rawassignee.patent_id = patent.id and \
                uspc.patent_id = patent.id and \
                inventor.id = rawinventor.inventor_id;')
data = pd.DataFrame.from_records(res.fetchall())
data = data.drop_duplicates((6,11))
data.columns = ['first_name', 'last_name', 'city', 'state', 'country', 'sequence', 'patent', 'app_year', 'grant_year', 'assignee', 'mainclass', 'inventorid']
data.to_csv('invpat.csv',index=False,encoding='utf8')
