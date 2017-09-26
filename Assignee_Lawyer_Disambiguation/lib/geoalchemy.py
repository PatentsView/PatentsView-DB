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
Performs geolocation disambiguation
"""
import sqlalchemy
import sqlalchemy.orm as orm
import sqlalchemy.ext.declarative as declarative
import sqlalchemy.sql.expression as expression
from sqlalchemy.sql.expression import bindparam
import geoalchemy_util
import itertools
import os
import datetime
import re
import sys
from collections import defaultdict, Counter
import pandas as pd
import csv
import codecs


import alchemy
from alchemy.match import commit_inserts, commit_updates
from tasks import bulk_commit_inserts, bulk_commit_updates

global doctype
doctype = ''

#The config file alchemy uses to store information
alchemy_config = alchemy.get_config()
#The path to the database which holds geolocation data
geo_data_dbpath = os.path.join(
    alchemy_config.get("location").get('path'),
    alchemy_config.get("location").get('database'))
geo_data_engine = sqlalchemy.create_engine('sqlite:///%s' % geo_data_dbpath)
geo_data_session_class = orm.sessionmaker(bind=geo_data_engine)
#Used to query the database that holds the data from google
#As well as a MaxMinds database containing every city in the world
geo_data_session = geo_data_session_class()
base = declarative.declarative_base()
commit_freq = alchemy_config.get("location").get("commit_frequency")

#Stores an address disambiguated by the Google API
class RawGoogle(base):
    __tablename__ = 'raw_google'
    id = sqlalchemy.Column("rowid", sqlalchemy.Integer, primary_key=True)
    input_address = sqlalchemy.Column(sqlalchemy.String)
    city = sqlalchemy.Column(sqlalchemy.String)
    region = sqlalchemy.Column(sqlalchemy.String)
    country = sqlalchemy.Column(sqlalchemy.String)
    latitude = sqlalchemy.Column(sqlalchemy.REAL)
    longitude = sqlalchemy.Column(sqlalchemy.REAL)
    confidence = sqlalchemy.Column(sqlalchemy.REAL)

    def __init__(self, input_address, city, region, country, latitude, longitude, confidence):
        self.input_address = input_address
        self.city = city
        self.region = region
        self.country = country
        self.latitude = latitude
        self.longitude = longitude
        self.confidence = confidence

    def __repr__(self):
        return "<RawGoogle('%s','%s','%s','%s','%s','%s','%s')>" % (self.input_address, self.city, self.region, self.country, self.latitude, self.longitude, self.confidence)

#One of the cities in the world as stored in the MaxMinds database
class AllCities(base):
    __tablename__ = 'all_cities'
    id = sqlalchemy.Column("rowid", sqlalchemy.Integer, primary_key=True)
    city = sqlalchemy.Column(sqlalchemy.String)
    region = sqlalchemy.Column(sqlalchemy.String)
    country = sqlalchemy.Column(sqlalchemy.String)
    latitude = sqlalchemy.Column(sqlalchemy.REAL)
    longitude = sqlalchemy.Column(sqlalchemy.REAL)

    def __init__(self, city, region, country, latitude, longitude):
        self.city = city
        self.region = region
        self.country = country
        self.latitude = latitude
        self.longitude = longitude

#Perform geocoding on the data stored in alchemy.db provided by parsing XML
def main(limit=None, offset=0, minimum_match_value=0.8, doctype='grant'):
    alchemy_session = alchemy.fetch_session(dbtype=doctype)
    t = datetime.datetime.now()
    print "geocoding started", doctype, t
    #Construct a list of all addresses which Google was capable of identifying
    #Making this now allows it to be referenced quickly later
    valid_input_addresses = construct_valid_input_addresses()
    #Get all of the raw locations in alchemy.db that were parsed from XML
    if doctype == 'grant':
        raw_parsed_locations = alchemy_session.query(alchemy.schema.RawLocation).limit(limit).offset(offset)
    elif doctype == 'application':
        raw_parsed_locations = alchemy_session.query(alchemy.schema.App_RawLocation).limit(limit).offset(offset)
    raw_parsed_locations_count = raw_parsed_locations.count()

    #If there are no locations, there is no point in continuing
    if raw_parsed_locations_count == 0:
        return False
    print 'Constructed list of all parsed locations containing', raw_parsed_locations_count, 'items'
    """
    grouped_loations will contain a list of dicts. Each dict will contain three values:
    raw_location = Location object containing the original location found in the XML
    matching_location = RawGoogle object containing the disambiguated location
    grouping_id = ID constructed from the city, region, and country of the matching_location
    """
    identified_grouped_locations = []
    unidentified_grouped_locations = []
    for instance in raw_parsed_locations:
        #Convert the location into a string that matches the Google format
        parsed_raw_location = geoalchemy_util.concatenate_location(instance.city, instance.state, instance.country)
        cleaned_location = geoalchemy_util.clean_raw_location(parsed_raw_location)
        #If the cleaned location has a match in the raw_google database,
        #we use that to classify it
        if input_address_exists(valid_input_addresses, cleaned_location):
            matching_location = geo_data_session.query(RawGoogle).filter(
                                     RawGoogle.input_address==cleaned_location).first()
            if matching_location:
                grouping_id = u"{0}|{1}".format(matching_location.latitude, matching_location.longitude)
                identified_grouped_locations.append({"raw_location": instance,
                                      "matching_location": matching_location,
                                      "grouping_id": grouping_id})
            else:
                print 'Cleaned location not matched', cleaned_location
                country = geoalchemy_util.get_country_from_cleaned(cleaned_location)
                unidentified_grouped_locations.append({"raw_location": instance,
                                                       "cleaned_location": cleaned_location,
                                                       "country": country})

        else:
            """
            If there is no match in the raw_google database, we leave the location alone
            TODO: analyze the location's edit distance to make minor adjustments to it
            such that it can be matched. Particularly good if we can combine the
            all_cities database with the list of valid input_address values in the
            raw_google database.
            """
            #Sort the locations by their country
            country = geoalchemy_util.get_country_from_cleaned(cleaned_location)
            unidentified_grouped_locations.append({"raw_location": instance,
                                                   "cleaned_location": cleaned_location,
                                                   "country": country})
        if ((len(identified_grouped_locations)+len(unidentified_grouped_locations))%10000 == 0):
            print "Processed", len(identified_grouped_locations)+len(unidentified_grouped_locations), datetime.datetime.now()
    print "locations grouped", datetime.datetime.now() - t
    print 'count of identified locations:', len(identified_grouped_locations)
    t = datetime.datetime.now()
    alchemy_session.close()


    #We now have two lists of locations. First, consider the unmatched locations.
    keyfunc = lambda x:x["country"]
    #Sort the list by the country
    unidentified_grouped_locations.sort(key=keyfunc)
    #Create an iterator that will access everything in the list with the same
    #country
    unidentified_grouped_locations_enum = enumerate(itertools.groupby(unidentified_grouped_locations, keyfunc))
    #Identify the correct location for each entry by comparing to all_cities
    identify_missing_locations(unidentified_grouped_locations_enum,
                               identified_grouped_locations,
                               minimum_match_value, t)
    print 'new count of identified locations:', len(identified_grouped_locations)

    #We now have a list of all locations in the file, along with their
    #matching locations and the id used to group them
    #Perform a quickfix to correct state names
    geoalchemy_util.fix_state_abbreviations(identified_grouped_locations)

    #Sort the list by the grouping_id
    keyfunc = lambda x: x['grouping_id']
    identified_grouped_locations.sort(key=keyfunc)
    #Create an iterator that will access everything in the list with the same
    #grouping_id
    identified_grouped_locations_enum = enumerate(itertools.groupby(identified_grouped_locations, keyfunc))
    print "identified_grouped_locations sorted", datetime.datetime.now() - t
    t = datetime.datetime.now()

    alchemy_session = alchemy.fetch_session(dbtype=doctype)

    #Match the locations
    match_grouped_locations(identified_grouped_locations_enum, t, alchemy_session)

    print "Matches made!", datetime.datetime.now() - t
    if doctype == 'grant':
        unique_group_count = alchemy_session.query(expression.func.count(sqlalchemy.distinct(alchemy.schema.Location.id))).all()
    elif doctype == 'application':
        unique_group_count = alchemy_session.query(expression.func.count(sqlalchemy.distinct(alchemy.schema.App_Location.id))).all()

    print "%s groups formed from %s locations" % (unique_group_count, raw_parsed_locations_count)
    alchemy_session.close()

#Identify locations that the Google disambiguation couldn't resolve
def identify_missing_locations(unidentified_grouped_locations_enum,
                               identified_grouped_locations,
                               minimum_match_value, t):
    #For each group of locations with the same country
    for i, item in unidentified_grouped_locations_enum:
        country, grouped_locations_list = item
        print 'Missing for: ', country, datetime.datetime.now()
        #Get a list of all cities that exist anywhere in that country
        all_cities_in_country = geo_data_session.query(AllCities.city, AllCities.region).filter_by(country=country)
        #Construct a name for each location that matches the normal cleaned location format
        all_cities_in_country = [geoalchemy_util.concatenate_location(x.city,
                           x.region if geoalchemy_util.region_is_a_state(x.region) else '',
                           country) for x in all_cities_in_country]
        #For each location found in this country, find its closest match
        #among the list of all cities from that country
        for grouped_location in grouped_locations_list:
            cleaned_location = grouped_location["cleaned_location"]
            closest_match = geoalchemy_util.get_closest_match_leven(cleaned_location, all_cities_in_country, minimum_match_value)
            #If no match was found or only the trivial match
            if closest_match=='' or closest_match==country:
                continue
            #If we have a good match, add it to the list of matched locations
            closest_match_split = re.split(",",closest_match)
            city = closest_match_split[0].strip()
            if len(closest_match_split)==3:
                region = closest_match_split[1].strip()
                country = closest_match_split[2].strip()
                matching_location = geo_data_session.query(AllCities).filter_by(city=city, region=region, country=country).first()
            else:
                country = closest_match_split[1].strip()
                matching_location = geo_data_session.query(AllCities).filter_by(city=city, country=country).first()
            if not matching_location:
                print 'Warning: all_cities match attempt failed for', cleaned_location.encode('utf8'), 'location not found'
            else:
                grouping_id = u"{0}|{1}".format(matching_location.latitude, matching_location.longitude)
                raw_location = grouped_location["raw_location"]
                identified_grouped_locations.append({"raw_location": raw_location,
                                      "matching_location": matching_location,
                                      "grouping_id": grouping_id})
            #print 'all_cities found additional location for', raw_location

"""
looks like there are not enough records being put into "update_statements". When I look at the rawlocation
table and look for how many records in there have null location_ids,  there are *far* too many.
I'm looking at totalmatchgroups, which is the number of records that get update_statements generated,
externalmatchgroups, which is the number of records that go into the process. I should also look
at identified_grouped_locations_enum, which is a collection of all the grouped locations.
"""
def match_grouped_locations(identified_grouped_locations_enum, t, alchemy_session):
    if alchemy.is_mysql():
        alchemy_session.execute("set foreign_key_checks = 0; truncate location;")
        alchemy_session.commit()
    for i, item in identified_grouped_locations_enum:
        #grouped_locations_list = a list of every grouped location with the same grouping_id
        # Note that a grouped_location is a dict, as described above
        #grouping_id = the grouping_id of all items in the list
        grouping_id, grouped_locations_list = item
        #We need to get only the RawLocation objects back from the grouped_location dict
        #match_group is the list of RawLocation objects which we call match on
        match_group = []
        first_pass=True
        for grouped_location in grouped_locations_list:
            match_group.append(grouped_location["raw_location"])
            if(first_pass):
                first_matching_location = grouped_location["matching_location"]
        """
        default is a dict containing the default values of the parameters
        (id, city, region, country, latitude, longtidue)
        for all locations that are part of the same group.
        Here we set the defaults to be the values for the first entry in the grouped_locations_list
        In theory, all entries in the grouped_locations_list should have the same lat/long.
        """
        default = {"id": grouping_id, "city":first_matching_location.city,
                   "state":first_matching_location.region,
                   "country": first_matching_location.country.upper(),
                   "latitude":first_matching_location.latitude,
                   "longitude":first_matching_location.longitude}
        #No need to run match() if no matching location was found.
        if(grouping_id!="nolocationfound"):
            run_geo_match(grouping_id, default, match_group, i, t, alchemy_session)

    if alchemy.is_mysql():
        alchemy_session.execute('truncate location; truncate location_assignee; truncate location_inventor;')
    else:
        alchemy_session.execute('delete from location;')
        alchemy_session.commit()
        alchemy_session.execute('delete from location_assignee;')
        alchemy_session.commit()
        alchemy_session.execute('delete from location_inventor;')
        alchemy_session.commit()
    if doctype == 'grant':
        bulk_commit_inserts(location_insert_statements, alchemy.schema.Location.__table__, alchemy.is_mysql(), commit_freq, 'grant')
        bulk_commit_updates('location_id', update_statements, alchemy.schema.RawLocation.__table__, alchemy.is_mysql(), commit_freq, 'grant')
    elif doctype == 'application':
        bulk_commit_inserts(location_insert_statements, alchemy.schema.App_Location.__table__, alchemy.is_mysql(), commit_freq, 'application')
        bulk_commit_updates('location_id', update_statements, alchemy.schema.App_RawLocation.__table__, alchemy.is_mysql(), commit_freq, 'application')
    alchemy_session.commit()
    session_generator = alchemy.session_generator(dbtype=doctype)
    session = session_generator()

    session.commit()
    print 'Committed!!!'

def run_geo_match(key, default, match_group, counter, runtime, alchemy_session):
    most_freq = 0
    #If there is more than one key, we need to figure out what attributes
    #(city, region, country, latitude, longitude) to assign the group
    """
    if len(match_group) > 1:
        # if key exists, look at the frequency
        # to determine the default summarization
        clean = alchemy_session.query(alchemy.schema.Location).filter(alchemy.schema.Location.id == key).first()
        if clean:
            param = clean.summarize
            param.pop("id")
            param.pop("latitude")
            param.pop("longitude")
            loc = alchemy_session.query(alchemy.schema.RawLocation)\
                .filter(alchemy.schema.RawLocation.city == param["city"])\
                .filter(alchemy.schema.RawLocation.state == param["state"])\
                .filter(alchemy.schema.RawLocation.country == param["country"])\
                .first()
            if loc:
                most_freq = len(loc.rawassignees) + len(loc.rawinventors)
                default.update(param)

        # took a look at the frequency of the items in the match_group
        for loc in match_group:
            freq = len(loc.rawassignees) + len(loc.rawinventors)
            if freq > most_freq:
                default.update(loc.summarize)
                most_freq = freq"""
    geo_match(match_group, alchemy_session, default)
    #if (counter + 1) % alchemy_config.get("location").get("commit_frequency") == 0:
    #    print " *", (counter + 1), datetime.datetime.now() - runtime
    #    alchemy_session.commit()

location_insert_statements = []
update_statements = []
def geo_match(objects, session, default):
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None
    class_type = None
    class_type = None
    for obj in objects:
        if not obj: continue
        class_type = obj.__related__
        raw_objects.append(obj)
        break

    for obj in raw_objects:
        for k, v in obj.summarize.iteritems():
            freq[k][v] += 1
        if "id" not in param:
            param["id"] = obj.uuid
        param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]

    param.update(default)
    if '?' in param['city']:
      print param['city']
      #TODO: Fix param city ?????

    location_insert_statements.append(param)
    tmpids = map(lambda x: x.id, objects)
    update_statements.extend([{'pk':x,'update':param['id']} for x in tmpids])


def clean_raw_locations_from_file(inputfilename, outputfilename):
    inputfile = open(inputfilename, 'r')
    outputfile = open(outputfilename, 'w+')
    for line in inputfile:
        line = line.decode('utf8')
        line = geoalchemy_util.clean_raw_location(line)
        line = line.encode('utf8')
        outputfile.write(line)

def analyze_input_addresses(inputfilename):
    valid_input_addresses = construct_valid_input_addresses()
    print datetime.datetime.now()
    inputfile = open(inputfilename, 'r')
    line_count=0
    good_count=0
    exists_in_all_cities_count=0
    #not_found_file = open('not_found.txt', 'w+')
    for line in inputfile:
        line = line.decode('utf8')
        input_address = geoalchemy_util.clean_raw_location(line)
        if input_address_exists(valid_input_addresses, input_address):
            good_count+=1
        #else:
            #not_found_file.write('{0}\n'.format(input_address.encode('utf8')))
        line_count+=1
    print 'All lines compared!'
    print '% good:', good_count*1.0/line_count
    print '% in all_cities:', exists_in_all_cities_count*1.0/line_count
    print datetime.datetime.now()

def construct_valid_input_addresses():
    valid_input_addresses = set()
    temp = geo_data_session.query(RawGoogle.input_address).filter(RawGoogle.confidence>0.1)\
                                .filter((RawGoogle.city!='') | (RawGoogle.region!=''))
    for row in temp:
        input_address = row.input_address
        valid_input_addresses.add(input_address)
    print 'List of all valid Google input_address values constructed with', len(valid_input_addresses), 'items'
    return valid_input_addresses

def input_address_exists(valid_input_addresses, input_address):
    if valid_input_addresses:
        return input_address in valid_input_addresses
    else:
        print 'Error: list of valid input addresses not constructed'
        return False

def find_difficult_locations_from_file(inputfilename, outputfilename):
    inputfile = open(inputfilename, 'r')
    outputfile = open(outputfilename, 'w+')
    t = datetime.datetime.now()
    all_japan_cities_query = geo_data_session.query(AllCities.city).filter(AllCities.country=='JP').group_by(AllCities.city).all()
    all_japan_cities = []
    for row in all_japan_cities_query:
        all_japan_cities.append(row.city)
    print 'list of all_japan_cities created', datetime.datetime.now()-t
    for line in inputfile:
        line = line.decode('utf8')
        line = geoalchemy_util.remove_eol_pattern.sub('', line)
        if line.endswith(', JP') or line.endswith(', JA'):
            city = line.split(',')[0].strip()
            most_similar_city = geoalchemy_util.get_closest_match_leven(city, all_japan_cities, 0.8)
            if most_similar_city!='':
                outputfile.write('{0}|{1}\n'.format(city.encode('utf8'), most_similar_city.encode('utf8')))
    print datetime.datetime.now()-t

if __name__=='__main__':
    global doctype
    doctype='grant'
    main(doctype='grant')
    doctype = 'application'
    main(doctype='application')
