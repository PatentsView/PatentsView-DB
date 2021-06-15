import datetime
import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

from QA.post_processing.LocationPostProcessing import LocationPostProcessingQC
from lib.configuration import get_connection_string, get_current_config
from updater.post_processing.create_lookup import load_lookup_table


class LocationPostProcessor():
    def __init__(self, config):
        self.state_dict = {
                'AL': 'Alabama',
                'AK': 'Alaska',
                'AS': 'American Samoa',
                'AZ': 'Arizona',
                'AR': 'Arkansas',
                'CA': 'California',
                'CO': 'Colorado',
                'CT': 'Connecticut',
                'DE': 'Delaware',
                'DC': 'District of Columbia',
                'FM': 'Federated States of Micronesia',
                'FL': 'Florida',
                'GA': 'Georgia',
                'GU': 'Guam',
                'HI': 'Hawaii',
                'ID': 'Idaho',
                'IL': 'Illinois',
                'IN': 'Indiana',
                'IA': 'Iowa',
                'KS': 'Kansas',
                'KY': 'Kentucky',
                'LA': 'Louisiana',
                'ME': 'Maine',
                'MH': 'Marshall Islands',
                'MD': 'Maryland',
                'MA': 'Massachusetts',
                'MI': 'Michigan',
                'MN': 'Minnesota',
                'MS': 'Mississippi',
                'MO': 'Missouri',
                'MT': 'Montana',
                'NE': 'Nebraska',
                'NV': 'Nevada',
                'NH': 'New Hampshire',
                'NJ': 'New Jersey',
                'NM': 'New Mexico',
                'NY': 'New York',
                'NC': 'North Carolina',
                'ND': 'North Dakota',
                'MP': 'Northern Mariana Islands',
                'OH': 'Ohio',
                'OK': 'Oklahoma',
                'OR': 'Oregon',
                'PW': 'Palau',
                'PA': 'Pennsylvania',
                'PR': 'Puerto Rico',
                'RI': 'Rhode Island',
                'SC': 'South Carolina',
                'SD': 'South Dakota',
                'TN': 'Tennessee',
                'TX': 'Texas',
                'UT': 'Utah',
                'VT': 'Vermont',
                'VI': 'Virgin Islands',
                'VA': 'Virginia',
                'WA': 'Washington',
                'WV': 'West Virginia',
                'WI': 'Wisconsin',
                'WY': 'Wyoming'
                }
        self.country_dict = {
                'AD': 'Andorra',
                'AE': 'United Arab Emirates',
                'AF': 'Afghanistan',
                'AG': 'Antigua and Barbuda',
                'AI': 'Anguilla',
                'AL': 'Albania',
                'AM': 'Armenia',
                'AN': 'Netherlands Antilles',
                'AO': 'Angola',
                'AQ': 'Antarctica',
                'AR': 'Argentina',
                'AS': 'American Samoa',
                'AT': 'Austria',
                'AU': 'Australia',
                'AW': 'Aruba',
                'AZ': 'Azerbaijan',
                'BA': 'Bosnia and Herzegovina',
                'BB': 'Barbados',
                'BD': 'Bangladesh',
                'BE': 'Belgium',
                'BF': 'Burkina Faso',
                'BG': 'Bulgaria',
                'BH': 'Bahrain',
                'BI': 'Burundi',
                'BJ': 'Benin',
                'BM': 'Bermuda',
                'BN': 'Brunei',
                'BO': 'Bolivia',
                'BR': 'Brazil',
                'BS': 'Bahamas',
                'BT': 'Bhutan',
                'BV': 'Bouvet Island',
                'BW': 'Botswana',
                'BY': 'Belarus',
                'BZ': 'Belize',
                'CA': 'Canada',
                'CC': 'Cocos [Keeling] Islands',
                'CD': 'Congo [DRC]',
                'CF': 'Central African Republic',
                'CG': 'Congo [Republic]',
                'CH': 'Switzerland',
                'CI': 'Côte d\'Ivoire',
                'CK': 'Cook Islands',
                'CL': 'Chile',
                'CM': 'Cameroon',
                'CN': 'China',
                'CO': 'Colombia',
                'CR': 'Costa Rica',
                'CU': 'Cuba',
                'CV': 'Cape Verde',
                'CX': 'Christmas Island',
                'CY': 'Cyprus',
                'CZ': 'Czech Republic',
                'DE': 'Germany',
                'DJ': 'Djibouti',
                'DK': 'Denmark',
                'DM': 'Dominica',
                'DO': 'Dominican Republic',
                'DZ': 'Algeria',
                'EC': 'Ecuador',
                'EE': 'Estonia',
                'EG': 'Egypt',
                'EH': 'Western Sahara',
                'ER': 'Eritrea',
                'ES': 'Spain',
                'ET': 'Ethiopia',
                'FI': 'Finland',
                'FJ': 'Fiji',
                'FK': 'Falkland Islands [Islas Malvinas]',
                'FM': 'Micronesia',
                'FO': 'Faroe Islands',
                'FR': 'France',
                'GA': 'Gabon',
                'GB': 'United Kingdom',
                'GD': 'Grenada',
                'GE': 'Georgia',
                'GF': 'French Guiana',
                'GG': 'Guernsey',
                'GH': 'Ghana',
                'GI': 'Gibraltar',
                'GL': 'Greenland',
                'GM': 'Gambia',
                'GN': 'Guinea',
                'GP': 'Guadeloupe',
                'GQ': 'Equatorial Guinea',
                'GR': 'Greece',
                'GS': 'South Georgia and the South Sandwich Islands',
                'GT': 'Guatemala',
                'GU': 'Guam',
                'GW': 'Guinea-Bissau',
                'GY': 'Guyana',
                'GZ': 'Gaza Strip',
                'HK': 'Hong Kong',
                'HM': 'Heard Island and McDonald Islands',
                'HN': 'Honduras',
                'HR': 'Croatia',
                'HT': 'Haiti',
                'HU': 'Hungary',
                'ID': 'Indonesia',
                'IE': 'Ireland',
                'IL': 'Israel',
                'IM': 'Isle of Man',
                'IN': 'India',
                'IO': 'British Indian Ocean Territory',
                'IQ': 'Iraq',
                'IR': 'Iran',
                'IS': 'Iceland',
                'IT': 'Italy',
                'JE': 'Jersey',
                'JM': 'Jamaica',
                'JO': 'Jordan',
                'JP': 'Japan',
                'KE': 'Kenya',
                'KG': 'Kyrgyzstan',
                'KH': 'Cambodia',
                'KI': 'Kiribati',
                'KM': 'Comoros',
                'KN': 'Saint Kitts and Nevis',
                'KP': 'North Korea',
                'KR': 'South Korea',
                'KW': 'Kuwait',
                'KY': 'Cayman Islands',
                'KZ': 'Kazakhstan',
                'LA': 'Laos',
                'LB': 'Lebanon',
                'LC': 'Saint Lucia',
                'LI': 'Liechtenstein',
                'LK': 'Sri Lanka',
                'LR': 'Liberia',
                'LS': 'Lesotho',
                'LT': 'Lithuania',
                'LU': 'Luxembourg',
                'LV': 'Latvia',
                'LY': 'Libya',
                'MA': 'Morocco',
                'MC': 'Monaco',
                'MD': 'Moldova',
                'ME': 'Montenegro',
                'MG': 'Madagascar',
                'MH': 'Marshall Islands',
                'MK': 'Macedonia [FYROM]',
                'ML': 'Mali',
                'MM': 'Myanmar [Burma]',
                'MN': 'Mongolia',
                'MO': 'Macau',
                'MP': 'Northern Mariana Islands',
                'MQ': 'Martinique',
                'MR': 'Mauritania',
                'MS': 'Montserrat',
                'MT': 'Malta',
                'MU': 'Mauritius',
                'MV': 'Maldives',
                'MW': 'Malawi',
                'MX': 'Mexico',
                'MY': 'Malaysia',
                'MZ': 'Mozambique',
                'NA': 'Namibia',
                'NC': 'New Caledonia',
                'NE': 'Niger',
                'NF': 'Norfolk Island',
                'NG': 'Nigeria',
                'NI': 'Nicaragua',
                'NL': 'Netherlands',
                'NO': 'Norway',
                'NP': 'Nepal',
                'NR': 'Nauru',
                'NU': 'Niue',
                'NZ': 'New Zealand',
                'OM': 'Oman',
                'PA': 'Panama',
                'PE': 'Peru',
                'PF': 'French Polynesia',
                'PG': 'Papua New Guinea',
                'PH': 'Philippines',
                'PK': 'Pakistan',
                'PL': 'Poland',
                'PM': 'Saint Pierre and Miquelon',
                'PN': 'Pitcairn Islands',
                'PR': 'Puerto Rico',
                'PS': 'Palestinian Territories',
                'PT': 'Portugal',
                'PW': 'Palau',
                'PY': 'Paraguay',
                'QA': 'Qatar',
                'RE': 'Réunion',
                'RO': 'Romania',
                'RS': 'Serbia',
                'RU': 'Russia',
                'RW': 'Rwanda',
                'SA': 'Saudi Arabia',
                'SB': 'Solomon Islands',
                'SC': 'Seychelles',
                'SD': 'Sudan',
                'SE': 'Sweden',
                'SG': 'Singapore',
                'SH': 'Saint Helena',
                'SI': 'Slovenia',
                'SJ': 'Svalbard and Jan Mayen',
                'SK': 'Slovakia',
                'SL': 'Sierra Leone',
                'SM': 'San Marino',
                'SN': 'Senegal',
                'SO': 'Somalia',
                'SR': 'Suriname',
                'ST': 'São Tomé and Príncipe',
                'SV': 'El Salvador',
                'SY': 'Syria',
                'SZ': 'Swaziland',
                'TC': 'Turks and Caicos Islands',
                'TD': 'Chad',
                'TF': 'French Southern Territories',
                'TG': 'Togo',
                'TH': 'Thailand',
                'TJ': 'Tajikistan',
                'TK': 'Tokelau',
                'TL': 'Timor-Leste',
                'TM': 'Turkmenistan',
                'TN': 'Tunisia',
                'TO': 'Tonga',
                'TR': 'Turkey',
                'TT': 'Trinidad and Tobago',
                'TV': 'Tuvalu',
                'TW': 'Taiwan',
                'TZ': 'Tanzania',
                'UA': 'Ukraine',
                'UG': 'Uganda',
                'UM': 'U.S. Minor Outlying Islands',
                'US': 'United States',
                'UY': 'Uruguay',
                'UZ': 'Uzbekistan',
                'VA': 'Vatican City',
                'VC': 'Saint Vincent and the Grenadines',
                'VE': 'Venezuela',
                'VG': 'British Virgin Islands',
                'VI': 'U.S. Virgin Islands',
                'VN': 'Vietnam',
                'VU': 'Vanuatu',
                'WF': 'Wallis and Futuna',
                'WS': 'Samoa',
                'XK': 'Kosovo',
                'YE': 'Yemen',
                'YT': 'Mayotte',
                'ZA': 'South Africa',
                'ZM': 'Zambia',
                'ZW': 'Zimbabwe',
                'SU': 'Soviet Union',
                'YU': 'Yugoslavia',
                'CW': 'Curacao',
                'AX': 'Aland Islands',
                'BQ': 'Bonaire, Sint Eustatius and Saba',
                'BL': 'Saint Barthelemy',
                'SX': 'Sint Maarten',
                'SS': 'South Sudan',
                'MF': 'Saint Martin'
                }
        self.config = config
        self.create_fips_lookups()

    def create_fips_lookups(self):
        persistent_files = self.config['FOLDERS']['PERSISTENT_FILES']
        state_lookup = pd.read_csv('{}/state_fips.csv'.format(persistent_files), dtype=object)
        # TODO: maybe just store this as a json file that directly becomes a dict?
        county_lookup = pd.read_csv('{}/county_lookup.csv'.format(persistent_files))
        self.fips_dict = state_lookup.set_index('State').to_dict(orient='index')
        for state in self.fips_dict:
            self.fips_dict[state]['counties'] = county_lookup.loc[
                county_lookup.state == state, ["county", "county_fips"]].drop_duplicates().set_index('county').to_dict(
                    orient='index')
            self.fips_dict[state]['cities'] = county_lookup.loc[county_lookup.state == state,
                                                                ["city", "county"]].set_index(
                    'city').to_dict(orient='index')

    def find_county(self, row):
        city = clean_loc(row['city'])
        state = clean_loc(row['state'])
        country = clean_loc(row['country'])
        state_fips = None
        county = None
        county_fips = None
        if country == 'US' and state in self.fips_dict:
            state_settings = self.fips_dict[state]
            state_fips = state_settings['State_FIPS']
            # fips_dict['DC']['cities']['Washington']
            # {'county': 'District of Columbia'}
            if city in state_settings['cities']:
                county = state_settings['cities'][city]['county']
                # fips_dict['DC']['counties']
                # {'District of Columbia': {'county_fips': 11001}}
                county_fips = state_settings['counties'][county]['county_fips']

        result_dict = {
                'found_county':      county,
                'found_county_fips': county_fips,
                'found_state_fips':  state_fips
                }

        return pd.Series(result_dict)

    def build_location_query(self, city, state, country):
        final_q = None
        full_q = "Select name, state, country_code, lat, lon from geo_data.places gl WHERE "
        city_q = "MATCH(gl.name) AGAINST('{city}')"
        state_q = "'{state}' = gl.state"
        country_q = "gl.country_code = '{country}'"

        added = False

        if city is None and state is None and country is None:
            return None

        else:
            if city is not None:
                final_q = full_q.format(score_select=city) + city_q.format(city=city)
                added = True
            if state is not None and state in self.state_dict:
                if added:
                    final_q = final_q + " and " + state_q.format(state=self.state_dict[state])
                else:
                    final_q = full_q.format(score_select=self.state_dict[state]) + state_q.format(
                            state=self.state_dict[state])
                    added = True
            if country is not None and country in self.country_dict:
                if added:
                    final_q = final_q + " and " + country_q.format(country=country)
                else:
                    full_q = full_q.format(score_select=self.country_dict[country]) + country_q.format(country=country)
                    added = True
        return final_q

    def run_query(self, row):
        lat = ''
        lon = ''
        city = clean_loc(row['city'])
        state = clean_loc(row['state'])
        country = clean_loc(row['country'])
        engine = create_engine(get_connection_string(config, "RAW_DB"))
        query = self.build_location_query(city, state, country)
        if query is not None:
            results = pd.read_sql_query(sql=query, con=engine)
            if len(results) > 0:
                found_name = results.loc[0]['name']
                found_state = results.loc[0]['state']
                found_country = results.loc[0]['country_code']
                lat = results.loc[0]['lat']
                lon = results.loc[0]['lon']
                result_dict = {
                        'found_name':    found_name,
                        'found_state':   found_state,
                        'found_country': found_country,
                        'lat':           lat,
                        'lon':           lon,
                        'query':         query
                        }
            else:
                result_dict = {
                        'found_name':    None,
                        'found_state':   None,
                        'found_country': None,
                        'lat':           None,
                        'long':          None,
                        'query':         query
                        }
        else:
            result_dict = {
                    'found_name':    None,
                    'found_state':   None,
                    'found_country': None,
                    'lat':           None,
                    'lon':           None,
                    'query':         query
                    }

        return pd.Series(result_dict)


def clean_loc(loc):
    if loc is not None:
        cleaned_loc = loc.replace("'", "''")
    else:
        cleaned_loc = None
    return cleaned_loc


def lookup_fips(city, state, country, lookup_dict):
    result = None
    if lookup_type == 'city':
        if country == 'US' and (state, city) in lookup_dict:
            result = lookup_dict[(state, city)]
    elif country == 'US' and state in lookup_dict:
        result = lookup_dict[state]
    return result


def update_rawlocation(update_config, database='RAW_DB', uuid_field='id'):
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = """
        UPDATE rawlocation rl left join location_disambiguation_mapping ldm
            on ldm.uuid = rl.{uuid_field}
        set rl.location_id = ldm.location_id
    """.format(uuid_field=uuid_field, granted_db=update_config['PATENTSVIEW_DATABASES']['RAW_DB'])
    print(update_statement)
    engine.execute(update_statement)


def precache_locations(config):
    location_cache_query = """
        INSERT IGNORE INTO disambiguated_location_ids (location_id)
        SELECT distinct location_id 
        from {granted_db}.rawlocation 
        where location_id is not null
        UNION
        SELECT distinct location_id 
        from {pregrant_db}.rawlocation
        where location_id is not null;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    print(location_cache_query)
    engine.execute(location_cache_query)


def generate_disambiguated_locations(engine, limit, offset):
    location_core_template = """
        SELECT location_id
        FROM disambiguated_location_ids order by location_id
        LIMIT {limit} OFFSET {offset}
    """

    location_data_template = """
        SELECT rl.location_id, rl.city, rl.state, rl.country_transformed as country, rl.location_id_transformed
        FROM rawlocation rl
            JOIN ({loc_core_query}) location on location.location_id = rl.location_id
        WHERE rl.location_id is not null
        UNION ALL
        SELECT rl2.location_id, rl2.city, rl2.state, rl2.country_transformed as country,rl2.location_id_transformed
            FROM pregrant_publications.rawlocation rl2
                JOIN ({loc_core_query}) location on location.location_id = rl2.location_id
            WHERE rl2.location_id is not null
    """

    location_core_query = location_core_template.format(limit=limit,
                                                        offset=offset)
    location_data_query = location_data_template.format(
            loc_core_query=location_core_query)

    current_location_data = pd.read_sql_query(sql=location_data_query, con=engine)
    return current_location_data


def create_date_table(engine):
    date_query = """
        SELECT ml.location_id, ml.city, ml.state, ml.country, ml.location_count, p.date
        from max_location_counts ml
                left join location_disambiguation_mapping ldm on ldm.location_id = ml.location_id
                left join rawassignee ra on ra.rawlocation_id = ldm.uuid
                left join patent p on p.id = ra.patent_id
        UNION ALL
        SELECT ml2.location_id, ml2.city, ml2.state, ml2.country, ml2.location_count, p2.date
        from max_location_counts ml2
                left join location_disambiguation_mapping ldm2 on ldm2.location_id = ml2.location_id
                left join rawinventor ri on ri.rawlocation_id = ldm2.uuid
                left join patent p2 on p2.id = ri.patent_id
        UNION ALL
        SELECT ml3.location_id, ml3.city, ml3.state, ml3.country, ml3.location_count, a.date
        from max_location_counts ml3
                left join pregrant_publications.location_disambiguation_mapping ldm3 on ldm3.location_id = ml3.location_id
                left join pregrant_publications.rawinventor pri on pri.rawlocation_id = ldm3.uuid
                left join pregrant_publications.application a on a.document_number = pri.document_number
                UNION ALL
        SELECT ml4.location_id, ml4.city, ml4.state, ml4.country, ml4.location_count, a.date
        from max_location_counts ml4
                left join pregrant_publications.location_disambiguation_mapping ldm4 on ldm4.location_id = ml4.location_id
                left join pregrant_publications.rawassignee pra on pra.rawlocation_id = ldm4.uuid
                left join pregrant_publications.application a on a.document_number = pra.document_number
    """
    location_date_df = pd.read_sql_query(sql=date_query, con=engine)
    return location_date_df


def truncate_max_locs(engine):
    truncate_query = """
    DELETE FROM max_location_counts;
    """
    with engine.connect() as connection:
        connection.execute(truncate_query)
        print('max_location_counts truncated')


def location_reduce(location_data, update_config, engine):
    truncate_max_locs(engine)
    location_data = location_data.groupby(
            ['location_id', 'city', 'state', 'country', 'location_id_transformed'],
            dropna=False).size().reset_index().groupby(
            ['location_id', 'city', 'state', 'country', 'location_id_transformed'], dropna=False)[[0]].max()
    location_data = location_data.reset_index()
    location_data = location_data.rename(columns={
            0: "location_count"
            })
    location_data['max_count'] = location_data.groupby('location_id')['location_count'].transform('max')
    max_df = location_data[location_data['location_count'] == location_data['max_count']].drop(['max_count'], axis=1)
    one_max = max_df.drop_duplicates(subset='location_id', keep=False)
    max_df[max_df.duplicated('location_id', keep=False)].to_sql('max_location_counts', con=engine, if_exists='append',
                                                                index=False)

    date_df = create_date_table(engine)

    location_date_df = date_df.sort_values(by=['location_id', 'date'], ascending=False).drop_duplicates(
            'location_id').sort_index()

    final_loc = one_max.append(location_date_df)
    final_loc = final_loc.join(pd.Series(np.where(
            pd.isnull(final_loc.location_id_transformed), None,
            final_loc.location_id_transformed.astype(str))).str.split("|", expand=True).rename(
            {
                    0: 'latitude',
                    1: 'longitude'
                    },
            axis=1))

    final_loc = final_loc.loc[:,
                final_loc.columns.isin(['location_id', 'city', 'state', 'country', 'latitude', 'longitude'])]
    final_loc = final_loc.rename(columns={
            'location_id': 'id'
            })
    final_loc = final_loc.reset_index(drop=True)
    return final_loc


def create_location(update_config, version_indicator):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_location_data = generate_disambiguated_locations(engine, limit, offset)
        print(current_location_data.shape[0])
        if current_location_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        step_time = time.time() - start
        canonical_assignments = location_reduce(current_location_data, update_config, engine)
        canonical_assignments = canonical_assignments.assign(version_indicator=version_indicator)
        canonical_assignments.to_sql(name='location', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset

    truncate_max_locs(engine)


def update_lat_lon(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    query = "update location l Join location_lat_lon lll set l.latitude = lll.latitude, l.longitude = lll.longitude " \
            "where l.`id` = lll.`id`"
    engine.execute(query)


def update_county_info(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    query = """
    update location l Join location_fips lf
    set l.county      = lf.county,
        l.county_fips = lf.county_fips,
        l.state_fips=lf.state_fips
    where l.`id` = lf.`id`
    """
    engine.execute(query)


def update_location_lat_lon(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    location_query = "select * from location"
    location_df = pd.read_sql_query(sql=location_query, con=engine)
    lp = LocationPostProcessor(config)
    tqdm.pandas()
    df = location_df.join(location_df.progress_apply(lp.run_query, axis=1))
    df2 = df[['id', 'lat', 'lon']]
    df2 = df2.rename(columns={
            'lat': 'latitude',
            'lon': 'longitude'
            })
    df2.to_sql("location_lat_lon", engine, if_exists="replace", index=False)
    update_lat_lon(config)


def update_fips(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    location_query = "select * from location"
    location_df = pd.read_sql_query(sql=location_query, con=engine)

    tqdm.pandas()
    lp = LocationPostProcessor(config)
    df = location_df.join(location_df.progress_apply(lp.find_county, axis=1))
    df2 = df[['id', 'found_county', 'found_county_fips', 'found_state_fips']]
    df2 = df2.rename(columns={
            'found_county':      'county',
            'found_county_fips': 'county_fips',
            'found_state_fips':  'state_fips'
            })
    df2.to_sql("location_fips", engine, if_exists="replace", index=False)
    update_county_info(config)


def post_process_location(**kwargs):
    config = get_current_config(**kwargs)
    version_indicator = config['DATES']['END_DATE']
    update_rawlocation(config)
    update_rawlocation(config, database='PGPUBS_DATABASE')
    precache_locations(config)
    create_location(config)
    # update_location_lat_lon(config)
    update_fips(config)
    load_lookup_table(update_config=config, database='RAW_DB', parent_entity='location',
                      parent_entity_id=None, entity='assignee', include_location=True,
                      location_strict=True, version_indicator=version_indicator)
    load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='location',
                      parent_entity_id=None, entity="inventor", include_location=True,
                      location_strict=True, version_indicator=version_indicator)


def post_process_qc(**kwargs):
    config = get_current_config(**kwargs)
    qc = LocationPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    post_process_location(**{
            "execution_date": datetime.date(2021, 3, 23)
            })
    post_process_qc(**{
            "execution_date": datetime.date(2021, 3, 23)
            })
