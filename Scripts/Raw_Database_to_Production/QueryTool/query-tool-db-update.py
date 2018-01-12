
import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
import os
print(os.environ['PV_PROD_HOST'])

host = os.environ['PV_INJ_HOST']
user = os.environ['PV_INJ_USERNAME']
password = os.environ['PV_INJ_PASSWORD']
port = os.environ['PV_INJ_PORT']

# In[ ]:

freshly_minted_reporting_db = 'PatentsView_20170808'
querytool_db = "QueryTool"


# In[ ]:

write_engine = create_engine(
    'mysql://' +
    str(user) + ':' + str(password) + '@' +
    str(host) + ':' + str(port) + '/' + str(querytool_db),
    echo=False)


# In[ ]:

read_engine = create_engine(
    'mysql://' +
    str(user) + ':' + str(password) + '@' +
    str(host) + ':' + str(port) + '/' + str(freshly_minted_reporting_db),
    echo=False)


# In[ ]:

ipc_statement = 'insert into new_ipc_lookup select section,ipc_class,subclass,main_group,subgroup from ' + str(
    freshly_minted_reporting_db
) + '.ipcr group by section,ipc_class,subclass,main_group,subgroup'
gov_org_statement = 'insert into new_gov_org select organization_id,name,level_one,level_two,level_three from ' + str(
    freshly_minted_reporting_db
) + '.government_organization group by organization_id,name,level_one,level_two,level_three'
assignee_org_statement = 'insert into new_assignee_org select distinct organization from ' + str(
    freshly_minted_reporting_db) + '.assignee'
cpc_subgroup_statement = 'insert into new_cpcsubgroup select distinct id, title from ' + str(
    freshly_minted_reporting_db) + '.cpc_subgroup'
with write_engine.connect() as con:
    con.execute(ipc_statement)
    con.execute(gov_org_statement)
    con.execute(assignee_org_statement)
    con.execute(cpc_subgroup_statement)


# In[ ]:

uspc_current = pd.read_sql(
    'select distinct subclass_id from uspc_current;', con=read_engine)
print('loaded dataframe from MySQL. records:', len(uspc_current))
uspc_current_new = usps_current.join(
    uspc_current.apply(lambda x: pd.Series(x.subclass_id.split("/")), 1))
uspc_current_new.columns = ["main_string", "mainclass_id", "subclass_id"]
uspc_current_to_write = uspc_current_new[["mainclass_id", "subclass_id"]]
uspc_current_to_write.to_sql(
    'new_uspc_lookup', con=engine, if_exists='append', index=False)


# In[1]:


# In[ ]:

cpc_current = pd.read_sql(
    'select distinct subgroup_id from cpc_current;', , con=read_engine)
print('loaded dataframe from MySQL. records:', len(cpc_current))
cpc_current_to_write = cpc_current.apply(lambda x: pd.Series(
    [x.subgroup_id[0], x.subgroup_id[1:3], x.subgroup_id[3], x.subgroup_id[4:]]), 1)
cpc_current_to_write.columns = [
    "section_id", "subsection_id", "group_id", "subgroup_id"
]
cpc_current_to_write.to_sql(
    'new_cpc_lookup', con=write_engine, if_exists='append', index=False)


# In[ ]:

country_dict = {
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

locations_current = pd.read_sql(
    'select distinct city,state,country from location;', con=readdb)
print('loaded dataframe from MySQL. records:', len(locations_current))


def location_matchup(x, country_dict):
    return pd.Series([
        x.city.replace('"', '') if x.city is not None else '', ''
        if x.country != 'US' else x.state, x.country, country_dict[x.country]
        if x.country in country_dict.keys() else ''
    ])

locations_new = locations_current.apply(
    location_matchup, 1, args=(country_dict,))
locations_new.columns = ["city", "stateShort", "countryCode", "countryName"]

locations_new.to_sql(
    'new_locations', con=write_engine, if_exists='append', index=False)


# In[ ]:


# In[ ]:

lookup_current = pd.read_sql(
    'select distinct city,state,country from location;', con=readdb)
print('loaded dataframe from MySQL. records:', len(lookup_current))


# In[ ]:

def location_matchup(x, country_dict):
    return pd.Series([
        x.city.replace('"', '') if x.city is not None else '', ''
        if x.country != 'US' else x.state, x.country, country_dict[x.country]
        if x.country in country_dict.keys() else ''
    ])


# In[ ]:

lookup_new = lookup_current.apply(location_matchup, 1, args=(country_dict,))
lookup_new.columns = ["city", "stateShort", "countryCode", "countryName"]


# In[ ]:

lookup_new.to_sql(
    'new_locations', con=write_engine, if_exists='append', index=False)
