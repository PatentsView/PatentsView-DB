import datetime
import pandas as pd
import time
import pymysql.cursors
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from math import radians, cos, sin, asin, sqrt

from QA.post_processing.DisambiguationTester import DisambiguationTester
from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_current_config
from lib.configuration import get_connection_string


class LocationUploadTest:
    def __init__(self, config):
        self.config = config

    def haversince(self, lat1, long1, lat2, long2):
        """
        Calculate the great circle distance in miles between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [long1, lat1, long2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 3956  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r


    def character_matches(self):
        print("TESTING FOR STATE AND COUNTRY MISMATCHES BETWEEN RAWLOCATION_ID & CURATED_ID")
        cstr = get_connection_string(self.config, 'TEMP_UPLOAD_DB')
        engine = create_engine(cstr)
        query_dict = {"Country":
            """
select count(*)
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.uuid
        left join patent.country_codes c on b.`country`=c.`name`
where a.country is not null 
and a.country <> `alpha-2`
and NOT a.qa_override <=> 1;"""
# note use of MySQL null-safe equals operator <=>
# returns 0 rather than NULL for "SELECT NULL <=> 1"
            ,
            "State": """
select count(*)
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.uuid
        inner join patent.state_codes d on b.state=d.`State/Possession`
        left join patent.country_codes c on b.`country`=c.`name`
where a.country is not null 
and a.state <> `Abbreviation`
and NOT a.qa_override <=> 1;"""
        }
        with engine.connect() as connection:
            for q in query_dict:
                print(query_dict[q])
                rows = connection.execute(query_dict[q])
                print(rows)
                q_rows = rows.first()[0]
                print(q_rows)
                if q_rows > 0:
                    raise Exception(f"""
                        {q} MISMATCH for {q_rows} rows. 
                        Please individually review each row in question to determine whether location mapping is incorrect or may be flagged for override
                        """)


    def no_location_id(self):
        print("TESTING FOR MISSING LOCATION_ID")
        cstr = get_connection_string(self.config, 'TEMP_UPLOAD_DB')
        engine = create_engine(cstr)
        # df = pd.read_sql("""select * from rawlocation where location_id is null;""", con=engine)
        with engine.connect() as connection:
            rows = connection.execute(f"""
select count(*)
from rawlocation a
where a.country is not null and location_id is null;""")
            q_rows = rows.first()[0]
            if q_rows > 0:
                # raise Exception(f"{q_rows} rawlocations DO NOT HAVE A LOCATION ID")
                print(f"{q_rows} rawlocations DO NOT HAVE A LOCATION ID -- See Table rawlocation_latlong_unmatched")

    def create_lat_long_comparison_table(self):
        print("TESTING FOR LAT/LONG DIFF BETWEEN RAWLOCATION_ID & CURATED_ID")
        temp_db = 'TEMP_UPLOAD_DB'
        # temp_db = 'PROD_DB'
        cstr = get_connection_string(self.config, temp_db)
        engine = create_engine(cstr)
        start = self.config["DATES"]["START_DATE"]
        end = self.config["DATES"]["END_DATE"]
        with engine.connect() as connection:
            connection.execute(" drop table if exists rawlocation_disambig_compare ;")
            hav_list = []
            df = pd.read_sql(f"""
select a.id as rawlocation_id
    , b.uuid as curated_location_id
    , a.version_indicator
    , latitude
    , longitude
    , lat
    , lon
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.uuid
where a.country is not null 
    and b.uuid is not null
    and a.version_indicator >= {start} 
    and a.version_indicator <= {end};""", con=connection)
            for index, row in df.iterrows():
                i = self.haversince(row['lat'], row['lon'], row['latitude'], row['longitude'])
                hav_list.append(i)
            df['haversince_distance'] = hav_list
            df.to_sql('rawlocation_disambig_compare', engine, if_exists='append', index=False)

            # Create Histogram of Lat and Long differences between Rawlocation and our Curated Locations table
            l = ['haversince_distance']
            f = plt.figure()
            plt.hist(df[l])
            # plt.show()
            f.savefig(f"{l[0]}.pdf", bbox_inches='tight')

            # SEE THE TOP OUTLIERS
            for a in [True, False]:
                print(df.sort_values(by=l, ascending=a).head(10))

            max_hav_dist = df['haversince_distance'].max()
            min_hav_dist = df['haversince_distance'].min()
            print(f"MIN HAVERSINE DISTANCE: {min_hav_dist} // MAX HAVERSINE DISTANCE: {max_hav_dist}")
            if (min_hav_dist < 0) or (max_hav_dist > 250):
                print("WARNING LAT/LONG LOOKS WRONG")
                # raise Exception("LAT OR LONG LOOKS WRONG")

    def percent_location_id_null(self):
        cstr = get_connection_string(self.config, 'TEMP_UPLOAD_DB')
        engine = create_engine(cstr)
        with engine.connect() as connection:
            query = connection.execute(f"""
select (sum(case when location_id is null then 1 else 0 end)/ count(*))
from rawlocation;""")
            q_rows = query.first()[0]
            print(f"""{q_rows} % NULL LOCATION_ID THIS WEEK """)
            if q_rows > .05:
                raise Exception(f"{q_rows} % of NULL LOCATION IDs -- TOO HIGH")

    def check_disambig_mapping_updated(self):
        # from lib.is_it_update_time import get_update_range
        # sd = datetime.datetime.strptime(self.config['DATES']['START_DATE'], '%Y%m%d')
        # q_start_date, q_end_date = get_update_range(sd + datetime.timedelta(days=6))
        # end_of_quarter = q_end_date.strftime('%Y%m%d')

        cstr = get_connection_string(self.config, 'TEMP_UPLOAD_DB')
        engine = create_engine(cstr)
        with engine.connect() as connection:
            query = connection.execute(f"""
select count(*)
from rawlocation a 
	inner join location_disambiguation_mapping b on a.id=b.id""")
            q_rows = query.first()[0]
            print(f"""{q_rows} Updated In the Weekly Location Disambiguation Table""")
            if q_rows == 0:
                raise Exception(f"LOCATION DISAMBIGUATION MAPPING NOT UPDATED FOR THE CURRENT WEEK")

    def runTests(self):
        self.character_matches()
        self.no_location_id()
        self.create_lat_long_comparison_table()
        self.percent_location_id_null()
        self.check_disambig_mapping_updated()


if __name__ == '__main__':
    # d = datetime.date(2022, 6, 28)
    # while d <= datetime.date(2022, 10, 1):
    #     print("----------------------------------------------------")
    #     print(f"STARTING FOR DATE: {d}")
    #     print("----------------------------------------------------")
    #     config = get_current_config('granted_patent', schedule='weekly', **{
    #         "execution_date": d
    #     })
    d = datetime.date(2022, 7, 5)
    config = get_current_config('granted_patent', schedule='quarterly', **{
        "execution_date": d
    })
    l = LocationUploadTest(config)
    l.create_lat_long_comparison_table()

