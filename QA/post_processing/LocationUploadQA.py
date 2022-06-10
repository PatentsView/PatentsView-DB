import datetime
import pandas as pd
import time
import pymysql.cursors
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

from QA.post_processing.DisambiguationTester import DisambiguationTester
from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_current_config
from lib.configuration import get_connection_string


class LocationUploadTest:
    def __init__(self, config):
        self.config = config

    def character_matches(self):
        print("TESTING FOR STATE AND COUNTRY MISMATCHES BETWEEN RAWLOCATION_ID & CURATED_ID")
        temp_db = 'TEMP_UPLOAD_DB'
        cstr = get_connection_string(self.config, temp_db)
        engine = create_engine(cstr)
        query_dict = {"Country":
            """
select count(*)
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.id
        left join patent.country_codes c on b.`country`=c.`name`
where a.country is not null and a.country <> `alpha-2`;"""
            ,
            "State": """
select count(*)
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.id
        inner join patent.state_codes d on b.state=d.`State/Possession`
        left join patent.country_codes c on b.`country`=c.`name`
where a.country is not null and a.state <> `Abbreviation`;"""
        }
        with engine.connect() as connection:
            for q in query_dict:
                print(query_dict[q])
                rows = connection.execute(query_dict[q])
                print(rows)
                q_rows = rows.first()[0]
                print(q_rows)
                if q_rows > 0:
                    raise Exception(f"{q} MISMATCH for {q_rows}")


    def no_location_id(self):
        print("TESTING FOR MISSING LOCATION_ID")
        temp_db = 'TEMP_UPLOAD_DB'
        cstr = get_connection_string(self.config, temp_db)
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
        cstr = get_connection_string(self.config, temp_db)
        engine = create_engine(cstr)
        with engine.connect() as connection:
            connection.execute(" drop table if exists rawlocation_disambig_compare ;")
            rows = connection.execute(f"""
create table rawlocation_disambig_compare 
select a.id as rawlocation_id, b.id as curated_location_id, a.version_indicator, latitude, longitude,  lat, lon, (latitude-lat) as lat_diff, (longitude-lon) as long_diff
from rawlocation a
        left join geo_data.curated_locations b on a.location_id=b.id
where a.country is not null and b.id is not null;""")
            df = pd.read_sql("select * from rawlocation_disambig_compare", con=connection)

            # Create Histogram of Lat and Long differences between Rawlocation and our Curated Locations table
            latlongdiff = ['lat_diff', 'long_diff']
            for l in latlongdiff:
                f = plt.figure()
                plt.hist(df[l])
                # plt.show()
                f.savefig(f"{l}.pdf", bbox_inches='tight')

            # SEE THE TOP OUTLIERS
            for b in latlongdiff:
                for a in [True, False]:
                    print(df.sort_values(by=b, ascending=a).head(10))

            max_lat_diff = df['lat_diff'].max()
            min_lat_diff = df['lat_diff'].min()
            max_long_diff = df['long_diff'].max()
            min_long_diff = df['long_diff'].min()
            print(f"LAT DIFF RANGE {min_lat_diff}- {max_lat_diff} --/-- LONG DIFF RANGE {min_long_diff}- {max_long_diff}")
            if (max_lat_diff > 5) or (min_lat_diff < -5) or (max_long_diff > 5) or (min_long_diff < -5):
                raise Exception("LAT OR LONG LOOKS WRONG")


    def runTests(self):
        self.character_matches()
        self.no_location_id()
        self.create_lat_long_comparison_table()

if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule='weekly', **{
        "execution_date": datetime.date(2022, 5, 24)
    })
    l = LocationUploadTest
    # l.character_matches(config)
    # l.no_location_id(config)
    l.create_lat_long_comparison_table(config)