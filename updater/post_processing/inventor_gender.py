from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd

from lib.configuration import get_config, get_connection_string


class InventorGenderGenerator:
    def __init__(self, config):
        self.config = config
        self.new_db = config['DATABASE']['NEW_DB']
        self.old_db = config['DATABASE']['OLD_DB']
        self.old_id_col = 'disamb_inventor_id_{}'.format(self.old_db[-8:])
        self.new_id_col = 'disamb_inventor_id_{}'.format(self.new_db[-8:])
        self.cstr = get_connection_string(config, database='NEW_DB')
        self.id_to_gender = {}
        pass

    def backup_old_inventor_gender(self):
        engine = create_engine(self.cstr)
        rename_sql = 'alter table inventor_gender rename temp_inventor_gender_{0}'.format(self.old_db)
        engine.execute(rename_sql)
        engine.dispose()

    def generate_id_to_gender_mapping(self):
        limit = 300000
        offset = 0
        batch_counter = 0
        engine = create_engine(self.cstr)
        while True:
            connection = engine.connect()
            batch_counter += 1
            print('Next iteration')
            counter = 0

            inv_gender_chunk = connection.execute(
                'select disamb_inventor_id_20170808, male from temp_inventor_gender_{0} order by disamb_inventor_id_20170808 limit {1} offset {2}'.format(
                    self.old_db, limit, offset))

            for row in tqdm(inv_gender_chunk, total=limit,
                            desc="inventor_gender processing - batch:" + str(batch_counter)):
                self.id_to_gender[row[0]] = row[1]
                counter += 1
            connection.close()
            offset += limit
            # means we have no more batches to process
            if counter == 0:
                break
        engine.dispose()

    def map_new_inventor_gender(self):
        results = []
        batch_counter = 0
        limit = 300000
        offset = 0

        processed_ids = set()
        engine = create_engine(self.cstr)
        while True:
            connection = engine.connect()
            batch_counter += 1
            print('Next iteration')
            counter = 0
            pid_chunk = connection.execute(
                'select disamb_inventor_id_20170808, {0}, {1} from persistent_inventor_disambig order by disamb_inventor_id_20170808 limit {2} offset {3}'.format(
                    self.old_id_col, self.new_id_col, limit, offset))
            for row in tqdm(pid_chunk, total=limit,
                            desc="persistent inventor processing - batch:" + str(batch_counter)):
                # row[0] =  disamb_inventor_id_20170808
                # row[1:len(row) - 1] = disamb_inventor_id_201127... previous cols
                # row[len(row) - 1] = most recent db col
                # if 20170808 id has not been seen previously already, add to processed ids list
                if row[0] not in processed_ids:
                    processed_ids.add(row[0])

                    # if 20170808 id exists and it is in the inventor_gender table, we have gender info!
                    if row[0] is not None and row[0] in self.id_to_gender.keys():
                        results.append(
                            [row[0]] + list(row[1:len(row) - 1]) + [row[len(row) - 1], self.id_to_gender[row[0]]])

                counter += 1
            # means we have no more batches to process
            offset += limit
            connection.close()
            if counter == 0:
                break

            offset = offset + limit

        print("now creating .tsv")
        gender_df = pd.DataFrame(results)
        # will always take prior db id and new id with 20170808
        gender_df.columns = ['disamb_inventor_id_20170808', self.old_id_col, self.new_id_col, 'male']

        print("print now inserting into table.....")
        chunk_size_sql = 300000
        gender_df.to_sql(con=engine, name='inventor_gender_{0}'.format(self.new_db), index=False, if_exists='append',
                         chunksize=chunk_size_sql, method='multi')
        # rename table to generic inventor gender
        engine.execute('drop table if exists inventor_gender')
        engine.execute('alter table inventor_gender_{0} rename inventor_gender'.format(self.new_db))

    def generate_inventor_gender(self):
        # self.backup_old_inventor_gender()
        self.generate_id_to_gender_mapping()
        self.map_new_inventor_gender()


if __name__ == '__main__':
    config = get_config()
    ig = InventorGenderGenerator(config)
    ig.generate_inventor_gender()
