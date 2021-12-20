#!/usr/bin/env python
# coding: utf-8


import csv
import datetime
import logging
import multiprocessing as mp
import os
import pprint
import time

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree, DistanceMetric
from sqlalchemy import create_engine

from lib.configuration import get_connection_string


def get_search_dataset(cstr, start_date, end_date):
    engine = create_engine(cstr)
    search_dataset_sql = """
    SELECT id, location_id_transformed from pregrant_publications.rawlocation where location_id_transformed is not null and location_id is 
    null and location_id_transformed !='undisambiguated' and version_indicator between '{start_dt}' and '{end_dt}' 
    """.format(start_dt=start_date, end_dt=end_date)
    search_dataset = pd.read_sql_query(sql=search_dataset_sql, con=engine)
    augmented_search_dataset = search_dataset.join(
        search_dataset.location_id_transformed.str.split("|", expand=True).rename(
            {
                0: 'lat',
                1: 'long'
            }, axis=1))

    augmented_search_dataset = augmented_search_dataset.assign(
        lattitude=augmented_search_dataset.lat.astype(np.float64))

    augmented_search_dataset = augmented_search_dataset.assign(
        longitude=augmented_search_dataset.long.astype(np.float64))

    return augmented_search_dataset


def build_search_tree(augmented_authority):
    hvdm = DistanceMetric.get_metric('haversine')
    bt = BallTree(np.asmatrix(augmented_authority[["lat", "long"]]), metric=hvdm, verbose=1)
    return bt


def mp_csv_writer(write_queue, target_file, header):
    with open(target_file, 'w', newline='') as writefile:
        filtered_writer = csv.writer(writefile,
                                     delimiter=',',
                                     quotechar='"',
                                     quoting=csv.QUOTE_NONNUMERIC)
        filtered_writer.writerow(header)
        while 1:
            message_data = write_queue.get()
            if len(message_data) != len(header):
                # "kill" is the special message to stop listening for messages
                if message_data[0] == 'kill':
                    break
                else:
                    raise Exception("Header and data length don't match")
            filtered_writer.writerow(message_data)


def search_for_nearest_neighbor(search_chunk, augmented_authority, balltree, csv_write_queue):
    search_data = np.asmatrix(search_chunk[["lat", "long"]])
    search_results = balltree.query(search_data, dualtree=True)
    scores = [x[0] for x in search_results[0]]
    lidxes = [x[0] for x in search_results[1]]
    location_ids = augmented_authority.iloc[lidxes, :].location_id.tolist()
    search_chunk = search_chunk.assign(location_id=location_ids, score=scores)
    for record in search_chunk[["id", "location_id", "score"]].to_numpy().tolist():
        csv_write_queue.put(record)


def queue_searchers(augmented_search_dataset, augmented_authority, bt, output):
    n = 10000
    header = ["id", "location_id", "score"]
    # must use Manager queue here, or will not work
    manager = mp.Manager()
    write_queue = manager.Queue()
    pool = mp.Pool()
    writer = pool.apply_async(mp_csv_writer, (
        write_queue,
        output,
        header))
    p_structure = {}
    for i in range(0, augmented_search_dataset.shape[0], n):
        search_chunk = augmented_search_dataset[i:i + n]
        p = pool.apply_async(search_for_nearest_neighbor,
                             (search_chunk, augmented_authority, bt, write_queue))
        p_structure[i] = {
            'process': p,
            'start_time': time.time(),
            'status': False
        }

    while True:
        if all([p_structure[x]['status'] for x in p_structure]):
            pprint.pprint({
                "level": None,
                "message": "kill"
            })
            write_queue.put(["kill"])
            break
        for p_filename in p_structure:
            if not p_structure[p_filename]['status']:
                process = p_structure[p_filename]['process']
                try:
                    process.get(timeout=1)
                except mp.TimeoutError:
                    continue
                p_structure[p_filename]['status'] = True
                p_structure[p_filename]['duration'] = time.time(
                ) - p_structure[p_filename]['start_time']
                pprint.pprint({
                    "level":
                        logging.INFO,
                    "message":
                        "{f} processed in {duration}".format(
                            duration=p_structure[p_filename]['duration'], f=p_filename)
                })
        time.sleep(10)
    writer.get()
    pool.close()
    pool.join()


def get_authority_dataset(cstr):
    engine = create_engine(cstr)
    authority = pd.read_sql_table(con=engine, table_name="location_by_lat_long")
    augmented_authority = authority.join(
        authority.location_id_transformed.str.split("|", expand=True).rename(
            {
                0: 'lat',
                1: 'long'
            }, axis=1))
    print(augmented_authority.head())
    augmented_authority = augmented_authority.assign(lattitude=augmented_authority.lat.astype(np.float64))
    augmented_authority = augmented_authority.assign(longitude=augmented_authority.long.astype(np.float64))
    return augmented_authority


def load_nearest_neighbor(config, src, output, cstr):
    suffix = datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y-%m-%d").strftime("%Y-%m-%d")
    engine = create_engine(cstr)
    target_table = "nearest_neighbor_results_{src}_{suffix}".format(suffix=suffix, src=src)
    nn_df = pd.read_csv(output)
    nn_df.to_sql(name=target_table, con=engine, index=False)
    index_query = """
    ALTER TABLE {target_table} add index (id)
    """.format(target_table=target_table)
    update_query = """
    UPDATE rawlocation rl join {target_table}  tt on tt.id = rl.id set rl.location_id = tt.location_id
    """.format(target_table=target_table)
    engine.execute(index_query)
    engine.execute(update_query)


def find_nearest_neighbor_for_source(config, source):
    cstr = get_connection_string(config, source)
    cstr_granted = get_connection_string(config, 'RAW_DB')
    augmented_authority = get_authority_dataset(cstr_granted)
    bt = build_search_tree(augmented_authority)
    augmented_search_dataset = get_search_dataset(cstr, start_date=config['DATES']["START_DATE"],
                                                  end_date=config['DATES']['END_DATE'])
    output = "{workdir}/disambiguation/{src}_nearest_neighbor.csv".format(src=source,
                                                                          workdir=config['FOLDERS']["WORKING_FOLDER"])
    os.makedirs(output, exist_ok=True)
    queue_searchers(augmented_search_dataset, augmented_authority, bt, output)
    load_nearest_neighbor(config, source, output, cstr)


if __name__ == '__main__':
    pass
    # augmented_authority = get_authority_dataset(cstr)
    # bt = build_search_tree(augmented_authority)
    # augmented_search_dataset = get_search_dataset(cstr)
    # queue_searchers(augmented_search_dataset, augmented_authority, bt)
