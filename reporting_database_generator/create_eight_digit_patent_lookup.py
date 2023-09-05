import os
import pymysql
import datetime
import pandas as pd
from sqlalchemy import create_engine
import logging
from lib.configuration import get_config, get_connection_string, get_current_config


logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

# INSTRUCTIONS
# Utility : Patent numbers consist of six, seven or eight digits. Enter the Patent number excluding commas and spaces and omit leading zeroes.
# Reissue : (e.g., Rennnnnn, RE000126) must enter leading zeroes between "RE" and number to create 6 digits.
# Plant Patents :(e.g., PPnnnnnn, PP000126) must enter leading zeroes between "PP" and number to create 6 digits.
# Design : (e.g., Dnnnnnnn, D0000126) must enter leading zeroes between "D" and number to create 7 digits.
# H Documents (statutory invention registration) : (e.g., Hnnnnnnn , H0000001) must enter leading zeroes between "H" and number to create 7 digits.
# T Documents (defensive publication) : (e.g., Tnnnnnnn , T0000001) must enter leading zeroes between "T" and number to create 7 digits.
# Additions of Improvements : (e.g., AInnnnnn , AI000126AI) must enter leading zeroes between "AI" and number tocreate 6 digits.
# X Patents : (e.g., Xnnnnnnn , X0000001) must enter leading zeroes between "X" and number to create 7 digits.

def update_patent_id_in_patent():
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2000, 1, 1)
    })
    prod_connection_string = get_connection_string(config, database='PROD_DB', connection='DATABASE_SETUP')
    engine = create_engine(prod_connection_string)
    q_list = []
    q0 = """truncate table patent_to_eight_char"""
    q_list.append(q0)
    q1 = """ 
insert into patent_to_eight_char (id, type)
select id, type 
from patent;"""
    q_list.append(q1)
    for query in q_list:
        logging.info(query)
        engine.execute(query)

    target_char = 8
    for i in ['reissue','plant', 'design', 'statutory invention registration','defensive publication', 'utility']:
        logger.info("------------------------------------------------------------")
        logger.info( f"{i} : {target_char} Target Characters")
        logger.info("------------------------------------------------------------")
        if i == 'utility':
            read_q = f"""
update patent_to_eight_char
set patent_id_eight_char = insert(id,1,0,'0')
where length(id) = 7 and type = '{i}';"""
            logger.info(read_q)
            engine.execute(read_q)
            read_q2 = f"""
update patent_to_eight_char
set patent_id_eight_char = id
where length(id) = 8 and type = '{i}';"""
            logger.info(read_q2)
            engine.execute(read_q2)
        else:
            start_position = 2
            if i == 'reissue' or i == 'plant':
                start_position = 3
            for k in range(2, target_char):
                z = target_char - k
                insert_zeros = "0" * z
                read_q = f"""
update patent_to_eight_char
set patent_id_eight_char = insert(id,{start_position},0,'{insert_zeros}')
where length(id) = {k} and type = '{i}';"""
                logger.info(read_q)
                engine.execute(read_q)
            read_q2 = f"""
update patent_to_eight_char
set patent_id_eight_char = id
where length(id) = 8 and type = '{i}';"""
            logger.info(read_q2)
            engine.execute(read_q2)

    logger.info("DONE.")

if __name__ == '__main__':
    update_patent_id_in_patent()
