from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string


def load_lookup_table(config, entity):
    patent_entity_table = "patent_{entity}".format(entity=entity)
    entity_field = "{entity}_id".format(entity=entity)
    rawtable = "raw{entity}".format(entity=entity)
    delete_query = "DELETE FROM {pet}".format(pet=patent_entity_table)
    insert_query = "INSERT INTO {pet} SELECT et.patent_id, et.{ef}, rl.location_id from {rt} et left join rawlocation " \
                   "rl on rl.id = et.rawlocation_id where {ef} is not null".format(
        pet=patent_entity_table, ef=entity_field,
        rt=rawtable)
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    with engine.begin() as cursor:
        print(delete_query)
        cursor.execute(delete_query)
        print(insert_query)
        cursor.execute(insert_query)


def create_lookup_tables(config):
    load_lookup_table(config, "inventor")
    load_lookup_table(config, "assignee")
    load_lookup_table(config, "lawyer")


if __name__ == '__main__':
    config = get_config()
    create_lookup_tables(config)
