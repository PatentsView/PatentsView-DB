from configparser import ConfigParser

from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string


def load_lookup_table(update_config: ConfigParser, database: str, parent_entity: str, parent_entity_id: str,
                      entity: str, version_indicator: int, include_location: bool = True):
    """
    Load Patent Crosswalk tables with disambiguated tables
    :param version_indicator:
    :param update_config: Configparser object
    :param database: Target database (Raw database / Pgpubs database)
    :param entity: disambiguated entity name (assignee, inventor, location)
    :param include_location: Boolean flag indicating if location id is included in crosswalk
    """
    patent_entity_table = "{parent}_{entity}".format(parent=parent_entity, entity=entity)
    entity_field = "{entity}_id".format(entity=entity)
    rawtable = "raw{entity}".format(entity=entity)
    field_list = ["'{vind}'", parent_entity_id, "et.{ef}", "et.sequence"]
    insert_sequence = ["version_indicator", "{peid}", "{ef}", "sequence"]
    join_query = ""
    if include_location:
        field_list.append("rl.location_id")
        insert_sequence.append("location_id")
        join_query = "left join rawlocation rl on rl.id = et.rawlocation_id"
    field_select = ", ".join(field_list).format(ef=entity_field, vind=version_indicator)
    field_list_sequence = ", ".join(insert_sequence).format(peid=parent_entity_id, ef=entity_field)
    delete_query = "DELETE FROM {pet}".format(pet=patent_entity_table)
    insert_query = """
        INSERT IGNORE INTO {pet} ({fseq}) SELECT {fs} from {rt} et {jq} where {ef} is not null
        """.format(fseq=field_list_sequence,
                   fs=field_select,
                   jq=join_query,
                   pet=patent_entity_table,
                   ef=entity_field,
                   rt=rawtable)
    engine = create_engine(get_connection_string(update_config, database))
    with engine.begin() as cursor:
        print(delete_query)
        cursor.execute(delete_query)
        print(insert_query)
        cursor.execute(insert_query)


if __name__ == '__main__':
    config = get_config()
