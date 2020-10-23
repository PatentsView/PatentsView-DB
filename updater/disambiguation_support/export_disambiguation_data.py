import csv

from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string
from lib.utils.table_exporter import export


def export_table(config, table, table_config):
    output_file = '{}/{}/{}.tsv'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_inputs', table)
    engine = create_engine(get_connection_string(config, 'NEW_DB'))
    database = config["DATABASE"]["NEW_DB"]
    fields = table_config['fields']
    order_fields = table_config['order_fields']
    separator = "\t"
    export(engine, database, table, fields, output_file, separator, quote_option=csv.QUOTE_NONE,
           chunk_options={
                   'chunk_size':   1000000,
                   'order_fields': order_fields,
                   'progress':     False,
                   'escapechar':   '\\'
                   })


def export_disambig_data(config):
    tables_to_export = {
            'patent':       {
                    "fields":       ["id", "type", "number", "country", "date", "abstract", "title", "kind",
                                     "num_claims",
                                     "filename"],
                    "order_fields": ["id"]
                    },
            'cpc_current':  {
                    "fields":       ["uuid", "patent_id", "section_id", "subsection_id", "group_id", "subgroup_id",
                                     "category",
                                     "sequence"],
                    "order_fields": ["uuid"]
                    },
            'ipcr':         {
                    "fields":       ["uuid", "patent_id", "classification_level", "section", "ipc_class", "subclass",
                                     "main_group",
                                     "subgroup", "symbol_position", "classification_value", "classification_status",
                                     "classification_data_source", "action_date", "ipc_version_indicator", "sequence"],
                    "order_fields": ["uuid"]
                    },
            'nber':         {
                    "fields":       ["uuid", "patent_id", "category_id", "subcategory_id"],
                    "order_fields": ["uuid"]
                    },
            'rawassignee':  {
                    "fields":       ["uuid", "patent_id", "assignee_id", "rawlocation_id", "type", "name_first",
                                     "name_last",
                                     "organization", "sequence"],
                    "order_fields": ["uuid"]
                    },
            'rawinventor':  {
                    "fields":       ["uuid", "patent_id", "inventor_id", "rawlocation_id",
                                     "coalesce(name_first, '') as name_first",
                                     "coalesce(name_last, '') as name_last", "sequence"],
                    "order_fields": ["uuid"]
                    },
            'uspc_current': {
                    "fields":       ["uuid", "patent_id", "mainclass_id", "subclass_id", "sequence"],
                    "order_fields": ["uuid"]
                    },
            'rawlocation':  {
                    "fields":       ["id", "location_id_transformed as location_id", "city", "state",
                                     "country_transformed as country"],
                    "order_fields": ["id"]
                    },
            'rawlawyer':    {
                    "fields":       ["uuid", "lawyer_id", "patent_id", "name_first", "name_last", "organization",
                                     "country",
                                     "sequence"],
                    "order_fields": ["uuid"]
                    }
            }
    for table in tables_to_export:
        export_table(config, table, tables_to_export[table])


if __name__ == '__main__':
    config = get_config()
    print(config)
    export_disambig_data(config)
