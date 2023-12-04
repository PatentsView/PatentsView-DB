from lib.download_check_delete_databases import run_table_archive


def archive_assignee_tables(**kwargs):
    for db_type in ['granted_patent', "pgpubs"]:
        run_table_archive(db_type, "assignee_disambiguation_mapping_", **kwargs)
        run_table_archive(db_type, "disambiguated_assignee_ids_", **kwargs)
    run_table_archive("granted_patent", "assignee_", **kwargs)

def archive_inventor_tables(**kwargs):
    for db_type in ['granted_patent', "pgpubs"]:
        run_table_archive(db_type, "inventor_disambiguation_mapping_", **kwargs)
        run_table_archive(db_type, "disambiguated_inventor_ids_", **kwargs)
    run_table_archive("granted_patent", "inventor_", **kwargs)

def archive_location_tables(**kwargs):
    for db_type in ['granted_patent', "pgpubs"]:
        run_table_archive(db_type, "location_disambiguation_mapping_", **kwargs)
    run_table_archive("granted_patent", "inventor_", **kwargs)