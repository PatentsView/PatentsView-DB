from QA.xml_to_csv.DownloadTest import DownloadTest
from QA.xml_to_csv.ParserTest import ParserTest
from QA.xml_to_csv.XMLTest import XMLTest


def xml_download_report(update_config):
    x = DownloadTest(update_config)
    return "Total files downloaded {count} totalling {size} bytes".format(count=x.xml_files_count,
                                                                          size=x.get_file_size())


def xml_process_report(update_config):
    x = XMLTest(update_config)
    return "Total files cleaned {count}".format(count=len(x.output_filenames), )


def parser_report(update_config):
    from tabulate import tabulate
    x = ParserTest(update_config)
    shape_frame = x.get_file_shapes(update_config)
    return tabulate(shape_frame)


def rename_report(update_config):
    message = "New database created:{db_name}".format(db_name=update_config["DATABASE"]["NEW_DB"])
    return message


def merge_report(update_config):
    qa_database = update_config["DATABASE"]['QA_DATABASE']
    message = "Merge Report available for :{db_name} in {qa_db}".format(db_name=update_config["DATABASE"]["NEW_DB"],
                                                                        qa_db=qa_database)
    return message


def backup_report(update_config):
    from pathlib import Path
    directory_parameter = "{datahome}/{database}_backup".format(datahome=update_config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=update_config["DATABASE"]["TEMP_UPLOAD_DB"])
    root_directory = Path(directory_parameter)
    message = "The backup occupies {size} bytes".format(
            size=sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file()))
    return message


def restore_report(update_config):
    directory_parameter = "{datahome}/{database}_backup".format(datahome=update_config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=update_config["DATABASE"]["TEMP_UPLOAD_DB"])
    database_parameter = "{database}".format(database=update_config["DATABASE"]["OLD_DB"])
    message = "Database restored from {dir_param} to {db}".format(dir_param=directory_parameter, db=database_parameter)
    return message


def text_table_create_report(update_config):
    message = "Tables created in the temp database: {temp_db}".format(
            temp_db=update_config['DATABASE']["TEMP_UPLOAD_DB"])
    return message


def text_parser_report(update_config):
    qa_database = update_config["DATABASE"]['QA_DATABASE']
    message = "Current update text data parsed in :{db_name}".format(
            db_name=update_config["DATABASE"]["TEMP_UPLOAD_DB"],
            qa_db=qa_database)
    return message


def upload_report(update_config):
    qa_database = update_config["DATABASE"]['QA_DATABASE']
    message = "Upload Report available for : {db_name} in {qa_db}".format(
            db_name=update_config["DATABASE"]["TEMP_UPLOAD_DB"],
            qa_db=qa_database)
    return message


def cpc_class_uploader_report(update_config):
    message = "CPC Class data uploaded"
    return message


def withdrawn_processor_report(update_config):
    message = "Withdrawn Patents marked in : {db_name}".format(db_name=update_config["DATABASE"]["NEW_DB"])
    return message


def wipo_processor_report(config):
    message = "Wipo Processing complete"
    return message


def qc_withdrawn_processor_report(config):
    message = "Withdrawn QC Complete"
    return message


def qc_upload_new_report(config):
    message = "New Data QC Complete"
    return message


def qc_rename_db_report(config):
    message = "Database Rename QC Complete"
    return message


def qc_parse_text_data_report(config):
    message = "Text Data Parsing QC Complete"
    return message


def cpc_current_processor_report(config):
    message = "CPC Current Processed"
    return message


def cpc_parser_report(config):
    message = "CPC Data Parsing completed"
    return message


def create_text_trigger_report(config):
    message = "Triggers added to Upload Text Tables"
    return message


def create_text_yearly_tables_report(config):
    message = "Yearly table added/verified in new database : {newdb}".format(newdb=config['DATABASES']['NEW_DB'])
    return message


def merge_text_db_report(config):
    message = "New Text Data Merged with Complete Data"
    return message


def qc_download_cpc_report(config):
    message = "CPC data Download QC Complete"
    return message


def qc_cpc_parser_report(config):
    message = "CPC data Parsed from download"
    return message


def qc_merge_db_report(config):
    message = "Merged Patent Database QC Complete"
    return message


def cpc_class_parser_report(config):
    message = "CPC Classes Parsed"
    return message


def download_cpc_report(config):
    message = "CPC Download Complete"
    return message


def qc_cpc_class_parser_report(config):
    message = "CPC Class Parser QC Complete"
    return message


def qc_cpc_current_wipo_report(config):
    message = "Uploaded WIPO & CPC Current QC Complete"
    return message


def qc_merge_text_db_report(config):
    message = "Merged Text Database QC Complete"
    return message


def persistent_wide_assignee_report(config):
    message = "Persistent Assignee IDs converted to wide format"
    return message


def persistent_wide_inventor_report(config):
    message = "Persistent Inventor IDs converted to wide format"
    return message


def disambiguation_download_report(config):
    message = "Disambiguation results downloaded"
    return message


def disambiguation_export_report(config):
    message = "Disambiguation Data exported"
    return message


def disambiguation_upload_report(config):
    message = "Disambiguation Data Uploaded to Disambig Server. Ready to start disambiguation"
    return message


def gi_NER_report(config):
    message = "Govt. interest NER processing complete"
    return message


def gi_NER_post_processing_report(config):
    message = "Govt. interest NER post processing complete. Ready for manual step"
    return message


def gi_post_manual_report(config):
    message = "Govt. interest post manual step complete"
    return message


def relationship_table_report(config):
    message = "Patent, Assignee, Inventor & Location crosswalks generated"
    return message


def post_process_assignee_report(config):
    message = "Assignee disambiguation post processed"
    return message


def post_process_inventor_report(config):
    message = "Inventor disambiguation post processed"
    return message


def post_process_location_report(config):
    message = "Location disambiguation post processed"
    return message


def qc_post_process_assignee_report(config):
    message = "Assignee disambiguation QC complete"
    return message


def qc_post_process_inventor_report(config):
    message = "Inventor disambiguation QC complete"
    return message


def qc_post_process_location_report(config):
    message = "Location disambiguation QC complete"
    return message


def lawyer_dismabig_report(config):
    message = "Lawyer disambigutation complete."
    return message


def persistent_long_assignee_report(config):
    message = "Latest disambiguated assignees added to persistent table"
    return message


def persistent_long_inventor_report(config):
    message = "Latest disambiguated inventors added to persistent table"
    return message


def api_check_report(config):
    message = "All API Queries in documentation return non zero results"
    return message


def get_report_message(task, update_config):
    report_lookup = {
            'download_xml':                    xml_download_report,
            'process_xml':                     xml_process_report,
            'parse_xml':                       parser_report,
            'backup_olddb':                    backup_report,
            'rename_db':                       rename_report,
            'merge_db':
                                               merge_report,
            'create_text_tables':              text_table_create_report,
            'parse_text_data':                 text_parser_report,
            'restore_olddb':                   restore_report,
            'upload_new':                      upload_report,
            'cpc_class_parser':                cpc_class_parser_report,
            'cpc_class_uploader':              cpc_class_uploader_report,
            'cpc_current_processor':           cpc_current_processor_report,
            'cpc_parser':                      cpc_parser_report,
            'download_cpc':                    download_cpc_report,
            'merge_text_db':                   merge_text_db_report,
            'qc_cpc_class_parser':             qc_cpc_class_parser_report,
            'qc_cpc_current_wipo':             qc_cpc_current_wipo_report,
            'qc_cpc_parser':                   qc_cpc_parser_report,
            'qc_download_cpc':                 qc_download_cpc_report,
            'qc_merge_db':                     qc_merge_db_report,
            'qc_merge_text_db':                qc_merge_text_db_report,
            'qc_parse_text_data':              qc_parse_text_data_report,
            'qc_rename_db':                    qc_rename_db_report,
            'qc_upload_new':                   qc_upload_new_report,
            'qc_withdrawn_processor':          qc_withdrawn_processor_report,
            'wipo_processor':                  wipo_processor_report,
            'withdrawn_processor':             withdrawn_processor_report,
            'create_persistent_wide_assignee': persistent_wide_assignee_report,
            'create_persistent_wide_inventor': persistent_wide_inventor_report,
            'download_disambiguation':         disambiguation_download_report,
            'export_disambig_data':            disambiguation_export_report,
            'gi_NER':                          gi_NER_report,
            'gi_post_manual':                  gi_post_manual_report,
            'lookup_tables':                   relationship_table_report,
            'post_process_assignee':           post_process_assignee_report,
            'post_process_inventor':           post_process_inventor_report,
            'post_process_location':           post_process_location_report,
            'qc_post_process_assignee':        qc_post_process_assignee_report,
            'qc_post_process_inventor':        qc_post_process_inventor_report,
            'qc_post_process_location':        qc_post_process_location_report,
            'postprocess_NER':                 gi_NER_post_processing_report,
            'run_lawyer_disambiguation':       lawyer_dismabig_report,
            'update_persistent_long_assignee': persistent_long_assignee_report,
            'update_persistent_long_inventor': persistent_long_inventor_report,
            'upload_disambig_files':           disambiguation_upload_report,
            'create_text_triggers':            create_text_trigger_report,
            'create_text_yearly_tables':       create_text_yearly_tables_report,
            'api_query_check':                 api_check_report
            }

    return report_lookup[task](update_config)
