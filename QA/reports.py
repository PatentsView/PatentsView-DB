from QA.xml_to_csv.ParserTest import ParserTest
from QA.xml_to_csv.XMLTest import XMLTest
from QA.xml_to_csv.DownloadTest import DownloadTest


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
    from pathlib import Path
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
    message = "Merge Report available for :{db_name} in {qa_db}".format(
        db_name=update_config["DATABASE"]["TEXT_DATABASE"],
        qa_db=qa_database)
    return message


def upload_report(update_config):
    qa_database = update_config["DATABASE"]['QA_DATABASE']
    message = "Upload Report available for :{db_name} in {qa_db}".format(
        db_name=update_config["DATABASE"]["TEMP_UPLOAD_DB"],
        qa_db=qa_database)
    return message


def get_report_message(task, update_config):
    report_lookup = {'download_xml': xml_download_report, 'process_xml': xml_process_report, 'parse_xml': parser_report,
                     'backup_olddb': backup_report, 'rename_db': rename_report, 'merge_db': merge_report,
                     'create_text_tables': text_table_create_report, 'parse_text_data': text_parser_report,
                     'restore_olddb': restore_report, 'upload_new': upload_report}

    return report_lookup[task](update_config)
