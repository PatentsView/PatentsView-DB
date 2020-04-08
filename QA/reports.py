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


def backup_report(update_config):
    from pathlib import Path
    directory_parameter = "{datahome}/{database}_backup".format(datahome=update_config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=update_config["DATABASE"]["TEMP_UPLOAD_DB"])
    root_directory = Path(directory_parameter)
    message = "The backup occupies {size}".format(
        size=sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file()))
    return message


def get_report_message(task, update_config):
    report_lookup = {'download_xml': xml_download_report, 'process_xml': xml_process_report, 'parse_xml': parser_report,
                     'backup_olddb': backup_report, 'rename_db': rename_report}
    return report_lookup[task](update_config)
