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


def get_report_message(task, update_config):
    report_lookup = {'download_xml': xml_download_report, 'process_xml': xml_process_report, 'parse_xml': parser_report}
    return report_lookup[task](update_config)
