from QA.tests import XMLTest


def xml_download_report(update_config):
    x = XMLTest(update_config)
    return "Total files downloaded {count} totalling {size} bytes".format(count=x.xml_files_count,
                                                                          size=x.get_file_size())


def get_report_message(task, update_config):
    report_lookup = {'download_xml': xml_download_report}
    return report_lookup[task](update_config)
