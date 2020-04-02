from QA.tests import XMLTest


def xml_download_report(update_config, project_home):
    x = XMLTest(update_config, project_home)
    return "Total files downloaded {count} totalling {size} bytes".format(count=x.xml_files_count,
                                                                          size=x.get_file_size())


def get_report_message(task, update_config, project_home):
    report_lookup = {'download_xml': xml_download_report}
    return report_lookup[task](update_config, project_home)
