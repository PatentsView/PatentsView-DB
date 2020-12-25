import os


class XMLTest:
    def __init__(self, update_config):
        input_folder = '{working_folder}/raw_data'.format(working_folder=update_config['FOLDERS'][
            'WORKING_FOLDER'])
        output_folder = '{working_folder}/clean_data'.format(working_folder=update_config['FOLDERS'][
            'WORKING_FOLDER'])
        import glob
        ip_glob = "{folder}/*.xml".format(folder=input_folder)
        op_glob = "{folder}/*.xml".format(folder=output_folder)
        self.xml_files_count = 0
        self.input_filenames = []
        for fname in glob.glob(ip_glob):
            self.input_filenames.append(fname)
            self.xml_files_count += 1

        self.output_filenames = []
        for fname in glob.glob(op_glob):
            self.output_filenames.append(fname)
            self.xml_files_count += 1

    def runTest(self, config):
        self.test_input_equals_output()
        self.test_clean_files(config)
        # self.test_valid_xml()

    def test_input_equals_output(self):
        assert len(self.input_filenames) == len(self.output_filenames)

    def test_clean_files(self, update_config):
        for i_filename in self.input_filenames:
            filename = os.path.basename(i_filename)
            name, ext = os.path.splitext(filename)
            clean_name = "{name}_clean{ext}".format(name=name, ext=ext)
            o_file = '{working_folder}/clean_data/{clean_name}'.format(working_folder=update_config['FOLDERS'][
                'WORKING_FOLDER'], clean_name=clean_name)
            print(o_file)
            assert o_file in self.output_filenames

    def test_valid_xml(self):
        from lxml import etree
        parser = etree.XMLParser()
        for filename in self.output_filenames:
            try:
                etree.parse(filename, parser)
            except etree.XMLSyntaxError as e:
                message = "Following Message was raised when processing {fname}: {message}".format(fname=filename,
                                                                                                   message=e.msg)
                raise AssertionError(message)
