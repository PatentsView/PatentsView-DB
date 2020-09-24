import glob


class CPCDownloadTest:
    def __init__(self, config):
        self.config = config

    def test_download(self):
        destination_folder = '{}/{}'.format(self.config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
        cpc_pattern = destination_folder + '/cpc-scheme*'
        files = [x for x in glob.glob(cpc_pattern)]
        if len(files) == 0:
            raise AssertionError("CPC Scheme files not found")

        granted_pattern = destination_folder + '/US_Grant_CPC_MCF_*.txt'
        files = [x for x in glob.glob(granted_pattern)]
        if len(files) == 0:
            raise AssertionError("Granted patent CPC assignment files not found")

        ipc_concordance = destination_folder + '/ipc_concordance.txt'
        files = [x for x in glob.glob(ipc_concordance)]
        if len(files) != 1:
            raise AssertionError("IPC Concordance files not found")

    def runTests(self):
        self.test_download()
