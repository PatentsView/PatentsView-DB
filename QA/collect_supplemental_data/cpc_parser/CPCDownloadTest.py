import glob


class CPCDownloadTest:
    def __init__(self, config, pgpubs_config):
        self.config = config
        self.pgpubs_config = pgpubs_config

    def test_download(self):
        upload_destination_folder = '{}/{}'.format(self.config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
        print(upload_destination_folder)
        cpc_pattern = upload_destination_folder + '/cpc-scheme*'
        files = [x for x in glob.glob(cpc_pattern)]
        if len(files) == 0:
            raise AssertionError("CPC Scheme files not found")

        granted_pattern = upload_destination_folder + '/US_Grant_CPC_MCF_*.xml'
        files = [x for x in glob.glob(granted_pattern)]
        if len(files) == 0:
            raise AssertionError("Granted patent CPC assignment files not found")

        pgpubs_destination_folder = '{}/{}'.format(self.pgpubs_config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
        print(pgpubs_destination_folder)
        pregranted_pattern = pgpubs_destination_folder + '/US_PGPub_CPC_MCF_*.txt'
        files = [x for x in glob.glob(pregranted_pattern)]
        if len(files) == 0:
            raise AssertionError("Pregranted CPC assignment files not found")

        g_ipc_concordance = upload_destination_folder + '/ipc_concordance.txt'
        files = [x for x in glob.glob(g_ipc_concordance)]
        if len(files) != 1:
            raise AssertionError("IPC Concordance files not found")

        pg_ipc_concordance = upload_destination_folder + '/ipc_concordance.txt'
        files = [x for x in glob.glob(pg_ipc_concordance)]
        if len(files) != 1:
            raise AssertionError("IPC Concordance files not found")

    def runTests(self):
        self.test_download()

if __name__ == '__main__':
    CPCDownloadTest.runTests()