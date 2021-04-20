from lib.configuration import get_current_config
from updater.collect_supplemental_data.cpc_parser.pgpubs_download_cpc_ipc import begin_download
from updater.collect_supplemental_data.cpc_parser.pgpubs_cpc_parser import begin_cpc_upload


def begin_update_cpc_current(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    begin_download(config)
    begin_cpc_upload(config)


if __name__ == "__main__":
    begin_update_cpc_current(**{
        "execution_date": datetime.date(2020, 12, 17)
    })
