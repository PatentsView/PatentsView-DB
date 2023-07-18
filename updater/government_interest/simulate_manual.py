import datetime
from shutil import copyfile

import pandas as pd

from lib.configuration import get_current_config


def simulate_manual(dbtype='granted_patent', **kwargs):
    config = get_current_config(type=dbtype, **kwargs)
    manual_inputs = '{}/government_interest/manual_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
    post_manual = '{}/government_interest/post_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    files_to_copy = {
        'to_check.csv': 'to_check_checked.csv',
        'automatically_matched.csv': 'automatically_matched.csv',
        'government_organization.csv': 'government_organization.csv',
    }
    for source, destination in files_to_copy.items():
        copyfile('{path}/{file}'.format(path=manual_inputs, file=source),
                 '{path}/{file}'.format(path=post_manual, file=destination))

    gorg = pd.DataFrame([], columns=['organization_id', 'name', 'level_one', 'level_two', 'level_three',
                                     'version_indicator', 'created_date', 'updated_date'])
    gorg.to_csv('{path}/{file}'.format(path=post_manual, file='new_organizations.csv'), index=False)


if __name__ == '__main__':
    kw = {
        "execution_date": datetime.date(2022, 6, 23)
    }
    simulate_manual(dbtype='pgpubs', **kw)
