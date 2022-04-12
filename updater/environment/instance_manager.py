from lib.configuration import get_current_config
from lib.utilities import manage_ec2_instance


def start_data_collector_instance(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    manage_ec2_instance(config=config, button='ON', identifier='xml_collector')


def stop_data_collector_instance(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    manage_ec2_instance(config=config, button='OFF', identifier='xml_collector')


def start_disambiguation_instance(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    manage_ec2_instance(config=config, button='ON', identifier='disambiguation')


def stop_disambiguation_instance(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    manage_ec2_instance(config=config, button='OFF', identifier='disambiguation')
