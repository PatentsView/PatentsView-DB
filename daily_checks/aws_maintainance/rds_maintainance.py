from lib.utilities import rds_free_space


def check_aws_rds_space(config):
    free_space_in_bytes = rds_free_space(
            config,
            config['PATENTSVIEW_DATABASES']['identifier'])
    if free_space_in_bytes / (1024 * 1024 * 1024) < 50:
        raise Exception("Free space less than 50G in RDS")
