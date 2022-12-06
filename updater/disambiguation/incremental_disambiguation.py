import datetime

from lib.configuration import get_current_config


def begin_inventor_disambiguation(**kwargs):
    config = get_current_config(**kwargs, supplemental_configs=['config/inventor/build_assignee_features_sql.ini'])
    from updater.disambiguation.hierarchical_clustering_disambiguation.pv.disambiguation.inventor.build_assignee_features_sql import \
        main
    main(config)




if __name__ == "__main__":
    begin_inventor_disambiguation(**{
            'execution_date': datetime.date(2020, 12, 30)
            })
