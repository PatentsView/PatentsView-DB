from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from updater.disambiguation.lawyer_disambiguation.lawyer_disambiguation import post_process_qc as lawyer_post_process_qc
from updater.post_processing.post_process_assignee import post_process_qc as assignee_post_process_qc
from updater.post_processing.post_process_inventor import post_process_qc as inventor_post_process_qc
from updater.post_processing.post_process_location import post_process_qc as location_post_process_qc
from updater.post_processing.post_process_persistent import prepare_wide_table, write_wide_table


def add_postprocessing_operators(disambiguation_post_processing, config, project_home, airflow_task_success,
                                 airflow_task_failure):
    from lib.configuration import get_scp_download_command
    from updater.post_processing.create_lookup import create_lookup_tables
    from updater.post_processing.post_process_assignee import post_process_assignee
    from updater.post_processing.post_process_inventor import post_process_inventor
    from updater.post_processing.post_process_location import post_process_location
    from updater.post_processing.post_process_persistent import update_long_entity

    download_disambig_operator = BashOperator(task_id='download_disambiguation',
                                              bash_command=get_scp_download_command(config),
                                              dag=disambiguation_post_processing,
                                              on_success_callback=airflow_task_success,
                                              on_failure_callback=airflow_task_failure
                                              )
    post_process_inventor_operator = PythonOperator(task_id='post_process_inventor',
                                                    python_callable=post_process_inventor,
                                                    op_kwargs={
                                                            'config': config
                                                            },
                                                    dag=disambiguation_post_processing,
                                                    on_success_callback=airflow_task_success,
                                                    on_failure_callback=airflow_task_failure)

    post_process_assignee_operator = PythonOperator(task_id='post_process_assignee',
                                                    python_callable=post_process_assignee,
                                                    op_kwargs={
                                                            'config': config
                                                            },
                                                    dag=disambiguation_post_processing,
                                                    on_success_callback=airflow_task_success,
                                                    on_failure_callback=airflow_task_failure)
    qc_post_process_assignee_operator = PythonOperator(task_id='qc_post_process_assignee',
                                                       python_callable=assignee_post_process_qc,
                                                       op_kwargs={
                                                               'config': config
                                                               },
                                                       dag=disambiguation_post_processing,
                                                       on_success_callback=airflow_task_success,
                                                       on_failure_callback=airflow_task_failure)
    qc_post_process_inventor_operator = PythonOperator(task_id='qc_post_process_inventor',
                                                       python_callable=inventor_post_process_qc,
                                                       op_kwargs={
                                                               'config': config
                                                               },
                                                       dag=disambiguation_post_processing,
                                                       on_success_callback=airflow_task_success,
                                                       on_failure_callback=airflow_task_failure)
    qc_post_process_location_operator = PythonOperator(task_id='qc_post_process_location',
                                                       python_callable=location_post_process_qc,
                                                       op_kwargs={
                                                               'config': config
                                                               },
                                                       dag=disambiguation_post_processing,
                                                       on_success_callback=airflow_task_success,
                                                       on_failure_callback=airflow_task_failure)
    qc_post_process_lawyer_operator = PythonOperator(task_id='qc_post_process_location',
                                                     python_callable=lawyer_post_process_qc,
                                                     op_kwargs={
                                                             'config': config
                                                             },
                                                     dag=disambiguation_post_processing,
                                                     on_success_callback=airflow_task_success,
                                                     on_failure_callback=airflow_task_failure)
    post_process_location_operator = PythonOperator(task_id='post_process_location',
                                                    python_callable=post_process_location,
                                                    op_kwargs={
                                                            'config': config
                                                            },
                                                    dag=disambiguation_post_processing,
                                                    on_success_callback=airflow_task_success,
                                                    on_failure_callback=airflow_task_failure)

    lookup_tables_operator = PythonOperator(task_id='lookup_tables',
                                            python_callable=create_lookup_tables,
                                            dag=disambiguation_post_processing,
                                            op_kwargs={
                                                    'config': config
                                                    },
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure
                                            )

    update_persistent_long_inventor = PythonOperator(
            task_id='update_persistent_long_inventor',
            python_callable=update_long_entity,
            op_kwargs={
                    'entity': 'inventor',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )

    update_persistent_long_assignee = PythonOperator(
            task_id='update_persistent_long_assignee',
            python_callable=update_long_entity,
            op_kwargs={
                    'entity': 'assignee',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )

    prepare_persistent_wide_inventor = PythonOperator(
            task_id='prepare_persistent_wide_inventor',
            python_callable=prepare_wide_table,
            op_kwargs={
                    'entity': 'inventor',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )

    prepare_persistent_wide_assignee = PythonOperator(
            task_id='prepare_persistent_wide_assignee',
            python_callable=prepare_wide_table,
            op_kwargs={
                    'entity': 'assignee',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )

    create_persistent_wide_inventor = PythonOperator(
            task_id='create_persistent_wide_inventor',
            python_callable=write_wide_table,
            op_kwargs={
                    'entity': 'inventor',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )

    create_persistent_wide_assignee = PythonOperator(
            task_id='create_persistent_wide_assignee',
            python_callable=write_wide_table,
            op_kwargs={
                    'entity': 'assignee',
                    'config': config
                    },
            dag=disambiguation_post_processing
            )
    post_process_inventor_operator.set_upstream(download_disambig_operator)
    post_process_assignee_operator.set_upstream(download_disambig_operator)
    post_process_location_operator.set_upstream(download_disambig_operator)

    lookup_tables_operator.set_upstream(post_process_inventor_operator)
    lookup_tables_operator.set_upstream(post_process_assignee_operator)
    lookup_tables_operator.set_upstream(post_process_location_operator)

    qc_post_process_assignee_operator.set_upstream(lookup_tables_operator)
    qc_post_process_inventor_operator.set_upstream(lookup_tables_operator)
    qc_post_process_location_operator.set_upstream(lookup_tables_operator)
    qc_post_process_lawyer_operator.set_upstream(lookup_tables_operator)

    update_persistent_long_inventor.set_upstream(qc_post_process_inventor_operator)
    update_persistent_long_assignee.set_upstream(qc_post_process_assignee_operator)

    prepare_persistent_wide_assignee.set_upstream(update_persistent_long_assignee)
    prepare_persistent_wide_inventor.set_upstream(update_persistent_long_inventor)
    create_persistent_wide_inventor.set_upstream(prepare_persistent_wide_inventor)
    create_persistent_wide_assignee.set_upstream(prepare_persistent_wide_assignee)
