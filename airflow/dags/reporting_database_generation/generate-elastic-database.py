        'source_sql': '12_03_publication_cpc.sql'
    }
)
endpoint_publications_cpc.set_upstream(endpoint_publications_publication_views)

endpoint_publications_gi = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_GI',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_04_publication_gi.sql'
    },
    templates_dict={
        'source_sql': '12_04_publication_gi.sql'
    }
)
endpoint_publications_gi.set_upstream(endpoint_publications_publication_views)

endpoint_publications_us_parties = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_US_Parties_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_05_us_parties.sql'
    },
    templates_dict={
        'source_sql': '12_05_us_parties.sql'
    }
)
endpoint_publications_us_parties.set_upstream(endpoint_publications_publication_views)

endpoint_rel_app_text_pgpub = SQLTemplatedPythonOperator(
    task_id='Related_App_Text_pgpub_Endpoint',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_06_relapptext_pgpub.sql'
    },
    templates_dict={
        'source_sql': '12_06_relapptext_pgpub.sql'
    }
)
endpoint_rel_app_text_pgpub.set_upstream(endpoint_publications_publication_views)