ARG PV_PIPELINE_VERSION=0.1
FROM patentsview/airflow:$PV_PIPELINE_VERSION
ENV PACKAGE_HOME /project
ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}"
ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}/updater/disambiguation/hierarchical_clustering_disambiguation"
WORKDIR $PACKAGE_HOME