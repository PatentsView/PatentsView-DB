#Steps to Rerun Assignee Disambiguation 

1. Nothing upstream of Assignee Run Clustering needs to be rerun 
2. Rename Current output on Disambiguation folder 
`/home/centos/PipelineData/Disambiguation/exp_out/assignee/2023-09-30`
`mv 2023-09-30 2023-09-30-thres7`
3. Change threshold in the PV-Disambiguation repo on the Dismabiguation server
`` vi /home/centos/PatentsView-DB/updater/disambiguation/hierarchical_clustering_disambiguation/config/new_consolidated_config.ini``
4. Re-run assignee disambiguation step (run_clustering) ~takes about 1 hour
5. Rename database tables in SQL: 
   1. `patent`:
``use patent;
rename table assignee_disambiguation_mapping_20230930 to assignee_disambiguation_mapping_20230930_thres7;
rename table assignee_20230930 to assignee_20230930_thres7;
rename table disambiguated_assignee_ids_20230930 to disambiguated_assignee_ids_20230930_thres7;
rename table assignee_reassignment_final to assignee_reassignment_final_thres7;
``
   2. ``pregrant_publications``:
``use pregrant_publications;
rename table assignee_disambiguation_mapping_20230930 to assignee_disambiguation_mapping_20230930_thres7;``
6. Update QA tables. Note that the data in DataMonitor tables are automatically deleted when the QA scripts are rerun. Move the current QA run
```insert into DataMonitor_topnentities_iterative
select * 
from DataMonitor_topnentities
where entity_name = 'assignee_20230930' and related_entity = 'rawassignee'

update DataMonitor_topnentities_iterative 
set threshold=7
where entity_name = 'assignee_20230930' and related_entity = 'rawassignee'
```
