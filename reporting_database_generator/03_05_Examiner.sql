
# BEGIN examiner 

##############################################################################################################################################

drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`examiner`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`examiner`
(
  `examiner_id` int unsigned not null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `role` varchar(20) null,
  `group` varchar(20) null,
  `persistent_examiner_id` varchar(36) not null,
  primary key (`examiner_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`examiner`
(
  `examiner_id`, `name_first`, `name_last`, `role`, `group`, `persistent_examiner_id`
)
select
  t.`new_examiner_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),  nullif(trim(i.`role`), ''),  nullif(trim(i.`group`), ''),
  i.`uuid`
from
  `patent`.`rawexaminer` i
  inner join `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`temp_id_mapping_examiner` t on t.`old_examiner_id` = i.`uuid` where i.version_indicator<= '{{ params.version_indicator }}';


drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent_examiner`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent_examiner`
(
  `patent_id` varchar(20) not null,
  `examiner_id` int unsigned not null,
  `role` varchar(20) not null,
  primary key (`patent_id`, `examiner_id`),
  unique index ak_patent_examiner (`examiner_id`, `patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent_examiner`
(
  `patent_id`, `examiner_id`, `role`
)
select distinct
  ri.`patent_id`, t.`new_examiner_id`, ri.`role`
from
  `patent`.`rawexaminer` ri
  inner join `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`temp_id_mapping_examiner` t on t.`old_examiner_id` = ri.`uuid`  where ri.version_indicator<= '{{ params.version_indicator }}';

# END examiner

################################################################################################################################################
