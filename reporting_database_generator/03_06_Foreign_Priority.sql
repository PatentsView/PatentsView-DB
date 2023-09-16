
# BEGIN foreignpriority 

#################################################################################################################################


drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`foreignpriority`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`foreignpriority`
(
  `patent_id` varchar(20) not null,
  `sequence` int not null,
  `foreign_doc_number` varchar(64) null,
  `date` date null,
  `country` varchar(64) null,
  `kind` varchar(24) null,
  primary key (`patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`foreignpriority`
(
  `patent_id`, `sequence`, `foreign_doc_number`,
  `date`, `country`, `kind`
)
select
  ac.`patent_id`, ac.`sequence`, ac.`number`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  nullif(trim(ac.`country_transformed`), ''),
  nullif(trim(ac.`kind`), '')
from
  `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent` p
  inner join `patent`.`foreign_priority` ac on ac.`patent_id` = p.`patent_id`;


# END foreignpriority

###################################################################################################################################