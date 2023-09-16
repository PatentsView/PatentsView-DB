# BEGIN application

###########################################################################################################################################


drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`application`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`application`
(
  `application_id` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `type` varchar(20) null,
  `number` varchar(64) null,
  `country` varchar(20) null,
  `date` date null,
  primary key (`application_id`, `patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`application`
  (`application_id`, `patent_id`, `type`, `number`, `country`, `date`)
select
  `id_transformed`, `patent_id`, nullif(trim(`type`), ''),
  nullif(trim(`number_transformed`), ''), nullif(trim(`country`), ''),
  case when `date` > date('1899-12-31') and `date` < date_add(current_date, interval 10 year) then `date` else null end
from
  `patent`.`application`  where version_indicator<='{{ macros.ds_add(dag_run.data_interval_end | ds, -1) }}';


# END application 

#############################################################################################################################################