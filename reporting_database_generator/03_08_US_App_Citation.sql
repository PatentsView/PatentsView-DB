
# BEGIN usapplicationcitation 

#################################################################################################################################


drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`usapplicationcitation`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`usapplicationcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_application_id` varchar(36) null,
  `date` date null,
  `name` varchar(128) null,
  `kind` varchar(10) null,
  `category` varchar(64) null,
  primary key (`citing_patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`usapplicationcitation`
(
  `citing_patent_id`, `sequence`, `cited_application_id`,
  `date`, `name`, `kind`, `category`
)
select
  ac.`patent_id`, ac.`sequence`, ac.`number_transformed`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  nullif(trim(ac.`name`), ''),
  nullif(trim(ac.`kind`), ''),
  nullif(trim(ac.`category`), '')
from
  `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent` p
  inner join `patent`.`usapplicationcitation` ac on ac.`patent_id` = p.`patent_id`;


# END usapplicationcitation 

###################################################################################################################################
