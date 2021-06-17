
# BEGIN foreignpriority 

#################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`foreignpriority`;
create table `{{params.reporting_database}}`.`foreignpriority`
(
  `patent_id` varchar(20) not null,
  `sequence` int not null,
  `foreign_doc_number` varchar(64) null,
  `date` date null,
  `country` varchar(64) null,
  `kind` varchar(24) null,
  primary key (`patent_id`, `sequence`)
)
engine=InnoDB;


# 13,617,656 @ 8:22
insert into `{{params.reporting_database}}`.`foreignpriority`
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
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`foreign_priority` ac on ac.`patent_id` = p.`patent_id`  where p.version_indicator<= {{ params.version_indicator }};


# END foreignpriority

###################################################################################################################################