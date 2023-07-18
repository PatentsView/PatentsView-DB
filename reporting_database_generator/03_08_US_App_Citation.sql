
# BEGIN usapplicationcitation 

#################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`usapplicationcitation`;
create table `{{params.reporting_database}}`.`usapplicationcitation`
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


insert into `{{params.reporting_database}}`.`usapplicationcitation`
(
  `citing_patent_id`, `sequence`, `cited_application_id`,
  `date`, `name`, `kind`, `category`
)
select
  ac.`patent_id`, ac.`sequence`, ac.`application_id_transformed`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  nullif(trim(ac.`name`), ''),
  nullif(trim(ac.`kind`), ''),
  nullif(trim(ac.`category`), '')
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`usapplicationcitation` ac on ac.`patent_id` = p.`patent_id`;


# END usapplicationcitation 

###################################################################################################################################
