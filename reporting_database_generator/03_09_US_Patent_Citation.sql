
# BEGIN uspatentcitation 

######################################################################################################################################


drop table if exists `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`uspatentcitation`;
create table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`uspatentcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_patent_id` varchar(20) null,
  `category` varchar(64) null,
  primary key (`citing_patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`uspatentcitation`
  (`citing_patent_id`, `sequence`, `cited_patent_id`, `category`)
select
  pc.`patent_id`, pc.`sequence`, nullif(trim(pc.`citation_id`), ''), nullif(trim(pc.`category`), '')
from
  `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`patent` p
  inner join `patent`.`uspatentcitation` pc on pc.`patent_id` = p.`patent_id`;


# END uspatentcitation 

########################################################################################################################################