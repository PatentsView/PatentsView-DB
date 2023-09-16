
# BEGIN uspatentcitation 

######################################################################################################################################


drop table if exists `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`uspatentcitation`;
create table `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`uspatentcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_patent_id` varchar(20) null,
  `category` varchar(64) null,
  primary key (`citing_patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`uspatentcitation`
  (`citing_patent_id`, `sequence`, `cited_patent_id`, `category`)
select
  pc.`patent_id`, pc.`sequence`, nullif(trim(pc.`citation_id`), ''), nullif(trim(pc.`category`), '')
from
  `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`patent` p
  inner join `patent`.`uspatentcitation` pc on pc.`patent_id` = p.`patent_id`;


# END uspatentcitation 

########################################################################################################################################