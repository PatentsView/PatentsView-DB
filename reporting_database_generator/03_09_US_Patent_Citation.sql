{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN uspatentcitation 

######################################################################################################################################


drop table if exists `{{reporting_db}}`.`uspatentcitation`;
create table `{{reporting_db}}`.`uspatentcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_patent_id` varchar(20) null,
  `category` varchar(64) null,
  primary key (`citing_patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`uspatentcitation`
  (`citing_patent_id`, `sequence`, `cited_patent_id`, `category`)
select
  pc.`patent_id`, pc.`sequence`, nullif(trim(pc.`citation_id`), ''), nullif(trim(pc.`category`), '')
from
  `{{reporting_db}}`.`patent` p
  inner join `patent`.`uspatentcitation` pc on pc.`patent_id` = p.`patent_id`;

alter table `{{reporting_db}}`.`uspatentcitation` add index `ix_uspatentcitation_cited_patent_id` (`cited_patent_id`);


# END uspatentcitation 

########################################################################################################################################