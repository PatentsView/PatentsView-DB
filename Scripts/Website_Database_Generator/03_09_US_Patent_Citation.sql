
# BEGIN uspatentcitation 

######################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`uspatentcitation`;
create table `{{params.reporting_database}}`.`uspatentcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_patent_id` varchar(20) null,
  `category` varchar(20) null,
  primary key (`citing_patent_id`, `sequence`)
)
engine=InnoDB;


# 71,126,097 @ 32:52
insert into `{{params.reporting_database}}`.`uspatentcitation`
  (`citing_patent_id`, `sequence`, `cited_patent_id`, `category`)
select
  pc.`patent_id`, pc.`sequence`, nullif(trim(pc.`citation_id`), ''), nullif(trim(pc.`category`), '')
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`uspatentcitation` pc on pc.`patent_id` = p.`patent_id`;


# END uspatentcitation 

########################################################################################################################################