
# Make the claim table 
#takes a very long time, idk why quite so long
create table `{{params.reporting_database}}`.`claim` like `{{params.raw_database}}`.`claim`;
insert into `{{params.reporting_database}}`.`claim` select * FROM  `{{params.raw_database}}`.`claim`;


