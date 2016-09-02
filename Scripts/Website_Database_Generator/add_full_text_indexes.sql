# At this point, the generate_database SQL and UnencodeHTMLEntities Python scripts should have
# been run.  This is the last step.


# BEGIN full text indexing ####################################################################################################################################


# According to documentation, it is faster to load a table without FTI then add the
# FTI afterwards.  In SQL Server world, we also found this to be true of regular indexes, fwiw...
# 3:16:38
alter table `PatentsView_dev`.`patent` add fulltext index `fti_patent_abstract` (`abstract`);
alter table `PatentsView_dev`.`patent` add fulltext index `fti_patent_title` (`title`);
ALTER TABLE `PatentsView_dev`.`government_interest` ADD FULLTEXT INDEX `fti_government_interest_gi_statement` (`gi_statement`);
alter table `PatentsView_dev`.`uspc_current` add fulltext index `fti_uspc_current_mainclass_title` (`mainclass_title`);
alter table `PatentsView_dev`.`uspc_current` add fulltext index `fti_uspc_current_subclass_title` (`subclass_title`);
alter table `PatentsView_dev`.`uspc_current_mainclass` add fulltext index `fti_uspc_current_mainclass_mainclass_title` (`mainclass_title`);


# END full text indexing ######################################################################################################################################
