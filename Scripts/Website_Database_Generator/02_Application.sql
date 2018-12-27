# BEGIN application

###########################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`application`;

CREATE TABLE `PatentsView_20181127`.`application`
(
   `application_id`    VARCHAR(36) NOT NULL,
   `patent_id`         VARCHAR(20) NOT NULL,
   `type`              VARCHAR(20) NULL,
   `number`            VARCHAR(64) NULL,
   `country`           VARCHAR(20) NULL,
   `date`              DATE NULL,
   PRIMARY KEY(`application_id`, `patent_id`)
)
ENGINE = INNODB;


# 5,425,879 @ 1:11

INSERT INTO `PatentsView_20181127`.`application`(`application_id`,
                                                 `patent_id`,
                                                 `type`,
                                                 `number`,
                                                 `country`,
                                                 `date`)
   SELECT `id_transformed`,
          `patent_id`,
          nullif(trim(`type`), ''),
          nullif(trim(`number_transformed`), ''),
          nullif(trim(`country`), ''),
          CASE
             WHEN     `date` > date('1899-12-31')
                  AND `date` < date_add(current_date, INTERVAL 10 YEAR)
             THEN
                `date`
             ELSE
                NULL
          END
     FROM `patent_20181127`.`application`;


# END application

#############################################################################################################################################
