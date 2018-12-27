# BEGIN usapplicationcitation

#################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`usapplicationcitation`;

CREATE TABLE `PatentsView_20181127`.`usapplicationcitation`
(
   `citing_patent_id`        VARCHAR(20) NOT NULL,
   `sequence`                INT NOT NULL,
   `cited_application_id`    VARCHAR(20) NULL,
   `date`                    DATE NULL,
   `name`                    VARCHAR(64) NULL,
   `kind`                    VARCHAR(10) NULL,
   `category`                VARCHAR(20) NULL,
   PRIMARY KEY(`citing_patent_id`, `sequence`)
)
ENGINE = INNODB;


# 13,617,656 @ 8:22

INSERT INTO `PatentsView_20181127`.`usapplicationcitation`(
               `citing_patent_id`,
               `sequence`,
               `cited_application_id`,
               `date`,
               `name`,
               `kind`,
               `category`)
   SELECT ac.`patent_id`,
          ac.`sequence`,
          ac.`application_id_transformed`,
          CASE
             WHEN     ac.`date` > date('1899-12-31')
                  AND ac.`date` < date_add(current_date, INTERVAL 10 YEAR)
             THEN
                ac.`date`
             ELSE
                NULL
          END,
          nullif(trim(ac.`name`), ''),
          nullif(trim(ac.`kind`), ''),
          nullif(trim(ac.`category`), '')
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`usapplicationcitation` ac
             ON ac.`patent_id` = p.`patent_id`;


# END usapplicationcitation

###################################################################################################################################


# BEGIN uspatentcitation

######################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`uspatentcitation`;

CREATE TABLE `PatentsView_20181127`.`uspatentcitation`
(
   `citing_patent_id`    VARCHAR(20) NOT NULL,
   `sequence`            INT NOT NULL,
   `cited_patent_id`     VARCHAR(20) NULL,
   `category`            VARCHAR(20) NULL,
   PRIMARY KEY(`citing_patent_id`, `sequence`)
)
ENGINE = INNODB;


# 71,126,097 @ 32:52

INSERT INTO `PatentsView_20181127`.`uspatentcitation`(`citing_patent_id`,
                                                      `sequence`,
                                                      `cited_patent_id`,
                                                      `category`)
   SELECT pc.`patent_id`,
          pc.`sequence`,
          nullif(trim(pc.`citation_id`), ''),
          nullif(trim(pc.`category`), '')
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`uspatentcitation` pc
             ON pc.`patent_id` = p.`patent_id`;


# END uspatentcitation

########################################################################################################################################
