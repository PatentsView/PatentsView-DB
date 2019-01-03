# BEGIN assignee_inventor ######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_inventor`;

CREATE TABLE `PatentsView_20181127`.`assignee_inventor`
(
   `assignee_id`    INT UNSIGNED NOT NULL,
   `inventor_id`    INT UNSIGNED NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 4,352,502 @ 1:52

INSERT INTO `PatentsView_20181127`.`assignee_inventor`(`assignee_id`,
                                                       `inventor_id`,
                                                       `num_patents`)
     SELECT pa.assignee_id, pi.inventor_id, count(DISTINCT pa.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`patent_inventor` pi
               USING (patent_id)
   GROUP BY pa.assignee_id, pi.inventor_id;


# END assignee_inventor ######################################################################################################################


# BEGIN inventor_coinventor

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_coinventor`;

CREATE TABLE `PatentsView_20181127`.`inventor_coinventor`
(
   `inventor_id`      INT UNSIGNED NOT NULL,
   `coinventor_id`    INT UNSIGNED NOT NULL,
   `num_patents`      INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 16,742,248 @ 11:55

INSERT INTO `PatentsView_20181127`.`inventor_coinventor`(`inventor_id`,
                                                         `coinventor_id`,
                                                         `num_patents`)
     SELECT pi.inventor_id, copi.inventor_id, count(DISTINCT copi.patent_id)
       FROM `PatentsView_20181127`.`patent_inventor` pi
            INNER JOIN `PatentsView_20181127`.`patent_inventor` copi
               ON     pi.patent_id = copi.patent_id
                  AND pi.inventor_id <> copi.inventor_id
   GROUP BY pi.inventor_id, copi.inventor_id;


# END inventor_coinventor ######################################################################################################################


# BEGIN inventor_cpc_subsection

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_cpc_subsection`;

CREATE TABLE `PatentsView_20181127`.`inventor_cpc_subsection`
(
   `inventor_id`      INT UNSIGNED NOT NULL,
   `subsection_id`    VARCHAR(20) NOT NULL,
   `num_patents`      INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 7,171,415 @ 11:55

INSERT INTO `PatentsView_20181127`.`inventor_cpc_subsection`(`inventor_id`,
                                                             `subsection_id`,
                                                             `num_patents`)
     SELECT pi.inventor_id, c.subsection_id, count(DISTINCT c.patent_id)
       FROM `PatentsView_20181127`.`patent_inventor` pi
            INNER JOIN `PatentsView_20181127`.`cpc_current_subsection` c
               USING (patent_id)
      WHERE c.subsection_id IS NOT NULL AND c.subsection_id != ''
   GROUP BY pi.inventor_id, c.subsection_id;


# END inventor_cpc_subsection

######################################################################################################################


# BEGIN inventor_cpc_group

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_cpc_group`;

CREATE TABLE `PatentsView_20181127`.`inventor_cpc_group`
(
   `inventor_id`    INT UNSIGNED NOT NULL,
   `group_id`       VARCHAR(20) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 7,171,415 @ 11:55

INSERT INTO `PatentsView_20181127`.`inventor_cpc_group`(`inventor_id`,
                                                        `group_id`,
                                                        `num_patents`)
     SELECT pi.inventor_id, c.group_id, count(DISTINCT c.patent_id)
       FROM `PatentsView_20181127`.`patent_inventor` pi
            INNER JOIN `PatentsView_20181127`.`cpc_current_group` c
               USING (patent_id)
      WHERE c.group_id IS NOT NULL AND c.group_id != ''
   GROUP BY pi.inventor_id, c.group_id;


# END inventor_cpc_group

######################################################################################################################


# BEGIN inventor_nber_subcategory

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_nber_subcategory`;

CREATE TABLE `PatentsView_20181127`.`inventor_nber_subcategory`
(
   `inventor_id`       INT UNSIGNED NOT NULL,
   `subcategory_id`    VARCHAR(20) NOT NULL,
   `num_patents`       INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

#

INSERT INTO `PatentsView_20181127`.`inventor_nber_subcategory`(
               `inventor_id`,
               `subcategory_id`,
               `num_patents`)
     SELECT pi.inventor_id, n.subcategory_id, count(DISTINCT n.patent_id)
       FROM `PatentsView_20181127`.`nber` n
            INNER JOIN `PatentsView_20181127`.`patent_inventor` pi
               USING (patent_id)
      WHERE n.subcategory_id IS NOT NULL AND n.subcategory_id != ''
   GROUP BY pi.inventor_id, n.subcategory_id;


# END inventor_nber_subcategory

######################################################################################################################


# BEGIN inventor_uspc_mainclass

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_uspc_mainclass`;

CREATE TABLE `PatentsView_20181127`.`inventor_uspc_mainclass`
(
   `inventor_id`     INT UNSIGNED NOT NULL,
   `mainclass_id`    VARCHAR(20) NOT NULL,
   `num_patents`     INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 10,350,577 @ 14:44

INSERT INTO `PatentsView_20181127`.`inventor_uspc_mainclass`(`inventor_id`,
                                                             `mainclass_id`,
                                                             `num_patents`)
     SELECT pi.inventor_id, u.mainclass_id, count(DISTINCT pi.patent_id)
       FROM `PatentsView_20181127`.`patent_inventor` pi
            INNER JOIN `PatentsView_20181127`.`uspc_current_mainclass` u
               ON pi.patent_id = u.patent_id
   GROUP BY pi.inventor_id, u.mainclass_id;


# END inventor_uspc_mainclass

######################################################################################################################


# BEGIN inventor_year ######################################################################################################################

DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor_year`;

CREATE TABLE `PatentsView_20181127`.`inventor_year`
(
   `inventor_id`    INT UNSIGNED NOT NULL,
   `patent_year`    SMALLINT NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 8,140,017 @ 2:19

INSERT INTO `PatentsView_20181127`.`inventor_year`(`inventor_id`,
                                                   `patent_year`,
                                                   `num_patents`)
     SELECT pi.inventor_id, p.year, count(DISTINCT pi.patent_id)
       FROM `PatentsView_20181127`.`patent_inventor` pi
            INNER JOIN `PatentsView_20181127`.`patent` p USING (patent_id)
   GROUP BY pi.inventor_id, p.year;


# END inventor_year ######################################################################################################################


# BEGIN assignee_cpc_subsection

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_cpc_subsection`;

CREATE TABLE `PatentsView_20181127`.`assignee_cpc_subsection`
(
   `assignee_id`      INT UNSIGNED NOT NULL,
   `subsection_id`    VARCHAR(20) NOT NULL,
   `num_patents`      INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 933,903 @ 2:22

INSERT INTO `PatentsView_20181127`.`assignee_cpc_subsection`(`assignee_id`,
                                                             `subsection_id`,
                                                             `num_patents`)
     SELECT pa.assignee_id, c.subsection_id, count(DISTINCT c.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`cpc_current_subsection` c
               USING (patent_id)
      WHERE c.subsection_id IS NOT NULL AND c.subsection_id != ''
   GROUP BY pa.assignee_id, c.subsection_id;


# END assignee_cpc_subsection

######################################################################################################################


# BEGIN assignee_cpc_group

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_cpc_group`;

CREATE TABLE `PatentsView_20181127`.`assignee_cpc_group`
(
   `assignee_id`    INT UNSIGNED NOT NULL,
   `group_id`       VARCHAR(20) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 933,903 @ 2:22

INSERT INTO `PatentsView_20181127`.`assignee_cpc_group`(`assignee_id`,
                                                        `group_id`,
                                                        `num_patents`)
     SELECT pa.assignee_id, c.group_id, count(DISTINCT c.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`cpc_current_group` c
               USING (patent_id)
      WHERE c.group_id IS NOT NULL AND c.group_id != ''
   GROUP BY pa.assignee_id, c.group_id;


# END assignee_cpc_group

######################################################################################################################

# BEGIN assignee_nber_subcategory

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_nber_subcategory`;

CREATE TABLE `PatentsView_20181127`.`assignee_nber_subcategory`
(
   `assignee_id`       INT UNSIGNED NOT NULL,
   `subcategory_id`    VARCHAR(20) NOT NULL,
   `num_patents`       INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 618,873 @ 0:48

INSERT INTO `PatentsView_20181127`.`assignee_nber_subcategory`(
               `assignee_id`,
               `subcategory_id`,
               `num_patents`)
     SELECT pa.assignee_id, n.subcategory_id, count(DISTINCT n.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`nber` n USING (patent_id)
      WHERE n.subcategory_id IS NOT NULL AND n.subcategory_id != ''
   GROUP BY pa.assignee_id, n.subcategory_id;


# END assignee_nber_subcategory

######################################################################################################################


# BEGIN assignee_uspc_mainclass

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_uspc_mainclass`;

CREATE TABLE `PatentsView_20181127`.`assignee_uspc_mainclass`
(
   `assignee_id`     INT UNSIGNED NOT NULL,
   `mainclass_id`    VARCHAR(20) NOT NULL,
   `num_patents`     INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 1,534,644 @ 3:30

INSERT INTO `PatentsView_20181127`.`assignee_uspc_mainclass`(`assignee_id`,
                                                             `mainclass_id`,
                                                             `num_patents`)
     SELECT pa.assignee_id, u.mainclass_id, count(DISTINCT pa.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`uspc_current_mainclass` u
               ON pa.patent_id = u.patent_id
   GROUP BY pa.assignee_id, u.mainclass_id;


# END assignee_uspc_mainclass

######################################################################################################################


# BEGIN assignee_year ######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee_year`;

CREATE TABLE `PatentsView_20181127`.`assignee_year`
(
   `assignee_id`    INT UNSIGNED NOT NULL,
   `patent_year`    SMALLINT NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;

# 931,856 @ 2:00

INSERT INTO `PatentsView_20181127`.`assignee_year`(`assignee_id`,
                                                   `patent_year`,
                                                   `num_patents`)
     SELECT pa.assignee_id, p.year, count(DISTINCT pa.patent_id)
       FROM `PatentsView_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`patent` p USING (patent_id)
   GROUP BY pa.assignee_id, p.year;


# END assignee_year ######################################################################################################################


# BEGIN location_assignee update num_patents

###################################################################################################################################


# 434,823 @ 0:17

UPDATE `PatentsView_20181127`.`location_assignee` la
       INNER JOIN
       (  SELECT `location_id`,
                 `assignee_id`,
                 count(DISTINCT `patent_id`) num_patents
            FROM `PatentsView_20181127`.`patent_assignee`
        GROUP BY `location_id`, `assignee_id`) pa
          ON     pa.`location_id` = la.`location_id`
             AND pa.`assignee_id` = la.`assignee_id`
   SET la.`num_patents` = pa.`num_patents`;


# END location_assignee update num_patents

###################################################################################################################################


# BEGIN location_inventor update num_patents

###################################################################################################################################


# 4,167,939 @ 2:33

UPDATE `PatentsView_20181127`.`location_inventor` li
       INNER JOIN
       (  SELECT `location_id`,
                 `inventor_id`,
                 count(DISTINCT `patent_id`) num_patents
            FROM `PatentsView_20181127`.`patent_inventor`
        GROUP BY `location_id`, `inventor_id`) pii
          ON     pii.`location_id` = li.`location_id`
             AND pii.`inventor_id` = li.`inventor_id`
   SET li.`num_patents` = pii.`num_patents`;


# END location_assignee update num_patents

###################################################################################################################################


# BEGIN location_cpc_subsection

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_cpc_subsection`;

CREATE TABLE `PatentsView_20181127`.`location_cpc_subsection`
(
   `location_id`      INT UNSIGNED NOT NULL,
   `subsection_id`    VARCHAR(20) NOT NULL,
   `num_patents`      INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 1,077,971 @ 6:19

INSERT INTO `PatentsView_20181127`.`location_cpc_subsection`(`location_id`,
                                                             `subsection_id`,
                                                             `num_patents`)
     SELECT tlp.`location_id`,
            cpc.`subsection_id`,
            count(DISTINCT tlp.`patent_id`)
       FROM `PatentsView_20181127`.`temp_location_patent` tlp
            INNER JOIN `PatentsView_20181127`.`cpc_current_subsection` cpc
               USING (`patent_id`)
   GROUP BY tlp.`location_id`, cpc.`subsection_id`;


# END location_cpc_subsection

######################################################################################################################

# BEGIN location_cpc_group

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_cpc_group`;

CREATE TABLE `PatentsView_20181127`.`location_cpc_group`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `group_id`       VARCHAR(20) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 1,077,971 @ 6:19

INSERT INTO `PatentsView_20181127`.`location_cpc_group`(`location_id`,
                                                        `group_id`,
                                                        `num_patents`)
     SELECT tlp.`location_id`, cpc.`group_id`, count(DISTINCT tlp.`patent_id`)
       FROM `PatentsView_20181127`.`temp_location_patent` tlp
            INNER JOIN `PatentsView_20181127`.`cpc_current_group` cpc
               USING (`patent_id`)
   GROUP BY tlp.`location_id`, cpc.`group_id`;


# END location_cpc_group

######################################################################################################################


# BEGIN location_uspc_mainclass

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_uspc_mainclass`;

CREATE TABLE `PatentsView_20181127`.`location_uspc_mainclass`
(
   `location_id`     INT UNSIGNED NOT NULL,
   `mainclass_id`    VARCHAR(20) NOT NULL,
   `num_patents`     INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 2,260,351 @ 7:47

INSERT INTO `PatentsView_20181127`.`location_uspc_mainclass`(`location_id`,
                                                             `mainclass_id`,
                                                             `num_patents`)
     SELECT tlp.`location_id`,
            uspc.`mainclass_id`,
            count(DISTINCT tlp.`patent_id`)
       FROM `PatentsView_20181127`.`temp_location_patent` tlp
            INNER JOIN `PatentsView_20181127`.`uspc_current_mainclass` uspc
               USING (`patent_id`)
   GROUP BY tlp.`location_id`, uspc.`mainclass_id`;


# END location_uspc_mainclass

######################################################################################################################


# BEGIN location_nber_subcategory

######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_nber_subcategory`;

CREATE TABLE `PatentsView_20181127`.`location_nber_subcategory`
(
   `location_id`       INT UNSIGNED NOT NULL,
   `subcategory_id`    VARCHAR(20) NOT NULL,
   `num_patents`       INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


#

INSERT INTO `PatentsView_20181127`.`location_nber_subcategory`(
               `location_id`,
               `subcategory_id`,
               `num_patents`)
     SELECT tlp.`location_id`,
            nber.`subcategory_id`,
            count(DISTINCT tlp.`patent_id`)
       FROM `PatentsView_20181127`.`temp_location_patent` tlp
            INNER JOIN `PatentsView_20181127`.`nber` nber USING (`patent_id`)
   GROUP BY tlp.`location_id`, nber.`subcategory_id`;


# END location_nber_subcategory

######################################################################################################################


# BEGIN location_year ######################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_year`;

CREATE TABLE `PatentsView_20181127`.`location_year`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `year`           SMALLINT NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL
)
ENGINE = INNODB;


# 867,942 @ 1:19

INSERT INTO `PatentsView_20181127`.`location_year`(`location_id`,
                                                   `year`,
                                                   `num_patents`)
     SELECT tlp.`location_id`, p.`year`, count(DISTINCT tlp.`patent_id`)
       FROM `PatentsView_20181127`.`temp_location_patent` tlp
            INNER JOIN `PatentsView_20181127`.`patent` p USING (`patent_id`)
   GROUP BY tlp.`location_id`, p.`year`;


# END location_year ######################################################################################################################
