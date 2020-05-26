-- Create syntax for TABLE 'brf_sum_text'
CREATE TABLE `temp_brf_sum_text` (
  `id`            varchar(512) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `patent_id` varchar(32)    DEFAULT NULL,
  `text`          mediumtext CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `filename`      varchar(32) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `created_date`  timestamp NULL DEFAULT current_timestamp(),
  `updated_date`  timestamp NULL DEFAULT NULL
  ON UPDATE current_timestamp(),
  UNIQUE KEY `patent_id` (`patent_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
CREATE TRIGGER before_insert_brf
  BEFORE INSERT
  ON temp_brf_sum_text
  FOR EACH row
  SET new.id = uuid();
-- Create syntax for TABLE 'claim'
CREATE TABLE `temp_claim` (
  `id`            varchar(512) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `patent_id` varchar(32)    CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `num`           varchar(16) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `text`          mediumtext CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `sequence`      int(11)        DEFAULT NULL,
  `dependent`     varchar(512) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `filename`      varchar(32) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `created_date`  timestamp NULL DEFAULT current_timestamp(),
  `updated_date`  timestamp NULL DEFAULT NULL
  ON UPDATE current_timestamp(),
  UNIQUE KEY `patent_id` (`patent_id`, `num`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
CREATE TRIGGER before_insert_claim
  BEFORE INSERT
  ON temp_claim
  FOR EACH row
  SET new.id = uuid();
-- Create syntax for TABLE 'detail_desc_text'
CREATE TABLE `temp_detail_desc_text` (
  `id`            varchar(512) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `patent_id` varchar(32)    DEFAULT NULL,
  `text`          mediumtext CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `length`        bigint(16)     DEFAULT NULL,
  `filename`      varchar(32) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `created_date`  timestamp NULL DEFAULT current_timestamp(),
  `updated_date`  timestamp NULL DEFAULT NULL
  ON UPDATE current_timestamp(),
  UNIQUE KEY `patent_id` (`patent_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
CREATE TRIGGER before_insert_ddt
  BEFORE INSERT
  ON temp_detail_desc_text
  FOR EACH row
  SET new.id = uuid();
-- Create syntax for TABLE 'draw_desc_text'
CREATE TABLE `temp_draw_desc_text` (
  `id`            varchar(512) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `patent_id` varchar(32)    DEFAULT NULL,
  `text`          mediumtext CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `filename`      varchar(32) CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
  `created_date`  timestamp NULL DEFAULT current_timestamp(),
  `updated_date`  timestamp NULL DEFAULT NULL
  ON UPDATE current_timestamp(),
  KEY `patent_id` (`patent_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TRIGGER before_insert_drawdt
  BEFORE INSERT
  ON temp_draw_desc_text
  FOR EACH row
  SET new.id = uuid();


CREATE TABLE `temp_claim_exemplary` (
  `exemplary`     text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `patent_id` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename`      text COLLATE utf8mb4_unicode_ci DEFAULT NULL
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;