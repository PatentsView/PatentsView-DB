CREATE TABLE IF NOT EXISTS `{{params.database}}`.`brf_sum_text_{{params.year}}`
(
    `uuid`           varchar(512) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`    varchar(32)     DEFAULT NULL,
    `text`         mediumtext CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `filename`     varchar(32) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `created_date` timestamp NULL  DEFAULT current_timestamp(),
    `updated_date` timestamp NULL  DEFAULT NULL
        ON UPDATE current_timestamp(),
    UNIQUE KEY `patent_id` (`patent_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
DROP TRIGGER IF EXISTS `{{params.database}}`.before_insert_brf;
CREATE TRIGGER `{{params.database}}`.before_insert_brf
    BEFORE INSERT
    ON `{{params.database}}`.`brf_sum_text_{{params.year}}`
    FOR EACH row
    SET new.uuid = uuid();

CREATE TABLE IF NOT EXISTS `{{params.database}}`.`claim_{{params.year}}`
(
    `uuid`           varchar(512) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`    varchar(32) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num`          varchar(16) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `text`         mediumtext CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `sequence`     int(11)         DEFAULT NULL,
    `dependent`    varchar(512) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `filename`     varchar(32) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `created_date` timestamp NULL  DEFAULT current_timestamp(),
    `updated_date` timestamp NULL  DEFAULT NULL
        ON UPDATE current_timestamp(),
    UNIQUE KEY `patent_id` (`patent_id`, `num`)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
DROP TRIGGER IF EXISTS `{{params.database}}`.before_insert_claim;
CREATE TRIGGER `{{params.database}}`.before_insert_claim
    BEFORE INSERT
    ON `{{params.database}}`.`claim_{{params.year}}`
    FOR EACH row
    SET new.uuid = uuid();

CREATE TABLE IF NOT EXISTS `{{params.database}}`.`detail_desc_text_{{params.year}}`
(
    `uuid`           varchar(512) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`    varchar(32)     DEFAULT NULL,
    `text`         mediumtext CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `length`       bigint(16)      DEFAULT NULL,
    `filename`     varchar(32) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `created_date` timestamp NULL  DEFAULT current_timestamp(),
    `updated_date` timestamp NULL  DEFAULT NULL
        ON UPDATE current_timestamp(),
    UNIQUE KEY `patent_id` (`patent_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
DROP TRIGGER IF EXISTS `{{params.database}}`.before_insert_ddt;
CREATE TRIGGER `{{params.database}}`.before_insert_ddt
    BEFORE INSERT
    ON `{{params.database}}`.`detail_desc_text_{{params.year}}`
    FOR EACH row
    SET new.uuid = uuid();

CREATE TABLE IF NOT EXISTS `{{params.database}}`.`draw_desc_text_{{params.year}}`
(
    `uuid`           varchar(512) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`    varchar(32)     DEFAULT NULL,
    `text`         mediumtext CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `sequence`     int(11)         DEFAULT NULL,
    `filename`     varchar(32) CHARACTER SET utf8mb4
        COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `created_date` timestamp NULL  DEFAULT current_timestamp(),
    `updated_date` timestamp NULL  DEFAULT NULL
        ON UPDATE current_timestamp(),
    KEY `patent_id` (`patent_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
DROP TRIGGER IF EXISTS `{{params.database}}`.before_insert_drawdt;
CREATE TRIGGER `{{params.database}}`.before_insert_drawdt
    BEFORE INSERT
    ON `{{params.database}}`.`draw_desc_text_{{params.year}}`
    FOR EACH row
    SET new.uuid = uuid();


CREATE TABLE IF NOT EXISTS `{{params.database}}`.`claim_exemplary_{{params.year}}`
(
    `exemplary` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `filename`  text COLLATE utf8mb4_unicode_ci DEFAULT NULL
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_unicode_ci;