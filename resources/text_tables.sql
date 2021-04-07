{% set target_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set target_database = target_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}

    CREATE TABLE IF NOT EXISTS `{{target_database}}`.`brf_sum_text_{{year}}`
    (
        `uuid`              varchar(512) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `patent_id`         varchar(32) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `text`              longtext CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `version_indicator` date           DEFAULT NULL,
        `created_date`      timestamp NULL DEFAULT current_timestamp(),
        `updated_date`      timestamp NULL DEFAULT NULL
            ON UPDATE current_timestamp(),
        UNIQUE KEY `patent_id` (`patent_id`)
    )
        ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4;
    DROP TRIGGER IF EXISTS `{{target_database}}`.before_insert_brf;
    CREATE TRIGGER `{{target_database}}`.before_insert_brf
        BEFORE INSERT
        ON `{{target_database}}`.`brf_sum_text_{{year}}`
        FOR EACH row
        SET new.uuid = uuid();

    CREATE TABLE IF NOT EXISTS `{{target_database}}`.`claims_{{year}}`
    (
        `uuid`              varchar(512) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `patent_id`         varchar(32) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `num`               varchar(16) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `text`              longtext CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `sequence`          int(11)        DEFAULT NULL,
        `dependent`         varchar(512) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `exemplary`         int(32)        DEFAULT NULL,
        `version_indicator` date           DEFAULT NULL,
        `created_date`      timestamp NULL DEFAULT current_timestamp(),
        `updated_date`      timestamp NULL DEFAULT NULL
            ON UPDATE current_timestamp(),
        UNIQUE KEY `patent_id` (`patent_id`, `num`)
    )
        ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4;
    DROP TRIGGER IF EXISTS `{{target_database}}`.before_insert_claim;
    CREATE TRIGGER `{{target_database}}`.before_insert_claim
        BEFORE INSERT
        ON `{{target_database}}`.`claims_{{year}}`
        FOR EACH row
        SET new.uuid = uuid();

    CREATE TABLE IF NOT EXISTS `{{target_database}}`.`detail_desc_text_{{year}}`
    (
        `uuid`              varchar(512) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `patent_id`         varchar(32) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `text`              longtext CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `length`            bigint(16)     DEFAULT NULL,
        `version_indicator` date           DEFAULT NULL,
        `created_date`      timestamp NULL DEFAULT current_timestamp(),
        `updated_date`      timestamp NULL DEFAULT NULL
            ON UPDATE current_timestamp(),
        UNIQUE KEY `patent_id` (`patent_id`)
    )
        ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4;
    DROP TRIGGER IF EXISTS `{{target_database}}`.before_insert_ddt;
    CREATE TRIGGER `{{target_database}}`.before_insert_ddt
        BEFORE INSERT
        ON `{{target_database}}`.`detail_desc_text_{{year}}`
        FOR EACH row
        SET new.uuid = uuid();

    CREATE TABLE IF NOT EXISTS `{{target_database}}`.`draw_desc_text_{{year}}`
    (
        `uuid`              varchar(512) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `patent_id`         varchar(32) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `text`              longtext CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci     DEFAULT NULL,
        `sequence`          int(11)        DEFAULT NULL,
        `version_indicator` date           DEFAULT NULL,
        `created_date`      timestamp NULL DEFAULT current_timestamp(),
        `updated_date`      timestamp NULL DEFAULT NULL
            ON UPDATE current_timestamp(),
        KEY `patent_id` (`patent_id`)
    )
        ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4;
    DROP TRIGGER IF EXISTS `{{target_database}}`.before_insert_drawdt;
    CREATE TRIGGER `{{target_database}}`.before_insert_drawdt
        BEFORE INSERT
        ON `{{target_database}}`.`draw_desc_text_{{year}}`
        FOR EACH row
        SET new.uuid = uuid();


    CREATE TABLE IF NOT EXISTS `{{target_database}}`.`claim_exemplary_{{year}}`
    (
        `exemplary`         text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `patent_id`         varchar(32) CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci                      DEFAULT NULL,
        `version_indicator` date                            DEFAULT NULL,
        KEY `patent_id` (`patent_id`)

    )
        ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4
        COLLATE = utf8mb4_unicode_ci;