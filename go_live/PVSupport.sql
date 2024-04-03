{% set PVSupport_db = "PVSupport_webtool_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set PVSupport_API_db = "PVSupport_API_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

create database {{PVSupport_db}};

CREATE TABLE {{PVSupport_db}}.QueryDef(
    QueryDefId bigint unsigned NOT NULL,
    QueryString text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    Created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (QueryDefId) )
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE {{PVSupport_db}}.QueryResults (QueryDefId bigint unsigned NOT NULL,
 Sequence int unsigned NOT NULL,
 EntityId varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
 KEY EntityId (EntityId), KEY QueryDefId (QueryDefId), KEY Sequence (Sequence) )
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create database {{PVSupport_API_db}};

CREATE TABLE {{PVSupport_API_db}}.QueryDef(
    QueryDefId bigint unsigned NOT NULL,
    QueryString text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    Created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (QueryDefId) )
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE {{PVSupport_API_db}}.QueryResults (QueryDefId bigint unsigned NOT NULL,
 Sequence int unsigned NOT NULL,
 EntityId varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
 KEY EntityId (EntityId), KEY QueryDefId (QueryDefId), KEY Sequence (Sequence) )
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;