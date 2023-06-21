select *
from
    `_root`.config
where
    `key` like '{{params.version_indicator}}';

SELECT
    `UpdateAPI_bulkdownloadstats`.`id`
  , `UpdateAPI_bulkdownloadstats`.`database_type`
  , `UpdateAPI_bulkdownloadstats`.`update_version`
  , `UpdateAPI_bulkdownloadstats`.`table`
  , `UpdateAPI_bulkdownloadstats`.`row_count`
  , `UpdateAPI_bulkdownloadstats`.`line_count`
  , `UpdateAPI_bulkdownloadstats`.`tsv_file_size`
  , `UpdateAPI_bulkdownloadstats`.`zip_file_size`
  , `UpdateAPI_bulkdownloadstats`.`original_size`
  , `UpdateAPI_bulkdownloadstats`.`upload_url`
  , `UpdateAPI_bulkdownloadstats`.`created`
  , `UpdateAPI_bulkdownloadstats`.`modified`
  , `UpdateAPI_bulkdownloadstats`.`live`
FROM
    `_root`.`UpdateAPI_bulkdownloadstats`
WHERE
    (`UpdateAPI_bulkdownloadstats`.`live` = 1 AND
    (`UpdateAPI_bulkdownloadstats`.`table` NOT IN ('detail\_desc', 'draw\_desc', 'claim', 'brf\_sum')))