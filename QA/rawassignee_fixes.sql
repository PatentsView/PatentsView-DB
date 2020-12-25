SELECT *
from patent
where id = '3931374';

SELECT *
from patent_assignee
where patent_id = '3931374';

select p.`id`,
       ta.`new_assignee_id`,
       ta.`old_assignee_id`,
       tl.`new_location_id`,
       tl.`old_location_id_transformed`,
       nullif(l.`city`, ''),
       nullif(l.`state`, ''),
       nullif(l.`country`, ''),
       l.`latitude`,
       l.`longitude`
from (SELECT * from `patent_20200630`.`patent` where id = '3931374') p
         left outer join `patent_20200630`.`rawassignee` ra on ra.`patent_id` = p.`id` and ra.`sequence` = 0
         left outer join `PatentsView_20200630`.`temp_id_mapping_assignee` ta on ta.`old_assignee_id` = ra.`assignee_id`
         left outer join `patent_20200630`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
         left outer join `patent_20200630`.`location` l on l.`id` = rl.`location_id`
         left outer join `PatentsView_20200630`.`temp_id_mapping_location_transformed` tl
                         on tl.`old_location_id_transformed` = rl.`location_id_transformed`
where ta.`new_assignee_id` is not null
   or tl.`new_location_id` is not null;


CREATE TABLE temp_duplicated_rawassignee_rows as
SELECT count(1), patent_id, sequence
from rawassignee
group by patent_id, sequence
having count(1) > 1;

SELECT count(distinct patent_id, sequence)
from temp_duplicated_rawassignee_rows;
SELECT ra.*
from rawassignee ra
         join temp_duplicated_rawassignee_rows tdrr on ra.patent_id = tdrr.patent_id and tdrr.sequence = ra.sequence;
-- All  = 48151
-- Null =
SELECT count(1),
       patent_id,
       sequence,
       name_first,
       name_last,
       organization,
       type
from rawassignee
group by patent_id, sequence, name_first, name_last, organization, type
having count(1) > 1;
;


START TRANSACTION;
DELETE ra2
from rawassignee ra
         join
     rawassignee ra2
     on ra.patent_id <=> ra2.patent_id and ra.sequence <=> ra2.sequence and ra.name_first <=> ra2.name_first and
        ra.name_last <=> ra2.name_last and ra.organization <=> ra2.organization and ra.type <=> ra2.type
         join (SELECT count(1),
                      patent_id,
                      sequence,
                      name_first,
                      name_last,
                      organization,
                      type
               from rawassignee
               group by patent_id, sequence, name_first, name_last, organization, type
               having count(1) > 1) t
              on t.patent_id <=> ra2.patent_id and t.sequence <=> ra2.sequence and t.name_first <=> ra2.name_first and
                 t.name_last <=> ra2.name_last and t.organization <=> ra2.organization and t.type <=> ra2.type
where ra.uuid > ra2.uuid;


SELECT count(1),
       patent_id,
       sequence,
       name_first,
       name_last,
       organization,
       type
from rawassignee
group by patent_id, sequence, name_first, name_last, organization, type
having count(1) > 1;



SELECT count(1),
       patent_id,
       sequence,
       name_first,
       name_last,
       organization
from rawassignee
group by patent_id, sequence, name_first, name_last, organization
having count(1) > 1;

DELETE ra
from rawassignee ra
         join (
    SELECT count(1),
           patent_id,
           sequence,
           name_first,
           name_last,
           organization
    from rawassignee
    group by patent_id, sequence, name_first, name_last, organization
    having count(1) > 1
) tnew on ra.patent_id = tnew.patent_id and ra.sequence = tnew.sequence
         join rawassignee ra2 on ra2.patent_id = ra.patent_id and ra2.sequence = ra2.sequence
where (ra2.type is null and ra.type = 0)
   or (ra2.type != 0 and ra.type is null);


DELETE ra
from rawassignee ra
         join (SELECT count(1), patent_id, sequence
               from rawassignee
               group by patent_id, sequence
               having count(1) > 1) tlast on tlast.patent_id = ra.patent_id and tlast.sequence = ra.sequence
         join rawassignee ra2 on ra2.patent_id = ra.patent_id and ra2.sequence = ra2.sequence
where (ra2.type is null and ra.type = 0)
   or (ra2.type != 0 and ra.type is null);


SELECT count(1), patent_id, sequence
               from rawassignee
               group by patent_id, sequence
               having count(1) > 1;

DELETE  from rawassignee where patent_id in('H0000400','H0001805');
COMMIT;