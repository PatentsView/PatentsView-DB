CREATE TABLE _temp_cpc_current_old as
SELECT *
from patent_20200331.cpc_current
ORDER BY RAND()
LIMIT 10000;

SELECT *
from patent_20200630.cpc_current new_cpc
         join _temp_cpc_current_old tcco
              on new_cpc.patent_id = tcco.patent_id and new_cpc.sequence = tcco.sequence;


CREATE TABLE _temp_duplicated_cpc_patents as
SELECT count(1), patent_id, sequence
from cpc_current
group by patent_id, sequence
having count(1) > 1;

SELEcT count(1)
from _temp_duplicated_cpc_patents;

CREATE TABLE _temp_dedpulicated_cpc as
SELECT c2.*
from cpc_current c2
         join _temp_duplicated_cpc_patents tdcp on tdcp.patent_id = c2.patent_id and tdcp.sequence = c2.sequence;


DELETE c2
from cpc_current c2
         join _temp_duplicated_cpc_patents tdcp on tdcp.patent_id = c2.patent_id and tdcp.sequence = c2.sequence;



INSERT INTO cpc_current (uuid, patent_id, section_id, subsection_id, group_id, subgroup_id, category, sequence)
SELECT UUID(), x.*
from (SELECT distinct patent_id, section_id, subsection_id, group_id, subgroup_id, category, sequence
      from _temp_dedpulicated_cpc
      where patent_id is not null
        and patent_id <> '') x;

SELECT count(1)
from cpc_current c2
         left join patent p on c2.patent_id = p.id
where p.id is null; -- 7,846,536


SELECT c2.*
from cpc_current c2
         left join patent p on c2.patent_id = p.id
where p.id is null
limit 10;

DELETE c2
from cpc_current c2
         left join patent p on c2.patent_id = p.id
where p.id is null;

SELECT count(1)
from cpc_current
where section_id is null; -- 1,410,026

SELECT count(1), sequence
from cpc_current
where section_id is null
group by sequence;


SELECT count(1)
from cpc_current c2
         join (SELECT patent_id, min(sequence) seq
               from cpc_current
               where section_id is null
               group by patent_id) min_sequence_null
              on min_sequence_null.patent_id = c2.patent_id and c2.sequence > min_sequence_null.seq
where c2.section_id is not null; -- 0

SELECT count(1)
from cpc_current c2
         join (SELECT patent_id, min(sequence) seq
               from cpc_current
               where section_id is null
               group by patent_id) min_sequence_null
              on min_sequence_null.patent_id = c2.patent_id and c2.sequence > min_sequence_null.seq
where c2.section_id is null;-- 800k

EXPLAIN EXTENDED SELECT count(1)
from patent_assignee pa
         left join rawassignee r on pa.assignee_id = r.assignee_id
where r.assignee_id is null;