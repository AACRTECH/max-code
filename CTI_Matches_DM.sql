CREATE SCHEMA TEST.CTI_MATCHES;


CREATE OR REPLACE VIEW TEST.CTI_MATCHES.NO_MATCH_v
AS
select a.*,b.golden_recordid
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null;

CREATE OR REPLACE VIEW TEST.CTI_MATCHES.MATCH_v
AS
select a.*,b.golden_recordid
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null;




-- Match for Invited Speakers
select a.*, b.golden_recordid from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null;

-- No match for Invited Speakers
select a.*, b.golden_recordid from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
;








--ABSTRACT PRESENTERS ALL
CREATE OR REPLACE VIEW TEST.CTI_MATCHES.ABSTRACT_PRESENTERS__v
AS
select a.*,b.*, false AS "MATCH" --these are non-matches, was b.golden_recordid
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
UNION ALL
select a.*,b.*, true AS "MATCH" --these are matches, was b.golden_recordid
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null;


--INVITED SPEAKERS ALL
CREATE OR REPLACE VIEW TEST.CTI_MATCHES.INVITED_SPEAKERS__v
AS
select a.*, b.*, true AS "MATCH" from --was b.golden_recordid
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null
UNION ALL 
select a.*, b.*, false AS "MATCH" from --was b.golden_recordid
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
;

SELECT * FROM TEST.CTI_MATCHES.ABSTRACT_PRESENTERS__V;

SELECT COUNT(*) FROM PRODUCTION.MART;



SELECT * FROM PRODUCTION.REPL_CTI.INVITED_SPEAKERS_CHAIRS WHERE AUTHOR_FIRST_NAME LIKE '%Nada%';


/*Determine if match+no match (together in each abstract and invited source separately)= totals (new tables) */



select a.*,b.*, false AS "MATCH" --these are non-matches
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
UNION ALL
select a.*,b.*, true AS "MATCH" --these are matches
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null;


CREATE SCHEMA PRODUCTION.MART_CTI_MATCHES;

CREATE OR REPLACE VIEW PRODUCTION.MART_CTI_MATCHES.ABSTRACT_PRESENTERS__v
AS
select a.*,b.*, false AS "MATCH" --these are non-matches
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
UNION ALL
select a.*,b.*, true AS "MATCH" --these are matches
--select count(*)
from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.abstract_presenters)  a left outer join
TEST.REPL_CTI.CTI_ABSTRACT_PRESENTER_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null;


CREATE OR REPLACE VIEW PRODUCTION.MART_CTI_MATCHES.INVITED_SPEAKERS__v
AS
select a.*, b.*, true AS "MATCH" from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is not null
UNION ALL 
select a.*, b.*, false AS "MATCH" from
(select *, dw_id || '-' || meeting_id || '-' || meeting_year || '-' || control_number || '-' || NVL(TO_CHAR(src_record_id),'0') as record_id from
PRODUCTION.repl_cti.invited_speakers_chairs) a left outer join
TEST.REPL_CTI.CTI_INVITED_SPEAKER_CHAIR_TO_MDH_CONTACT_IDENTIFIER b on
 a.record_id = b.SOURCE_ENTITYID
where b.source_entityid is null
;