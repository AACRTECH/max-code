
CREATE SCHEMA IF NOT EXISTS IDR.DEDUPED;


--For person
--prod: IDR.DEDUPED.PERSON_DEDUPED 
CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_PERSON_DD
AS
with KeepEmails AS (
    SELECT 
        "PersonId" 
    FROM IDR.RESULTS.EMAIL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "Email" ORDER BY "PersonId" ASC) = 1
),
NullFontevaEmails AS (
    SELECT 
        A."Id" AS "PersonId"
    FROM IDR.RESULTS.PERSON A
        JOIN TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA B
            ON A.ORIGIN_LEGACY_ID = B."Id"
    WHERE (B."OrderApi__Work_Email__c" IS NULL AND B."OrderApi__Personal_Email__c" IS NULL AND B."OrderApi__Other_Email__c" IS NULL)
)
SELECT 
    * 
FROM IDR.RESULTS.PERSON
WHERE "Id" IN (SELECT "PersonId" FROM KeepEmails)
OR "Id" IN (SELECT "PersonId" FROM NullFontevaEmails);


SELECT * FROM TEST.TEST_AREA.IDR_PERSON_DD;


CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_UNIQUE_ID_DEDUPED
AS 
SELECT * FROM IDR.RESULTS.UNIQUEID;



SELECT COUNT(DISTINCT "Id") FROM IDR.RESULTS.PERSON;


--Correct uniqueid table
DELETE FROM TEST.TEST_AREA.IDR_UNIQUE_ID_DEDUPED t
WHERE NOT EXISTS (
    SELECT 1
    FROM TEST.TEST_AREA.IDR_PERSON_DD d
    WHERE t."PersonId" = d."Id"
);

SELECT * FROM TEST.TEST_AREA.IDR_UNIQUE_ID_DEDUPED;


SELECT * FROM IDR.RESULTS.UNIQUEID;


/*scratch bullshit */
SELECT 
    *
FROM IDR.RESULTS.PERSON WHERE "Id" NOT IN (SELECT "PersonId" FROM IDR.RESULTS.UNIQUEID)
ORDER BY "FirstName", "LastName";






CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.PERSON_DUPLICATES
AS
with AllPeople AS (
    SELECT
        P."Id" AS PERSON_ID,
        P."FirstName",
        P."Middle_Name__c",
        P."LastName",
        P."CompanyName",
        P."ORIGIN_LEGACY_ID",
        U."PersonId" AS "UID_PersonId",
        U."SourceId",
        U."SourceSystem",
        E."Id" AS EMAIL_ID,
        E."Email",
        E."EmailType",
        G."Id" AS FONTEVA_ID,
        G.AACR_ID,
        G."FirstName" AS "FirstName_FONTEVA",
        G."MiddleName" AS "MiddleName_FONTEVA",
        G."LastName" AS "LastName_FONTEVA"
    FROM IDR.RESULTS.PERSON P
        left JOIN IDR.RESULTS.UNIQUEID U
            ON P."Id" = U."PersonId"
            AND U."SourceSystem" = 'Fonteva'
        Left JOIN IDR.RESULTS.EMAIL E
            ON P."Id" = E."Id"
        Left join test.data_for_idr_seeding.person_data G
            on U."SourceId" = G."Id"
    where G."AACR_ID" is not null
    ORDER BY G."AACR_ID", P."Id"
),
Dupes AS (
    SELECT * FROM ALLPEOPLE WHERE FONTEVA_ID IN (SELECT FONTEVA_ID FROM ALLPEOPLE GROUP BY 1 HAVING COUNT(*) > 1)
)
SELECT
    Dupes.*,
    B."GoldenRecordId"
FROM Dupes
    LEFT JOIN PRODUCTION.MART_DATACLOUD.MARKETINGCLOUD_LEGACYMASTERCONTACTSLIST B
        ON Dupes.AACR_ID = B.AACRID
;













--
SELECT * FROM TEST.TEST_AREA.PERSON_DUPLICATES;

CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.EMAIL_PID_CORRECTED
AS 
with Mins AS (
    SELECT
        AACR_ID,
        MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC) AS KEEP_PERSON_ID
    FROM TEST.TEST_AREA.PERSON_DUPLICATES
)
    SELECT
        DISTINCT A.ORIGIN_LEGACY_ID,
        A.AACR_ID,
        B.KEEP_PERSON_ID
    FROM TEST.TEST_AREA.PERSON_DUPLICATES A
        LEFT JOIN Mins AS B
            ON A.AACR_ID = B.AACR_ID
    ORDER BY A.AACR_ID;




UPDATE IDR.RESULTS.EMAIL e
SET "PersonId" = c."KEEP_PERSON_ID"
FROM IDR.BACKUP_TABLES.EMAIL_PID_CORRECTED c
WHERE LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
  AND NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1);


SELECT * FROM IDR.RESULTS.EMAIL;

--Person
DELETE FROM IDR.RESULTS.PERSON A
WHERE A."Id" IN (SELECT OLD_PERSON_ID FROM IDR.BACKUP_TABLES.PERSON_RECORDS_TO_DELETE);

SELECT * FROM IDR.RESULTS.UNIQUEID;


/*Both tables for EMAIL */

--Fonteva
with E AS ( --Unpivots email fields for new row for each email per Fonteva ID from Cass' view
    SELECT 
        "Id", --Fonteva ID
        email_address AS "Email",
        email_type AS "ET",
        CURRENT_TIMESTAMP AS "CreatedDate"
    FROM TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA --Cass' view
    UNPIVOT (email_address FOR email_type IN (
        "OrderApi__Work_Email__c",
        "OrderApi__Personal_Email__c",
        "OrderApi__Other_Email__c"
        )) AS unpvt
    WHERE email_address IS NOT NULL
    ),
People AS ( --Grabs PersonId from Person IDR table, joins Cass' view on Fonteva ID = Origin Legacy ID
    SELECT 
        P."Id" AS "PersonId",
        E."Email",
        CASE 
            WHEN E."ET" = 'OrderApi__Work_Email__c' THEN 'Work'
            WHEN E."ET" = 'OrderApi__Personal_Email__c' THEN 'Personal'
            WHEN E."ET" = 'OrderApi__Other_Email__c' THEN 'Other'
        END AS "EmailType",
        E."CreatedDate"
    FROM IDR.RESULTS.PERSON AS P
        JOIN E
            ON P.ORIGIN_LEGACY_ID = E."Id" --E."Id" is fonteva ID, ORIGING_LEGACY_ID should match for data added yesterday
    )
SELECT
    *
FROM People;

--NPC
with CE AS (
    SELECT 
        "Id",
        A."npe01__Preferred_Email__c" AS "PrefEmailType",
        CASE
            WHEN A."npe01__Preferred_Email__c" = 'Personal' THEN A."OrderApi__Personal_Email__c"
            WHEN A."npe01__Preferred_Email__c" = 'Work' THEN A."OrderApi__Work_Email__c"
            ELSE A."OrderApi__Other_Email__c"
        END AS "PreferredEmail"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS A
)
SELECT
    P."Id" AS "PersonId",
    P.ORIGIN_LEGACY_ID AS "Email",
    CE."PrefEmailType" AS "EmailType",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM IDR.RESULTS.PERSON P
    LEFT JOIN CE
        ON P.ORIGIN_LEGACY_ID = CE."PreferredEmail"
WHERE P.ORIGIN_LEGACY_ID LIKE '%@%';


--Deduped?
with KeepEmails AS (
    SELECT 
        "PersonId" 
    FROM IDR.RESULTS.EMAIL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "Email" ORDER BY "PersonId" ASC) = 1
),
NullFontevaEmails AS (
    SELECT 
        A."Id" AS "PersonId"
    FROM IDR.RESULTS.PERSON A
        JOIN TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA B
            ON A.ORIGIN_LEGACY_ID = B."Id"
    WHERE (B."OrderApi__Work_Email__c" IS NULL AND B."OrderApi__Personal_Email__c" IS NULL AND B."OrderApi__Other_Email__c" IS NULL)
),
Finals AS (
    SELECT 
        * 
    FROM IDR.RESULTS.PERSON 
    WHERE "Id" IN (SELECT "PersonId" FROM KeepEmails)
    OR "Id" IN (SELECT "PersonId" FROM NullFontevaEmails)
),
Luigi AS (
    SELECT 
        A.*, B.*
    FROM IDR.RESULTS.EMAIL A
        JOIN Finals B
            ON A."PersonId" = B."Id"
)
SELECT * FROM Luigi;       



SELECT * FROM IDR.RESULTS.UNIQUEID;

SELECT "SourceSystem", COUNT(*) FROM IDR.RESULTS.UNIQUEID GROUP BY 1 ORDER BY 2 DESC;

SELECT * FROM IDR.RESULTS.EMAIL;


SELECT * FROM PRODUCTION.MART_DATACLOUD.MARKETINGCLOUD_LEGACYMASTERCONTACTSLIST;


SELECT * FROM PRODUCTION.MART_DATACLOUD.MARKETINGCLOUD_LEGACYMASTERCONTACTSLIST 
WHERE "GoldenRecordId" NOT IN (SELECT "PersonId" FROM IDR.RESULTS.UNIQUEID);


SELECT
    A."PersonId",
    A."Email",
    A."EmailType"
FROM IDR.RESULTS.EMAIL A
    LEFT JOIN IDR.RESULTS.UNIQUEID B
        ON A."PersonId" = B."PersonId"
WHERE B."PersonId" IS NULL;



CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.UNIQUE_ID_TEST
AS
SELECT * FROM IDR.RESULTS.UNIQUEID
;


/**********************************************************

HERE IS WHERE WE INSERT NEW CONTACTS INTO UNIQUEID

***********************************************************/

INSERT INTO TEST.TEST_AREA.UNIQUE_ID_TEST (
    "PersonId",
    "SourceId",
    "SourceSystem",
    "CreatedDate"
)
WITH unpivoted_contact as (
    -- UNPIVOT contact to join onto idr.email
    SELECT DISTINCT
        unpivoted."Id" AS CONTACT_ID,
        unpivoted.EMAIL AS EMAIL,
        unpivoted.EMAIL_TYPE AS EMAIL_TYPE
    FROM (
        SELECT
            "Id",
            EMAIL,
            EMAIL_TYPE,
            "AccountId"
        FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT
        UNPIVOT(EMAIL FOR EMAIL_TYPE IN(
            "OrderApi__Personal_Email__c",
            "OrderApi__Work_Email__c",
            "OrderApi__Other_Email__c"
        ))
    ) AS unpivoted
        LEFT JOIN production.repl_salesforce_ownbackup.account a
            ON unpivoted."AccountId" = a."Id"
    WHERE 
        EMAIL IS NOT NULL
        AND EMAIL NOT ILIKE '%aacr.test%'
        AND EMAIL NOT ILIKE '%aacrtest%'
        AND a."Name" NOT ILIKE '%aacr test%'
),
idr_email_notin_uniqueid AS (
    -- all idr.email records not in uniqeid table
    SELECT
        A."PersonId",
        A."Email",
        A."EmailType"
    FROM IDR.RESULTS.EMAIL A
        LEFT JOIN IDR.RESULTS.UNIQUEID B
            ON A."PersonId" = B."PersonId"
    WHERE B."PersonId" IS NULL
),
idr_fonteva_email_notin_uniqueid AS (
    -- all idr.email records with fonteva emails without ids in uniqueid
    SELECT
        TRUE AS IN_FONTEVA,
        B.CONTACT_ID,
        A."PersonId"
    FROM idr_email_notin_uniqueid A
        LEFT JOIN unpivoted_contact B
            ON A."Email" = B.EMAIL
),
Final AS (
    SELECT DISTINCT
        a."Id",
        B."PersonId" AS UNIQEID_PERSONID,
        F."PersonId" AS EMAIL_PERSONID,
        A."Member_Type__c"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT A
        LEFT JOIN IDR.RESULTS.UNIQUEID B
            ON A."Id" = B."SourceId"
        LEFT JOIN TEST.TEST_AREA.ATTAIN_CONTACT_EXCLUSION C
            ON A."Id" = C.ID
        LEFT JOIN idr_fonteva_email_notin_uniqueid F
            ON A."Id" = F.CONTACT_ID
    WHERE C.ID IS NULL
    AND F.IN_FONTEVA = TRUE
)
SELECT 
    "EMAIL_PERSONID" AS "PersonId",
    "Id" AS "SourceId",
    'Fonteva' AS "SourceSystem",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM Final;
--Are there going to be dupes? Should we pull in email too?


SELECT COUNT(*), COUNT(DISTINCT "PersonId") FROM TEST.TEST_AREA.UNIQUE_ID_TEST;

SELECT * FROM TEST.TEST_AREA.UNIQUE_ID_TEST 
WHERE "PersonId" IN (SELECT "PersonId" FROM TEST.TEST_AREA.UNIQUE_ID_TEST GROUP BY 1 HAVING COUNT(*) > 1)
ORDER BY 1;

/*
66681

0031I00001DFlfNQAT
0031I000013kKc5QAE

 */

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" IN ('0031I00001DFlfNQAT', '0031I000013kKc5QAE');

SELECT "SourceId" FROM TEST.TEST_AREA.UNIQUE_ID_TEST GROUP BY 1 HAVING COUNT(DISTINCT "PersonId") > 1;


SELECT * FROM TEST.TEST_AREA.UNIQUE_ID_TEST WHERE "SourceId" = '0031I00000WrwvkQAB';

/***********************************

DEDUPE BASED ON FONTEVA ID

***********************************/

CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.UNIQUE_ID_TEST_DEDUPED
AS
SELECT * FROM TEST.TEST_AREA.UNIQUE_ID_TEST QUALIFY ROW_NUMBER() OVER (PARTITION BY "SourceId" ORDER BY "PersonId" ASC) = 1;

DELETE FROM TEST.TEST_AREA.UNIQUE_ID_TEST 
WHERE "PersonId" NOT IN (SELECT "PersonId" FROM TEST.TEST_AREA.UNIQUE_ID_TEST_DEDUPED);


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C 
WHERE "Name" LIKE '%Test%'  
OR "Name" LIKE '%test%';

SELECT * FROM TEST.TEST_AREA.UNIQUE_ID_TEST;

SELECT * FROM IDR.RESULTS.UNIQUEID;

SELECT 
    COUNT(DISTINCT "Golden_Record_Id")
FROM PRODUCTION.MART_DATACLOUD.SUBSCRIBER_MASTER A
    JOIN IDR.RESULTS.UNIQUEID B
        ON A."Golden_Record_Id" = B."PersonId"
WHERE "Membership_Status" = 'Current';


SELECT 
    COUNT(DISTINCT "Golden_Record_Id")
FROM PRODUCTION.MART_DATACLOUD.SUBSCRIBER_MASTER A
    JOIN TEST.TEST_AREA.UNIQUE_ID_TEST B
        ON A."Golden_Record_Id" = B."PersonId"
WHERE "Membership_Status" = 'Current';

SELECT 
    DISTINCT "Membership_Status"
FROM PRODUCTION.MART_DATACLOUD.SUBSCRIBER_MASTER;


with Subs AS (
select DISTINCT
    CAST(u."Id" AS VARCHAR) as "Golden_Record_Id"
    , c."FirstName" as "First_Name"
    , c."LastName" as "Last_Name"
    , c."Salutation"
    , c."OrderApi__Preferred_Email_Type__c" as "Preferred_Email_Type"
    , case 
        when c."OrderApi__Preferred_Email_Type__c" = 'Work' and c."OrderApi__Work_Email__c" is not null then c."OrderApi__Work_Email__c"
        when c."OrderApi__Preferred_Email_Type__c" = 'Personal' and c."OrderApi__Personal_Email__c" is not null then c."OrderApi__Personal_Email__c"
        when c."OrderApi__Preferred_Email_Type__c" = 'Other' and c."OrderApi__Other_Email__c" is not null then c."OrderApi__Other_Email__c"
        else c."Email" 
        end as "Preferred_Email_Address"
    , TRUE as "Email_Is_Primary"
    , c."Member_Type__c" as "Membersip_Type"
    , c."Membership_Status__c" as "Membership_Status"
    , c."Initial_Join_Date__c" as "Member_Join_Date"
    , c."Paid_thru_date__c" as "Member_Paid_Thru_Date"
    , '[Place Holder]' as "Member_Number"
    , TRUE as "Primary_Membership"
    , c."DonorApi__Do_Not_Solicit__c" as "Do_Not_Solicit"
    , FALSE as "Do_Not_Contact"
    , c."MailingCity" as "City"
    , c."MailingState" as "State"
    , c."MailingCountry" as "Country"
    , c."Primary_Research_Area_of_Expertise__c" as "Primary_Research_Area"
    , c."Professional_Role__c" as "Professional_Role"
    , c."Work_Setting__c" as "Work_Setting"
    , c."Highest_Degree__c" as "Highest_Degree"
    , c."Current_Education_Status__c" as "Current_Education_Status"
    , c."Pre_Post_Doc__c" as "Pre_Post_Doc"
    , c."Advocate__c" as "Advocate"
    , c."Family_Member_with_Cancer__c" as "Family_Member_with_Cancer"
    , c."Foundation_Do_Not_Solicit__c" as "Foundation_Only"
    , a."Name" as "Organization_Name"
    , a."Institution_Type__c" as "Organization_Industry"
    , c."LeadSource" as "Source"
    , c."Id" as "Fonteva_Id"
from PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT c
join IDR.RESULTS.EMAIL u
on c."Email" = u."Email"
    OR u."Email" = c."OrderApi__Work_Email__c"
            OR u."Email" = c."OrderApi__Personal_Email__c"
            OR u."Email" = c."OrderApi__Other_Email__c"
left join PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT a
on c."npsp__Primary_Affiliation__c" = a."Id"
and c."EmailBouncedDate" is null and c."Membership_Status__c" <> 'Deceased' and c."Do_Not_Email__c" = 0
)
select COUNT(*) from Subs WHERE "Golden_Record_Id" IS NOT NULL;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT C WHERE C."Membership_Status__c" IN ('Current', 'Suspended');


SELECT "Email" FROM IDR.RESULTS.EMAIL GROUP BY 1 HAVING COUNT(DISTINCT "PersonId") > 1; 



/*PJ Ask */
--0. everyone who matched
SELECT
    COUNT(DISTINCT "Fonteva_Id"), --540563
    COUNT(DISTINCT "Golden_Record_Id") --587707
FROM TEST.TEST_AREA.SUBSCRIBER_TEST A
WHERE A."Golden_Record_Id" IS NOT NULL;

SELECT
    A."Fonteva_Id",
    A."Golden_Record_Id",
    A."First_Name",
    A."Last_Name",
    A."Preferred_Email_Address",
    A."Preferred_Email_Type"
FROM TEST.TEST_AREA.SUBSCRIBER_TEST A
WHERE A."Golden_Record_Id" IS NOT NULL
ORDER BY 1, 2;


--1. contacts who are not matching at all
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT 
WHERE "Id" NOT IN (SELECT DISTINCT "Fonteva_Id" FROM TEST.TEST_AREA.SUBSCRIBER_TEST WHERE "Golden_Record_Id" IS NOT NULL);

--2. records in PERSON table that don't show up in EMAIL
SELECT 
    * 
FROM IDR.RESULTS.PERSON WHERE "Id" NOT IN (SELECT "PersonId" FROM IDR.RESULTS.EMAIL)
AND ORIGIN_LEGACY_ID IS NOT NULL;

--do contacts in 1. match 2.?
with Fonteva AS (
    SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT 
    WHERE "Id" NOT IN (SELECT DISTINCT "Fonteva_Id" FROM TEST.TEST_AREA.SUBSCRIBER_TEST WHERE "Golden_Record_Id" IS NOT NULL)
),
IDRs AS (
    SELECT 
        * 
    FROM IDR.RESULTS.PERSON WHERE "Id" NOT IN (SELECT "PersonId" FROM IDR.RESULTS.EMAIL)
    AND ORIGIN_LEGACY_ID IS NOT NULL
)
SELECT * FROM Fonteva JOIN IDRs ON Fonteva."Id" = IDRs.ORIGIN_LEGACY_ID;


with Subs AS (
    SELECT  
        "Fonteva_Id",
        MIN("Golden_Record_Id") AS "GRID"
    FROM TEST.TEST_AREA.SUBSCRIBER_TEST
    GROUP BY 1
),
IDRNoEmail AS (
    SELECT 
        ORIGIN_LEGACY_ID AS "Fonteva_Id",
        "Id" AS "GRID"
    FROM IDR.RESULTS.PERSON WHERE "Id" NOT IN (SELECT "PersonId" FROM IDR.RESULTS.EMAIL)
    AND ORIGIN_LEGACY_ID IS NOT NULL
),
Final AS (
    SELECT * FROM Subs
    UNION ALL 
    SELECT * FROM IDRNoEmail
)
SELECT 
    "Fonteva_Id",
    MIN("GRID")
FROM Final
GROUP BY 1;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT;
