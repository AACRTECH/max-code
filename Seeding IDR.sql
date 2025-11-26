create table TEST_IDR_PJ_PERSON as 
select IDR.RESULTS.PERSON



SELECT * FROM IDR.RESULTS.PERSON;

SELECT *
FROM IDR.RESULTS.EMAIL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY "Email"
    ORDER BY "PersonId" DESC
) = 1
AND COUNT(*) OVER (PARTITION BY "Email") > 1;

--Frst Initial: LEFT()FirstName, 1)
--LastName
--Email

INSERT INTO IDR.RESULTS.UNIQUEID (
    "PersonId",
	"SourceId",
	"SourceSystem",
	"CreatedDate"
)
with FontevaData AS (
    SELECT
        "Id" AS FONTEVA_ID,
    	AACR_ID,
    	"FirstName",
    	"MiddleName",
    	"LastName",
    	"OrderApi__Work_Email__c",
    	"OrderApi__Personal_Email__c",
    	"OrderApi__Other_Email__c",
    	"OrderApi__Work_Phone__c",
    	"MobilePhone",
    	"HomePhone",
    	"OtherPhone",
        'Fonteva' AS SOURCE_SYSTEM,
        CONCAT(LEFT("FirstName", 1), "LastName") AS UNIQUE_ID
    FROM TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA
),
NPCData AS (
    SELECT 
        C.*,
        E."Email",
        CONCAT(LEFT(C."FirstName", 1), C."LastName") AS UNIQUE_ID
    FROM IDR.RESULTS.PERSON AS C
        LEFT JOIN IDR.RESULTS.EMAIL AS E
            ON C."Id" = E."PersonId"
)
SELECT  
    B."Id" AS PersonId,
    A.FONTEVA_ID AS SOURCEID,
    A.SOURCE_SYSTEM AS SourceSystem,
    CURRENT_TIMESTAMP AS CreatedDate
FROM FontevaData AS A
    JOIN NPCData AS B
        ON A.UNIQUE_ID = B. UNIQUE_ID
        AND (B."Email" = A."OrderApi__Work_Email__c"
            OR B."Email" = A."OrderApi__Personal_Email__c"
            OR B."Email" = A."OrderApi__Other_Email__c"
            )
;

SELECT * FROM IDR.RESULTS.UNIQUEID

/*

	"PersonId" NUMBER(38,0) NOT NULL,
	"SourceId" VARCHAR(16777216) NOT NULL,
	"SourceSystem" VARCHAR(16777216) NOT NULL,
	"CreatedDate" TIMESTAMP_LTZ(9),
	constraint ID_PK primary key ("SourceSystem", "SourceId"),
	constraint PID_FK foreign key ("PersonId") references IDR.RESULTS.PERSON(Id)
*/


--Fonteva ID + Golden Record ID







        
/*
"OrderApi__Work_Email__c",
    "OrderApi__Personal_Email__c",
    "OrderApi__Other_Email__c",
    "OrderApi__Work_Phone__c",
*/
    
CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_PERSON_SEED_TEST_TABLE
AS 
SELECT * FROM IDR.RESULTS.PERSON;


CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE
AS 
SELECT * FROM IDR.RESULTS.EMAIL;



SELECT * FROM TEST.TEST_AREA.IDR_PERSON_SEED_TEST;

/*
"Id",
	AACR_ID,
	"FirstName",
	"MiddleName",
	"LastName",
	"OrderApi__Work_Email__c",
	"OrderApi__Personal_Email__c",
	"OrderApi__Other_Email__c",
	"OrderApi__Work_Phone__c",
	"MobilePhone",
	"HomePhone",
	"OtherPhone"

    "FirstName" VARCHAR(16777216),
	"Middle_Name__c" VARCHAR(16777216),
	"LastName" VARCHAR(16777216),
	"CompanyName" VARCHAR(16777216),
	CREATEDDATE TIMESTAMP_LTZ(9) NOT NULL,
	LASTMODIFIED TIMESTAMP_NTZ(9),
	ORIGIN_LEGACY_ID VARCHAR(16777216),
*/



INSERT INTO IDR.RESULTS.PERSON(
    "FirstName",
	"Middle_Name__c",
	"LastName",
	"CompanyName",
	CREATEDDATE,
	LASTMODIFIED,
	ORIGIN_LEGACY_ID
)
SELECT 
    "FirstName",
    "MiddleName" AS "Middle_Name__c",
    "LastName",
    "ACCOUNT_NAME" AS "CompanyName",
    CURRENT_TIMESTAMP AS CreatedDate,
    CURRENT_TIMESTAMP AS LastModified,
    "Id" AS ORIGIN_LEGACY_ID
FROM TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA
WHERE "Id" NOT IN (SELECT "SourceId" FROM IDR.RESULTS.UNIQUEID);

SELECT 
    ORIGIN_LEGACY_ID
FROM TEST.TEST_AREA.IDR_PERSON_SEED_TEST_TABLE
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT * FROM IDR.RESULTS.PERSON;


SELECT * FROM IDR.RESULTS.EMAIL;

/*
Id
PersonId
Email
CreatedDate
*/


INSERT INTO TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE (
    "Email",
    "CreatedDate"
)
SELECT 
    email_address AS "Email",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA
UNPIVOT (email_address FOR email_type IN (
    "OrderApi__Work_Email__c",
    "OrderApi__Personal_Email__c",
    "OrderApi__Other_Email__c"
    )) AS unpvt
WHERE email_address IS NOT NULL;

SELECT * FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE;






SELECT * FROM IDR.RESULTS.PERSON WHERE "Id" IS NULL;



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
SELECT * FROM People       
;




























/*
"OrderApi__Work_Email__c",
        "OrderApi__Personal_Email__c",
        "OrderApi__Other_Email__c"
*/


SELECT DISTINCT "EmailType" FROM IDR.RESULTS.EMAIL;






SELECT * FROM IDR.RESULTS.EMAIL;


--Create new email table
--Bring over email for people with email as OLID (marketing cloud)
--Bring over unpivoted thing from above
--Dedupe on first nitial, last name, email
--Keep smallest golden record id

/*

(t) create a duplicate of person and email table to test this
(t) empty the email table
(t) pivot the fonteva emails and insert
() grab person table and insert emails on origin id
() Merge the person table, email table, and marketing cloud table in a new table
() De-deuplicate the records keeping the earliest GRID where there is a marketing cloud id present
() Remove the people in the person table and email table whose golden record ID's no longer exist


*/


SELECT * FROM IDR.RESULTS.EMAIL;


--Create copy of email table
CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE
AS 
SELECT * FROM IDR.RESULTS.EMAIL;

SELECT * FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE; --421k


--Empty email table (copy)
TRUNCATE TABLE TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE; 

SELECT * FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE; --Empty


--Pivot emails from Cass' view and insert into email table (test)
INSERT INTO TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE (
    "PersonId",
    "Email",
    "EmailType",
    "CreatedDate"
)
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

SELECT * FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE; --154k


--Grab person table and insert emails (test)
SELECT * FROM IDR.RESULTS.PERSON WHERE ORIGIN_LEGACY_ID LIKE '%@%'; --Pull emails only/exclude legacy 003x ids


--get email type from REPL_SALESFORCE_OWNBACKUP.CONTACT
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
    --Email type?
    CE."PrefEmailType" AS "EmailType",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM IDR.RESULTS.PERSON P
    LEFT JOIN CE
        ON P.ORIGIN_LEGACY_ID = CE."PreferredEmail"
WHERE P.ORIGIN_LEGACY_ID LIKE '%@%';

--Insertion statement
INSERT INTO TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE (
    "PersonId",
    "Email",
    "EmailType",
    "CreatedDate"
)
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
    --Email type?
    CE."PrefEmailType" AS "EmailType",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM IDR.RESULTS.PERSON P
    LEFT JOIN CE
        ON P.ORIGIN_LEGACY_ID = CE."PreferredEmail"
WHERE P.ORIGIN_LEGACY_ID LIKE '%@%'; --added 408k


SELECT * FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE;

SELECT 
    COUNT(*),
    COUNT(DISTINCT "Email"),
    COUNT(DISTINCT "PersonId")
FROM TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE;


SELECT COUNT(*) FROM IDR.RESULTS.PERSON WHERE ORIGIN_LEGACY_ID IS NULL;


GRANT SELECT ON TEST.TEST_AREA.IDR_EMAIL_SEED_TEST_TABLE TO ROLE SYSADMIN;







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
   E.*
FROM IDR.RESULTS.EMAIL E
    JOIN CE
        ON E."Email" = CE."PreferredEmail";

SELECT * FROM IDR.RESULTS.EMAIL;


SELECT * FROM TEST.DATA_FOR_IDR_SEEDING.PERSON_DATA;








/****************************

Duplicates issue

****************************/


SELECT * FROM IDR.BACKUP_TABLES.UNIQUE_ID;






CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.UNIQUE_ID
AS 
SELECT * FROM IDR.RESULTS.UNIQUEID;


with UIDDeduped AS (
    SELECT * FROM IDR.BACKUP_TABLES.UNIQUE_ID
)



SELECT * FROM IDR.RESULTS.PERSON t
WHERE NOT EXISTS (
    SELECT 1
    FROM IDR.DEDUPED.PERSON_DEDUPED d
    WHERE t."Id" = d."Id"
);


select * from IDR.RESULTS.PERSON where "Id" in ('258646','200880')




CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.IDR_UNIQUE_ID_DEDUPED
AS 
SELECT * FROM IDR.RESULTS.UNIQUEID;


SELECT * FROM TEST.TEST_AREA.IDR_UNIQUE_ID_DEDUPED --427k


DELETE FROM IDR.RESULTS.UNIQUEID t
WHERE NOT EXISTS (
    SELECT 1
    FROM IDR.DEDUPED.PERSON_DEDUPED d
    WHERE t."PersonId" = d."Id"
);








--Duplicates list
with AllPeople AS (
    SELECT
        p.*,
        u.*,
        e.*,
        g.*,
        g."Id" as FONTEVA_ID
    FROM IDR.RESULTS.PERSON p
        left JOIN IDR.RESULTS.UNIQUEID u
            ON p."Id" = u."PersonId"
            AND u."SourceSystem" = 'Fonteva'
        Left JOIN IDR.RESULTS.EMAIL e
            ON p."Id" = e."Id"
        Left join test.data_for_idr_seeding.person_data g
            on u."SourceId" = g."Id"
    where g."AACR_ID" is not null
    ORDER BY g."AACR_ID", p."Id"
),
Dupes AS (
    SELECT * FROM ALLPEOPLE WHERE FONTEVA_ID IN (SELECT FONTEVA_ID FROM ALLPEOPLE GROUP BY 1 HAVING COUNT(*) > 1)
    --ORDER BY "AACR_ID", "PersonId"
)
SELECT
    *
FROM Dupes
    LEFT JOIN PRODUCTION.MART_DATACLOUD.MARKETINGCLOUD_LEGACYMASTERCONTACTSLIST B
        ON Dupes.AACR_ID = B.AACRID

;

--join marketing cloud table

--if source id is same, update record with lowest golden record id 

--update email table so that all records get updated with lowest golden record ID
--keep those in person table and unique id table, delete others 
--both emails on email talbe have lowest golden record id
--to keep table and to delete table

SELECT * FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES;

--To keep, i.e. Min Person IDs
SELECT
    AACR_ID,
    MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC)
FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES;

SELECT * FROM IDR.RESULTS.email WHERE "Email" IN (SELECT ORIGIN_LEGACY_ID FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES);


--this is the master table of corrected personids
with Mins AS (
    SELECT
        AACR_ID,
        MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC) AS KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES
)
SELECT
    DISTINCT A.ORIGIN_LEGACY_ID,
    A.AACR_ID,
    B.KEEP_PERSON_ID
FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES A
    LEFT JOIN Mins AS B
        ON A.AACR_ID = B.AACR_ID
ORDER BY A.AACR_ID;

SELECT * FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES WHERE AACR_ID = 325227 --4?


/* Testing */
with Mins AS (
    SELECT
        AACR_ID,
        MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC) AS KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES
),
EMAIL_PID_CORRECTED AS (
    SELECT
        DISTINCT A.ORIGIN_LEGACY_ID,
        A.AACR_ID,
        B.KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES A
        LEFT JOIN Mins AS B
            ON A.AACR_ID = B.AACR_ID
    ORDER BY A.AACR_ID
)
SELECT
  e."Email",
  e."PersonId"      AS old_person_id,
  c."KEEP_PERSON_ID" AS new_person_id,
  c.AACR_ID
FROM IDR.RESULTS.EMAIL e
JOIN EMAIL_PID_CORRECTED c
  ON LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
WHERE NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1)
AND e."PersonId" <> c."KEEP_PERSON_ID";


/*                       old     new    aacrid

jiye2785@naver.com	    309009	208458	296728
sshelake@its.jnj.com	309132	110668	311990
*/

SELECT * FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES WHERE AACR_ID = 311990; --looks fine



CREATE TABLE IF NOT EXISTS TEST.TEST_AREA.IDR_EMAILS_TO_CORRECT
AS
SELECT * FROM IDR.RESULTS.EMAIL;


CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.EMAIL_PID_CORRECTED
AS 
with Mins AS (
    SELECT
        AACR_ID,
        MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC) AS KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES
)
    SELECT
        DISTINCT A.ORIGIN_LEGACY_ID,
        A.AACR_ID,
        B.KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES A
        LEFT JOIN Mins AS B
            ON A.AACR_ID = B.AACR_ID
    ORDER BY A.AACR_ID;





--running on test email table
UPDATE TEST.TEST_AREA.IDR_EMAILS_TO_CORRECT e
SET "PersonId" = c."KEEP_PERSON_ID"
FROM IDR.BACKUP_TABLES.EMAIL_PID_CORRECTED c
WHERE LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
  AND NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1);


SELECT * FROM TEST.TEST_AREA.IDR_EMAILS_TO_CORRECT;





/* Testing PERSON */

CREATE OR REPLACE TABLE TEST.TEST_AREA.IDR_PERSON_FIXED
AS
SELECT * FROM IDR.RESULTS.PERSON;


--all ids that need to be deleted, i.e. old id is different than new (golden record id was changed)
CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.PERSON_RECORDS_TO_DELETE
AS 
with Mins AS (
    SELECT
        AACR_ID,
        MIN(PERSON_ID) OVER(PARTITION BY AACR_ID ORDER BY PERSON_ID ASC) AS KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES
),
EMAIL_PID_CORRECTED AS (
    SELECT
        DISTINCT A.ORIGIN_LEGACY_ID,
        A.AACR_ID,
        B.KEEP_PERSON_ID
    FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES A
        LEFT JOIN Mins AS B
            ON A.AACR_ID = B.AACR_ID
    ORDER BY A.AACR_ID
)
SELECT
  e."Email",
  e."PersonId"      AS old_person_id,
  c."KEEP_PERSON_ID" AS new_person_id,
  c.AACR_ID
FROM IDR.RESULTS.EMAIL e
JOIN EMAIL_PID_CORRECTED c
  ON LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
WHERE NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1)
AND e."PersonId" <> c."KEEP_PERSON_ID";


DELETE FROM TEST.TEST_AREA.IDR_PERSON_FIXED A
WHERE A."Id" IN (SELECT OLD_PERSON_ID FROM IDR.BACKUP_TABLES.PERSON_RECORDS_TO_DELETE)

SELECT * FROM TEST.TEST_AREA.IDR_PERSON_FIXED;


/***********************************

PROD RUN

***********************************/

--Emails
UPDATE IDR.RESULTS.EMAIL e
SET "PersonId" = c."KEEP_PERSON_ID"
FROM IDR.BACKUP_TABLES.EMAIL_PID_CORRECTED c
WHERE LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
  AND NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1);


SELECT * FROM IDR.RESULTS.EMAIL;

--Person
DELETE FROM IDR.RESULTS.PERSON A
WHERE A."Id" IN (SELECT OLD_PERSON_ID FROM IDR.BACKUP_TABLES.PERSON_RECORDS_TO_DELETE);