/***********************************

PROD RUN BELOW

************************************/

SELECT * FROM IDR.RESULTS.EMAIL;
SELECT * FROM IDR.RESULTS.PERSON ORDER BY CREATEDDATE;


--create backup tables for PERSON and EMAIL
CREATE SCHEMA IF NOT EXISTS IDR.BACKUP_TABLES;

CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.EMAIL_BACKUP
AS 
SELECT * FROM IDR.RESULTS.EMAIL;

CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.PERSON_BACKUP
AS 
SELECT * FROM IDR.RESULTS.PERSON;


/* Step 1: Empty Email Table */
SELECT * FROM IDR.RESULTS.EMAIL;

--Empty email table
TRUNCATE TABLE IDR.RESULTS.EMAIL;

SELECT * FROM IDR.RESULTS.EMAIL; --confirm empty

/* Step 2: Insert Salesforce Data (all email fields for each contact from Fonteva, 1 row per different email) */
INSERT INTO IDR.RESULTS.EMAIL (
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


SELECT * FROM IDR.RESULTS.EMAIL; --confirm data has been inserted


/* Step 3: Insert Data from PERSON table where ORIGIN_LEGACY_ID is email address */
INSERT INTO IDR.RESULTS.EMAIL (
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
    CE."PrefEmailType" AS "EmailType",
    CURRENT_TIMESTAMP AS "CreatedDate"
FROM IDR.RESULTS.PERSON P
    LEFT JOIN CE
        ON P.ORIGIN_LEGACY_ID = CE."PreferredEmail"
WHERE P.ORIGIN_LEGACY_ID LIKE '%@%';


SELECT * FROM IDR.RESULTS.EMAIL; --check


/* Step 4: Find and Keep Non-Dupes */

CREATE SCHEMA IF NOT EXISTS IDR.DEDUPED;


--For person
CREATE OR REPLACE TABLE IDR.DEDUPED.PERSON_DEDUPED 
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


SELECT * FROM IDR.DEDUPED.PERSON_DEDUPED;

SELECT COUNT(DISTINCT "Id") FROM IDR.RESULTS.PERSON;


--Correct uniqueid table
DELETE FROM IDR.RESULTS.UNIQUEID t
WHERE NOT EXISTS (
    SELECT 1
    FROM IDR.DEDUPED.PERSON_DEDUPED d
    WHERE t."PersonId" = d."Id"
);


--For emails
CREATE OR REPLACE TABLE IDR.DEDUPED.EMAIL_DEDUPED 
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
),
Finals AS (
SELECT 
    * 
FROM IDR.RESULTS.PERSON 
WHERE "Id" IN (SELECT "PersonId" FROM KeepEmails)
OR "Id" IN (SELECT "PersonId" FROM NullFontevaEmails)
)
SELECT 
    A.* 
FROM IDR.RESULTS.EMAIL A
    JOIN Finals B
        ON A."PersonId" = B."Id";









/*
Person:
P."Id" AS PERSON_ID,
P."FirstName"
P."Middle_Name__c"
P."LastName"
P."CompanyName"
P."ORIGIN_LEGACY_ID"

UNIQUEID
U."PersonId"
U."SourceId"
U."SourceSystem"

EMAIL
E."Id" AS EMAIL_ID,
E."Email"
E."EmailType"

Cass View
G."Id",
G.AACR_ID,
G."FirstName_FONTEVA",
G."MiddleName_FONTEVA",
G."LastName_FONTEVA"






P."Id" AS PERSON_ID,
P."FirstName"
P."Middle_Name__c"
P."LastName"
P."CompanyName"
P."ORIGIN_LEGACY_ID"
U."UID_PersonId"
U."SourceId"
U."SourceSystem"
E."Id" AS EMAIL_ID,
E."Email"
E."EmailType"
G."Id" SD FONTEVA_ID,
G.AACR_ID,
G."FirstName_FONTEVA",
G."MiddleName_FONTEVA",
G."LastName_FONTEVA"





36551	115654
36551	115654
136609	303400
136609	303400
351694	212125
351694	212125

(36551, 136609, 351694)


 */


CREATE TABLE IF NOT EXISTS IDR.BACKUP_TABLES.PERSON_DUPLICATES
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

SELECT * FROM IDR.BACKUP_TABLES.PERSON_DUPLICATES;


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




UPDATE IDR.RESULTS.EMAIL e
SET "PersonId" = c."KEEP_PERSON_ID"
FROM IDR.BACKUP_TABLES.EMAIL_PID_CORRECTED c
WHERE LOWER(TRIM(e."Email")) = LOWER(TRIM(c."ORIGIN_LEGACY_ID"))
  AND NVL(e."PersonId", -1) <> NVL(c."KEEP_PERSON_ID", -1);


SELECT * FROM IDR.RESULTS.EMAIL;

--Person
DELETE FROM IDR.RESULTS.PERSON A
WHERE A."Id" IN (SELECT OLD_PERSON_ID FROM IDR.BACKUP_TABLES.PERSON_RECORDS_TO_DELETE);







select * from  LEAD_LISTS_DB.RESULTS_TABLE.RESULTS
WHERE ticket_number = 'Lead List 6';


/*
GOLDEN_RECORD_ID,
AACR_ID,
FIRST_NAME,
LAST_NAME,
GENDER,
RACE,
EMAIL,
COMPANY_NAME,
STREET,
STREET_2,
CITY,
STATE_PROVINCE,
COUNTRY,
ZIP,
MOBILE_NUMBER,
CONSENT_DECLARATION,
SOURCE_NAME,
DATE_COLLECTED,
LEAD_USE,
LEAD_DESCRIPTION,
TICKET_NUMBER,
SUBMITTER_NAME,
DEPARTMENT,
DOUBLE_OPT_IN


 */


SELECT C. FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS C ;





with DONORDRIVE AS (
    SELECT 
        *
    FROM PRODUCTION.REPL_DONORDRIVE.DD_DONATIONS_NEW
    WHERE "donationmodifieddate" > '2025-10-01'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY "donationtransactionid" ORDER BY "donationmodifieddate") = 1
)
SELECT 
    A."donationtransactionid" AS DD_TRANSACTION_ID,
    A."donationmodifieddate" AS DD_MODIFIED_DATE,
    A."donationentereddate" AS DD_ENTERED_DATE,
    A."donationdepositdate" AS DD_DEPOSIT_DATE,
    A."donationpaymentdescription" AS DD_PAYMENT_DESCRIPTION,
    A."donationgiftdescription" AS DD_GIFT_DESCRIPTION,
    A."donationreceiptdate" AS DD_RECEIPT_DATE,
    A."campid" AS DD_CAMPAIGN_ID,
    A."letter_recipient_name" AS DD_RECIPIENT_NAME,
    A."donorconstituentid" AS DD_CONSTITUENT_ID,
    A."donationamount" AS DD_DONATION_AMOUNT,
    A."appeal_code" AS DD_APPEAL_CODE,
    A."donationreceiptid" AS DD_RECEIPT_ID,
    A."donorfirstname" AS DD_FIRSTNAME,
    A."donorlastname" AS DD_LASTNAME,
    A."donationmessage" AS DD_DONATION_MESSAGE,
    B."Id" AS NPC_ACCOUNT_ID,
    B."DonorDrive_ID__c" AS NPC_DD_ID,
    B."FirstName" AS NPC_FIRSTNAME,
    B."LastName" AS NPC_LASTNAME,
    C."Id" AS NPC_GIFTTRANSACTION_ID,
    C."Name" AS NPC_GT_NAME,
    C."CreatedDate" AS NPC_GT_CREATED_DATE,
    C."Donor_Drive_Revenue_Type__c" AS NPC_DD_REVENUE_TYPE,
    C."Donor_Drive_Payment_Description__c" AS NPC_DD_PAYMENT_DESCRIPTION,
    C."DD_Recurring_Profile_ID__c" AS NPC_DD_RECURRING_PROFILE,
    C."OwnerId" AS NPC_OWNER_ID,
    C."CampaignId" AS NPC_CAMPAIGN_ID,
    C."OriginalAmount" AS NPC_TRANS_AMOUNT,
    C."TransactionDate" AS NPC_TRANSACTION_DATE
FROM DONORDRIVE A
    LEFT JOIN PRODUCTION.REPL_NPC.ACCOUNT B 
        ON A."donorconstituentid" = B."DonorDrive_ID__c"
    LEFT JOIN PRODUCTION.REPL_NPC.GIFTTRANSACTION C
        ON B."Id" = C."DonorId"  --also join on amount
        AND A."donationamount" = C."OriginalAmount"
WHERE CAST(LEFT(A."donationentereddate", 10) AS DATE) = CAST(LEFT(C."CreatedDate", 10) AS DATE) --what on transaction is day to match to dd? on day that it happened. did it make it into npc?
;

--B."Id" = '001Vq00000UACnHIAX'



SELECT 
    A.date
FROM PRODUCTION.REPL_DONORDRIVE.DD_DONATIONS_NEW A;













