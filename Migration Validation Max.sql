--Personal Email
SELECT
    CASE
        WHEN PROD.AACR_ID IS NULL AND TEST."AACR_Id__c" IS NULL THEN 'PASS NULL'
        WHEN PROD.AACR_ID = TEST."AACR_Id__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD.AACR_ID, 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."AACR_Id__c", 'NULL')
    END AS "AACR_ID_Validation"
    ,
    CASE
        WHEN PROD."OrderApi__Personal_Email__c" IS NULL AND (TEST."Personal_Email__c" IS NULL OR TEST."Personal_Email__c" = '') THEN 'PASS NULL'
        WHEN PROD."OrderApi__Personal_Email__c" = TEST."Personal_Email__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."OrderApi__Personal_Email__c", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."Personal_Email__c", 'NULL')
    END AS "OrderApi__Personal_Email__c_Validation"
    ,
    CASE
        WHEN PROD."Title" IS NULL AND (TEST."PersonTitle" IS NULL OR TEST."PersonTitle" = '') THEN 'PASS NULL'
        WHEN PROD."Title" = TEST."PersonTitle" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."Title", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."PersonTitle", 'NULL')
    END AS "Title_Validation"
    ,
    CASE
        WHEN PROD."OrderApi__Work_Phone__c" IS NULL AND (TEST."Work_Phone__c" IS NULL OR TEST."Work_Phone__c" = '') THEN 'PASS NULL'
        WHEN PROD."OrderApi__Work_Phone__c" = TEST."Work_Phone__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."OrderApi__Work_Phone__c", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."Work_Phone__c", 'NULL')
    END AS "OrderApi__Work_Phone__c_Validation"
    ,
    CASE
        WHEN PROD."Work_Setting__c" IS NULL AND (TEST."Work_Setting__c" IS NULL OR TEST."Work_Setting__c" = '') THEN 'PASS NULL'
        WHEN PROD."Work_Setting__c" = TEST."Work_Setting__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."Work_Setting__c", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."Work_Setting__c", 'NULL')
    END AS "Work_Setting__c_Validation"
    ,
    CASE
        WHEN PROD."AccountId" IS NULL AND (TEST."AACRAccountID__c" IS NULL OR TEST."AACRAccountID__c" = '') THEN 'PASS NULL'
        WHEN PROD."AccountId" = TEST."AACRAccountID__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."AccountId", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."AACRAccountID__c", 'NULL')
    END AS "AccountId_Validation"
FROM
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT_PT_Copy AS PROD
JOIN 
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS TEST ON PROD."Id" = TEST."Legacy_ID__c"
;








with AccAACRID AS (
    SELECT
        CASE
            WHEN PROD."AccountId" IS NULL AND (TEST."AACRAccountID__c" IS NULL OR TEST."AACRAccountID__c" = '') THEN 'PASS NULL'
            WHEN PROD."AccountId" = TEST."AACRAccountID__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."AccountId", 'NULL') || 
                    ' AND TEST: ' || COALESCE(TEST."AACRAccountID__c", 'NULL')
        END AS "AccountId_Validation"
    FROM
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT_PT_COPY AS PROD
    JOIN 
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS TEST ON PROD."Id" = TEST."Legacy_ID__c"
)
SELECT * FROM AccAACRID WHERE "AccountId_Validation" NOT IN ('PASS', 'PASS NULL');


with AccountsFixed AS (
    SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT FA WHERE FA."Name" NOT LIKE '%Individual%'
),
NPCAccs AS (
    SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT WHERE "Name" IS NOT NULL
),
AccAACRID AS (
    SELECT
        PROD."Id",
        TEST."Fonteva_Account_ID__c",
        CASE
            WHEN PROD."iMIS_ID__c" IS NULL AND (TEST."AACR_Id__c" IS NULL OR TEST."AACR_Id__c" = '') THEN 'PASS NULL'
            WHEN PROD."iMIS_ID__c" = TEST."AACR_Id__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."iMIS_ID__c", 'NULL') || 
                    ' AND TEST: ' || COALESCE(TEST."AACR_Id__c", 'NULL')
        END AS "AccountId_Validation"
    FROM
        AccountsFixed AS PROD
    JOIN 
        NPCAccs AS TEST ON PROD."Id" = TEST."Fonteva_Account_ID__c"
)
SELECT * FROM AccAACRID WHERE "AccountId_Validation" NOT IN ('PASS', 'PASS NULL');



SELECT 
    *
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS A
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT AS B
WHERE "Fonteva_Account_ID__c" <> "Legacy_ID__c" AND "Name" IS NOT NULL;

SELECT B."iMIS_ID__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT AS B WHERE "iMIS_ID__c" IS NOT NULL;



SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK WHERE ACCOUNT_ID_FONTEVA IS NOT NULL AND ACCOUNT_IND_ID_FULL IS NOT NULL;







SELECT A."Company__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT A WHERE A."Company__c" <> '';

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_AACRIDS WHERE "ID" LIKE '001%';

SELECT ACC.* FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT ACC WHERE ACC."iMIS_ID__c" IS NOT NULL;









SELECT "OrderApi__Work_Phone__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '0031I00000fvrhQQAQ';

SELECT  
    CASE
        WHEN PROD."AccountId" IS NULL AND (TEST."AACRAccountID__c" IS NULL OR TEST."AACRAccountID__c" = '') THEN 'PASS NULL'
        WHEN PROD."AccountId" = TEST."AACRAccountID__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."AccountId", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."AACRAccountID__c", 'NULL')
    END AS "AccountId_Validation"
FROM
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT_PT_Copy AS PROD
JOIN 
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS TEST ON PROD."Id" = TEST."Legacy_ID__c"
;



/*
CASE
            WHEN PROD."AccountId" IS NULL AND TEST."Fonteva_Account_ID__c" IS NULL THEN 'PASS NULL'
            WHEN PROD."AccountId" = TEST."Fonteva_Account_ID__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."AccountId", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."Fonteva_Account_ID__c", 'NULL')
END AS "AccountId_Validation"

CASE
            WHEN PROD."Id" IS NULL AND TEST."Fonteva_Account_ID__c" IS NULL THEN 'PASS NULL'
            WHEN PROD."Id" = TEST."Fonteva_Account_ID__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."Id", 'NULL') || 
                 ' AND TEST: ' || COALESCE(TEST."Fonteva_Account_ID__c", 'NULL')
        END AS "Account_ID_Validation"

    CASE
        WHEN PROD."iMIS_ID__c" IS NULL AND TEST."AACRAccountID__c" IS NULL THEN 'PASS NULL'
        WHEN PROD."iMIS_ID__c" = TEST."AACRAccountID__c" THEN 'PASS'
        ELSE 'FAIL: PROD: ' || COALESCE(PROD."iMIS_ID__c", 'NULL') || 
            ' AND TEST: ' || COALESCE(TEST."AACRAccountID__c", 'NULL')
    END AS "AACR_Account_ID_Validation"

FROM
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT_PT_Copy AS PROD
JOIN 
    ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS TEST ON PROD."Id" = TEST."Legacy_ID__c"

 */






/*
AACR ID
FAIL: PROD: 1433590 AND TEST: NULL
FAIL: PROD: 1185475 AND TEST: NULL
FAIL: PROD: 1185478 AND TEST: NULL
FAIL: PROD: 1202621 AND TEST: NULL
FAIL: PROD: 1073530 AND TEST: NULL

1433590
1185475
1185478
1202621
1073530


 */


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST;



/****************************

New round 5/28/25

****************************/
SET prod_field = 'Membership_Information_c';
SET test_field = 'Membership_Information_c';
SET prod_table = 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT';
SET test_table = 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST';

with ValidationResults AS (
    SELECT
        PROD."Id",
        TEST."Legacy_Id__c",
        CASE
            WHEN PROD."Minorities_in_Cancer_Research__c" IS NULL AND (TEST."Minorities_in_Cancer_Research__c" IS NULL OR TEST."Minorities_in_Cancer_Research__c" = '') THEN 'PASS NULL'
            WHEN PROD."Minorities_in_Cancer_Research__c" = TEST."Minorities_in_Cancer_Research__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."Minorities_in_Cancer_Research__c", 'NULL') || 
                    ' AND TEST: ' || COALESCE(TEST."Minorities_in_Cancer_Research__c", 'NULL')
        END AS "Membership_Information_Validation"
    FROM
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS PROD
    JOIN 
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST AS TEST ON PROD."Id" = TEST."Legacy_Id__c"
)
SELECT COUNT(*), "Membership_Information_Validation" FROM ValidationResults GROUP BY 2;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

--join on contact id and another field, year??

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C;

with Dedupe AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY "Education_Related_to_Contact__c" ORDER BY "Degree_Completion_Year__c" DESC) AS RN,
        "Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_FONTEVA
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C
),
Crosswalk AS (
    SELECT
        A.*,
        A."Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_NPC,
        B.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION AS A
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
            ON A."AccountIdMatch" = B.CONTACT_ID_FONTEVA
)
SELECT 
    A."Current_Education_Status__c",
    B."Academic_Status__c",
    A."Degree__c",
    A."Education_Related_to_Contact__c" AS FONTEVA_CONTACT,
    B.CONTACT_ID AS NPC_CONTACT,
    A.FIXED_GRAD_DATE_FONTEVA,
    B.FIXED_GRAD_DATE_NPC,
    *
FROM Dedupe AS A
    LEFT JOIN Crosswalk AS B
        ON A."Education_Related_to_Contact__c" = B.CONTACT_ID
        AND A.FIXED_GRAD_DATE_FONTEVA = B.FIXED_GRAD_DATE_NPC
WHERE RN = 1
AND A."Current_Education_Status__c" <> B."Academic_Status__c"
;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C AS A WHERE A."Education_Related_to_Contact__c" = '0031I000012D3hcQAC';


SELECT *
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION
WHERE "DonorIdMatch" = '0031I00000WsqAxQAJ';

/*

DD."donationmodifieddate"           AS DD_MODIFIED_DATE,
DD."donationentereddate"            AS DD_ENTERED_DATE,
DD."donationdepositdate"            AS DD_DEPOSIT_DATE,
DD."donationreceiptdate"            AS DD_RECEIPT_DATE,
DD."donationpaymentdescription"     AS DD_PAYMENT_DESCRIPTION,
DD."donationgiftdescription"        AS DD_GIFT_DESCRIPTION,
DD."campid"                         AS DD_CAMPAIGN_ID,
DD."letter_recipient_name"          AS DD_RECIPIENT_NAME,
DD."donorconstituentid"             AS DD_CONSTITUENT_ID,
DD."donationamount"                 AS DD_DONATION_AMOUNT,
DD."appeal_code"                    AS DD_APPEAL_CODE,
DD."donationreceiptid"              AS DD_RECEIPT_ID,
DD."donorfirstname"                 AS DD_FIRSTNAME,
DD."donorlastname"                  AS DD_LASTNAME,
DD."donationmessage"                AS DD_DONATION_MESSAGE,

A."Donor_Drive_Revenue_Type__c"    AS NPC_DD_REVENUE_TYPE,
A."Donor_Drive_Payment_Description__c" AS NPC_DD_PAYMENT_DESCRIPTION,
A."DD_Recurring_Profile_ID__c"     AS NPC_DD_RECURRING_PROFILE,
A."OwnerId"                        AS NPC_OWNER_ID,
A."CampaignId"                     AS NPC_CAMPAIGN_ID,
A."OriginalAmount"                 AS NPC_TRANS_AMOUNT,
A."TransactionDate"                AS NPC_TRANSACTION_DATE,




    CAST(A.GOLDEN_RECORD_ID AS VARCHAR) as "Primary_Key",
    CAST(A.GOLDEN_RECORD_ID AS VARCHAR) AS "GOLDEN_RECORD_ID",
    A.AACR_ID,
    A.FIRST_NAME,
    A.LAST_NAME,
    A.GENDER,
    A.RACE,
    A.EMAIL,
    A.COMPANY_NAME,
    A.STREET,
    A.STREET_2,
    A.CITY,
    A.STATE_PROVINCE,
    A.COUNTRY,
    A.ZIP,
    A.MOBILE_NUMBER,
    A.CONSENT_DECLARATION,
    A.SOURCE_NAME,
    A.DATE_COLLECTED,
    A.LEAD_USE,
    A.LEAD_DESCRIPTION,
    A.TICKET_NUMBER,
    A.SUBMITTER_NAME,
    A.DEPARTMENT,
    A.DOUBLE_OPT_IN


    A.AACR_ID,
    A.FIRST_NAME,
    A.LAST_NAME,
    A.GENDER,
    A.RACE,
    A.EMAIL,
    A.COMPANY_NAME,
    A.STREET,
    A.STREET_2,
    A.CITY,
    A.STATE_PROVINCE,
    A.COUNTRY,
    A.ZIP,
    A.MOBILE_NUMBER,
    A.CONSENT_DECLARATION,
    A.SOURCE_NAME,
    A.DATE_COLLECTED,
    A.LEAD_USE,
    A.LEAD_DESCRIPTION,
    A.TICKET_NUMBER,
    A.SUBMITTER_NAME,
    A.DEPARTMENT,
    A.DOUBLE_OPT_IN



SELECT 
    CONCAT(A.GOLDEN_RECORD_ID, A.TICKET_NUMBER) AS "Primary_Key",
    CAST(A.GOLDEN_RECORD_ID AS VARCHAR) AS "GOLDEN_RECORD_ID",
    A.AACR_ID,
    A.FIRST_NAME,
    A.LAST_NAME,
    A.GENDER,
    A.RACE,
    A.EMAIL,
    A.COMPANY_NAME,
    A.STREET,
    A.STREET_2,
    A.CITY,
    A.STATE_PROVINCE,
    A.COUNTRY,
    A.ZIP,
    A.MOBILE_NUMBER,
    A.CONSENT_DECLARATION,
    A.SOURCE_NAME,
    A.DATE_COLLECTED,
    A.LEAD_USE,
    A.LEAD_DESCRIPTION,
    A.TICKET_NUMBER,
    A.SUBMITTER_NAME,
    A.DEPARTMENT,
    A.DOUBLE_OPT_IN
FROM LEAD_LISTS_DB.RESULTS_TABLE.RESULTS AS A
WHERE A.EMAIL NOT LIKE '%@aacrtest.org'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(A.GOLDEN_RECORD_ID, A.TICKET_NUMBER) ORDER BY A.GOLDEN_RECORD_ID) = 1;


















 */


SELECT A.Country FROM PRODUCTION.REPL_NPC.ACCOUNT A ;

SELECT A."Name", A."Id", B."Id", B."Name" FROM PRODUCTION.REPL_NPC.ACCOUNT A JOIN PRODUCTION.REPL_NPC.RECORDTYPE B ON A."RecordTypeId" = B."Id" ORDER BY RANDOM() LIMIT 100;


--AACRGROUP01\jeffrey.cheung


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK;


SELECT DISTINCT A.donation_payment_description FROM PRODUCTION.REPL_DONORDRIVE.DD_DONATIONS A;

SELECT B."CampaignId" FROM  PRODUCTION.REPL_NPC.GIFTTRANSACTION B WHERE B."CampaignId" IS NOT NULL;


SELECT
        A.*,
        A."Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_NPC,
        B.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION AS A
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
            ON A."AccountIdMatch" = B.CONTACT_ID_FONTEVA;





SELECT DISTINCT CON. FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS CON;

/*
Done. All records PASS or PASS NULL
*/

/*Contact Interest*/
BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'AccountId';
    LET test_field  STRING := 'Account__c';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Id",
                    TEST."Legacy_Id__c",
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :test_table || '  TEST
            ON PROD."Id" = TEST."Legacy_Id__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'') 
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


/*
Done. All records PASS or PASS NULL

*/

/*Contact to PersonAccount*/
BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'AccountId';
    LET test_field  STRING := 'Company__c';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Id",
                    TEST."Legacy_ID__c",
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :test_table || '  TEST
            ON PROD."Id" = TEST."Legacy_ID__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'')
        AND $prod_field NOT IN (SELECT "Id" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT WHERE "Name" LIKE ''%Individual%'')
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


/*
Done. All records PASS or PASS NULL

*/

/*Account to PersonAccount*/
BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'BillingCountry';
    LET test_field  STRING := 'BillingCountry';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Id",
                    TEST."Legacy_ID__c",
                    PROD."Name" AS ACCOUNT_NAME,
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :test_table || '  TEST
            ON PROD."Id" = TEST."Legacy_ID__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'') AND ACCOUNT_NAME NOT LIKE ''%Individual%''
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA
;
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C;



SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;
SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP;

SELECT * FROM PRODUCTION.REPL_CSI.CSI_REGISTRATIONS;

SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE EVENT_ID = 'a4jRm000000rDS9IAM';

SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS a WHERE a."SALESFORCE ID" NOT IN (SELECT "Primary_Contact__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP)
;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP WHERE "Primary_Contact__c" NOT IN (SELECT "SALESFORCE ID" FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE)
;

SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE WHERE SUBSCRIPTION_STATUS = 'Active';

SELECT COUNT(*) FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP;

SELECT COUNT(*) FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;


SELECT a.* FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS a;


SELECT ACC."Name" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS ACC WHERE ACC."Id" = '001Va00000VjreeIAB';



SELECT ACC.* FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CON."AccountId" = ACC."Id"
WHERE CON."Id" = '003Va00000Lc2sDIAR';

SELECT a.* FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS a;

Legacy_Id__c;

with OldMEms AS (
    SELECT
        A.*,
        CONCAT(A."TERM ID",A."TERM YEAR") AS TERM_UNIQUE_ID,
        CONCAT(A.)
    FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS A
),
NewMems AS (
    SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP
),

SELECT * FROM OldMems WHERE OldMems.TERM_UNIQUE_ID NOT IN (SELECT "Legacy_Id__c" FROM NewMems);


SELECT PA.* FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT PA WHERE PA."Name" IS NOT NULL;


SELECT MP.a FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE MP;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP WHERE "Primary_Contact__c" = '003Va00000Lc2sDIAR';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB;

SELECT 
    A."Fonteva_Contact_ID__c", 
    A."Legacy_ID__c",
    A."Fonteva_Account_ID__c" 
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS A WHERE A."Legacy_ID__c" LIKE '003%';



/********************************************/
--Find Salesforce RecordTypes on any object.
SELECT Id, Name, DeveloperName, SObjectType 
FROM RecordType 
WHERE SObjectType = 'Account';
/********************************************/

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.;

SELECT DISTINCT "RecordTypeId" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT;


 


/*
ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT."Fonteva_Account_ID__c" = ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP."Account__c"

PersonAccount."Legacy_ID__c" = Membership."Primary_Contact_ID_Match1"


 */


--Try Legacy_ID__c
--


SELECT * FROM ;

with MemMod AS (
    SELECT 
        *,
         CONCAT(A."TERM ID",A."TERM YEAR") AS TERM_UNIQUE_ID,
        FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS A
),
ValidationResults AS(
    SELECT
        PROD.TERM_UNIQUE_ID,
        TEST."Legacy_Id__c",
        CASE
            WHEN PROD."ITEM_ID" IS NULL AND (TEST."Membership_Product__c" IS NULL OR TEST."Membership_Product__c" = '') THEN 'PASS NULL'
            WHEN PROD."ITEM_ID" = TEST."Membership_Product__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."ITEM_ID", 'NULL') || 
                    ' AND TEST: ' || COALESCE(TEST."Membership_Product__c", 'NULL')
        END AS "Membership_Information_Validation"
    FROM
        MemMod AS PROD
    JOIN 
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS TEST ON PROD.TERM_UNIQUE_ID = TEST."Legacy_Id__c"
)
SELECT COUNT(*), "Membership_Information_Validation" FROM ValidationResults GROUP BY 2;




with ProdItems AS (
    SELECT 
        "ITEM_ID",
        "ITEM NAME",
        "SALESFORCE ID"
    FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE
    WHERE "ITEM NAME" IS NOT NULL
)
SELECT
    ProdItems.*,
    A."Legacy_Id__c",
    A."Membership_Product__c",
    B.ID,
    B.NAME
FROM PRODITEMS
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS A
        ON ProdItems."SALESFORCE ID" = A."Primary_Contact__c_Match1"
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP_PRODUCT AS B
        ON A."Membership_Product__c" = B.ID
LIMIT 500
;

SELECT
    DISTINCT LEFT("ITEM NAME", CHARINDEX(' ', "ITEM NAME"))
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE
;

with Stg AS (
SELECT
    *,
    RIGHT("NAME", LEN("NAME")-CHARINDEX(' - ', "NAME") - 2) AS CLEANED_NEW_MEM_NAME
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP_PRODUCT
WHERE "NAME" LIKE '%Membership%'
),
Font AS (
    SELECT 
        *,
        LEFT("ITEM NAME", CHARINDEX(' ', "ITEM NAME")-1) AS CLEANED_OLD_MEM_NAME
    FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE
)
SELECT
    A."SALESFORCE ID",
    C."Primary_Contact__c_Match1",
    A.CLEANED_OLD_MEM_NAME,
    B.CLEANED_NEW_MEM_NAME
FROM Font AS A
        JOIN Stg AS B
            ON A."ITEM_ID" = B.LEGACY_ID__C
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS C
            ON C."Membership_Product__c" = B.ID
            AND C."Primary_Contact__c_Match1" = A."SALESFORCE ID"
WHERE A.CLEANED_OLD_MEM_NAME <> B.CLEANED_NEW_MEM_NAME;




FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE AS A
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP_PRODUCT AS B
            ON A."ITEM_ID" = B.LEGACY_ID__C
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS C
            ON C."Membership_Product__c" = B.ID
            ;


FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS A
    LEFT JOIN MemProductsCW AS B
        ON A."Membership_Product__c" = B.MEM_PRODUCT_ID;













with ProdItems AS (
    SELECT 
        "ITEM_ID",
        "ITEM NAME"
    FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE
    WHERE "ITEM NAME" IS NOT NULL
    GROUP BY 1, 2
),
    MemProductsCW AS (
    SELECT 
        ProdItems.*,
        A.LEGACY_ID__C,
        A."NAME",
        A.ID AS MEM_PRODUCT_ID
    FROM PRODITEMS
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP_PRODUCT AS A
            ON PRODITEMS."ITEM_ID" = A.LEGACY_ID__C
)
SELECT 
    A."Legacy_Id__c",
    A."Membership_Product__c",
    B.MEM_PRODUCT_ID,
    B."NAME",
    B."ITEM NAME"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS A
    LEFT JOIN MemProductsCW AS B
        ON A."Membership_Product__c" = B.MEM_PRODUCT_ID;






SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP_PRODUCT;


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP;












SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT WHERE "Name" IS NOT NULL AND "Fonteva_Contact_ID__c" IN (SELECT DISTINCT M."Primary_Contact__c_Match1" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS M);




SELECT "Name" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP WHERE "Primary_Contact__c_Match1" = '0031I000012DS9eQAG';
--Value 1: Siyun Wang Student Member Membership
--Value 2: Siyun Wang Associate Member Membership
SELECT "Name" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT WHERE "Fonteva_Contact_ID__c" = '0031I000012DS9eQAG';
--Value: null


/*OrderApi__Contact__c */
SELECT
    MPM."SALESFORCE ID",
    CW.CONTACT_ID_FONTEVA,
    CW.ACCOUNT_IND_ID_FULL,
    MEM."Account__c"
FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS MPM
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS CW
        ON MPM."SALESFORCE ID" = CW.CONTACT_ID_FONTEVA
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS MEM
        ON CW.ACCOUNT_IND_ID_FULL = MEM."Account__c"
WHERE MPM."SALESFORCE ID" <> CW.CONTACT_ID_FONTEVA OR CW.ACCOUNT_IND_ID_FULL <> MEM."Account__c"
;

SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK;


with MemMod AS (
    SELECT 
        *,
        A.
         CONCAT(A."TERM ID",A."TERM YEAR") AS TERM_UNIQUE_ID,
        FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS A
)
SELECT
    *
FROM
        MemMod AS PROD
    JOIN 
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS TEST ON PROD.TERM_UNIQUE_ID = TEST."Legacy_Id__c"
WHERE PROD."SALESFORCE ID" <> TEST."Primary_Contact__c_Match1";




SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;



SELECT 
    a."SALESFORCE ID",
    b."Primary_Contact__c"
FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE AS a
    FULL OUTER JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP AS b
        ON a."SALESFORCE ID" = b."Primary_Contact__c"
WHERE a."SALESFORCE ID" IS NULL OR b."Primary_Contact__c" IS NULL;


SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE WHERE "SALESFORCE ID" NOT IN (SELECT "Primary_Contact__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP);


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP WHERE "Primary_Contact__c" NOT IN (SELECT "SALESFORCE ID" FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE);

/*Subscriptions/Membership to Membership*/
BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'BillingCountry';
    LET test_field  STRING := 'BillingCountry';
    LET prod_table  STRING := 'TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_MEMBERSHIP';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Id",
                    TEST."Legacy_ID__c",
                    PROD."Name" AS ACCOUNT_NAME,
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :test_table || '  TEST
            ON PROD."Id" = TEST."Legacy_ID__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'') AND ACCOUNT_NAME NOT LIKE ''%Individual%''
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


/*Account ID Joins*/
BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'Current_Education_Status__c';
    LET test_field  STRING := 'Academic_Status__c';
    LET prod_table  STRING := 'PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C';
    LET cross_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Education_Related_to_Contact__c" AS SALESFORCE_ID,
                    CRS."CONTACT_ID_FONTEVA" AS CROSSWALK_CONTACT_ID,
                    CRS."ACCOUNT_IND_ID_FULL" AS CROSSWALK_ACCOUNT_ID,
                    TEST."Account__c" AS STAGING_ACCOUNT_ID,
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :cross_table || '  CRS
            ON PROD."Education_Related_to_Contact__c" = CRS."CONTACT_ID_FONTEVA"
            JOIN '  || :test_table || '  TEST
            ON CRS."ACCOUNT_IND_ID_FULL" = TEST."Account__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'')
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK;

SELECT A. FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS A;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C;

















SELECT 
    CON."Id"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC AS CON WHERE CON."Id" NOT IN (SELECT PA."Fonteva_Contact_ID__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS PA);

SELECT PA."Fonteva_Contact_ID__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS PA
WHERE PA."Fonteva_Contact_ID__c" NOT IN (SELECT 
    CON."Id"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS CON);



















BEGIN
    /*Parameters you’ll tweak each time*/
    LET prod_field  STRING := 'AccountId';
    LET test_field  STRING := 'Company__c';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT';

    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with ValidationResults AS (
            SELECT  PROD."Id",
                    TEST."Legacy_ID__c",
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''

                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''

                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM '  || :prod_table || '  PROD
            JOIN '  || :test_table || '  TEST
            ON PROD."Id" = TEST."Legacy_ID__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'')
        AND $prod_field NOT IN (SELECT "Id" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT WHERE "Name" LIKE ''%Individual%'')
    ';

    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);

    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


with ValidationR AS (
    SELECT
        PROD."Id",
        TEST."Legacy_ID__c",
        PROD."AccountId",
        CASE
            WHEN PROD."AccountId" IS NULL AND (TEST."Company__c" IS NULL OR TEST."Company__c" = '') THEN 'PASS NULL'
            WHEN PROD."AccountId" = TEST."Company__c" THEN 'PASS'
            ELSE 'FAIL: PROD: ' || COALESCE(PROD."AccountId", 'NULL') || 
                    ' AND TEST: ' || COALESCE(TEST."Company__c", 'NULL')
        END AS "Validation"
    FROM
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS PROD
    JOIN 
        ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS TEST ON PROD."Id" = TEST."Legacy_ID__c"
)
SELECT * FROM ValidationR WHERE "Validation" NOT IN ('PASS', 'PASS NULL')
        AND "AccountId" NOT IN (SELECT "Id" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT WHERE "Name" LIKE '%Individual%')
;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT WHERE "Id" = '0018W00002DTY6qQAH';









BEGIN
    /*Parameters you'll tweak each time*/
    LET prod_field  STRING := 'Date_of_Expected_Graduation__c';
    LET test_field  STRING := 'Date_of_Expected_Graduation__c';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_EDUCATION';
    LET cross_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION';
    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with 
        EdFixed AS (
            SELECT 
                "Id",
                "IsDeleted",
                "Name",
                "CreatedDate",
                "CreatedById",
                "LastModifiedDate",
                "LastModifiedById",
                "SystemModstamp",
                "Education_Related_to_Contact__c",
                TO_VARCHAR("Date_of_Expected_Graduation__c"::Date) AS "Date_of_Expected_Graduation__c",
                "Degree_Completion_Year__c",
                "Degree__c",
                "Current_Education_Status__c",
                "Degree_Level__c"
            FROM '  || :prod_table || ' 
        ),
        ValidationResults AS (
            SELECT  PROD."Education_Related_to_Contact__c" AS SALESFORCE_ID,
                    CRS."CONTACT_ID_FONTEVA" AS CROSSWALK_CONTACT_ID,
                    CRS."ACCOUNT_IND_ID_FULL" AS CROSSWALK_ACCOUNT_ID,
                    TEST."Account__c" AS STAGING_ACCOUNT_ID,
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''
                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''
                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM EdFixed AS PROD
            JOIN '  || :cross_table || '  CRS
            ON PROD."Education_Related_to_Contact__c" = CRS."CONTACT_ID_FONTEVA"
            JOIN '  || :test_table || '  TEST
            ON CRS."ACCOUNT_IND_ID_FULL" = TEST."Account__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'')
    ';
    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);
    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_EDUCATION;

with EdFixed AS (
    SELECT 
        "Id",
        "IsDeleted",
        "Name",
        "CreatedDate",
        "CreatedById",
        "LastModifiedDate",
        "LastModifiedById",
        "SystemModstamp",
        "Education_Related_to_Contact__c",
        TO_VARCHAR("Date_of_Expected_Graduation__c"::Date),
        "Degree_Completion_Year__c",
        "Degree__c",
        "Current_Education_Status__c",
        "Degree_Level__c"
    FROM '  || :prod_table || ' 
)
SELECT * FROM EdFixed;



SELECT 
    A."Id",
    A."Name" AS CONTACT_NAME,
    A."AccountId",
    B."Fonteva_Contact_ID__c",
    B."Fonteva_Account_ID__c",
    B."Company__c"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS A
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS B
        ON A."Id" = B."Fonteva_Contact_ID__c"
WHERE A."Id" = '003Rm00000J3guEIAR';

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT WHERE "Name" IS NOT NULL;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT A WHERE A."Fonteva_Contact_ID__c" = '003Rm00000J3guEIAR';
--company is 001Va00000VhixMIAR
SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT A WHERE A."Id" = '001Va00000VhixMIAR';

--https://www.linkedin.com/in/maxwell-bicking
--https://github.com/maxbicking/


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK CW WHERE CW.ACCOUNT_ORG_ID_FULL = '001Va00000VhixMIAR';

SELECT
    A."Id" AS FONTEVA_ACCOUNT_ID,
    B.ACCOUNT_ID_FONTEVA AS FONTEVA_ACCOUNT_ID_CW,
    B.ACCOUNT_ORG_ID_FULL AS CW_ACCOUNT_ID,
    C."Company__c"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT AS A
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
        ON A."Id" = B.ACCOUNT_ID_FONTEVA
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS C
        ON B.ACCOUNT_ORG_ID_FULL = C."Company__c"
WHERE A."Name" NOT LIKE '%Individual%'
    AND (A."Id" <> B.ACCOUNT_ID_FONTEVA OR B.ACCOUNT_ORG_ID_FULL <> C."Company__c");



BEGIN
    /*Parameters you'll tweak each time*/
    LET prod_field  STRING := 'Date_of_Expected_Graduation__c';
    LET test_field  STRING := 'Date_of_Expected_Graduation__c';
    LET prod_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_EDUCATION';
    LET cross_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK';
    LET test_table  STRING := 'ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION';
    /*Assemble the dynamic SQL text*/
    LET sql_txt STRING := '
        with
        EdFixed AS (
            SELECT
                "Id",
                "IsDeleted",
                "Name",
                "CreatedDate",
                "CreatedById",
                "LastModifiedDate",
                "LastModifiedById",
                "SystemModstamp",
                "Education_Related_to_Contact__c",
                TO_VARCHAR("Date_of_Expected_Graduation__c"::Date) AS "Date_of_Expected_Graduation__c",
                "Degree_Completion_Year__c",
                "Degree__c",
                "Current_Education_Status__c",
                "Degree_Level__c"
            FROM '  || :prod_table || '
        ),
        ValidationResults AS (
            SELECT  PROD."Education_Related_to_Contact__c" AS SALESFORCE_ID,
                    CRS."CONTACT_ID_FONTEVA" AS CROSSWALK_CONTACT_ID,
                    CRS."ACCOUNT_IND_ID_FULL" AS CROSSWALK_ACCOUNT_ID,
                    TEST."Account__c" AS STAGING_ACCOUNT_ID,
                    CASE
                        WHEN PROD."' || :prod_field || '" IS NULL
                            AND (TEST."' || :test_field || '" IS NULL
                                OR TEST."' || :test_field || '" = '''')
                            THEN ''PASS NULL''
                        WHEN PROD."' || :prod_field || '" = TEST."' || :test_field || '"
                            THEN ''PASS''
                        ELSE ''FAIL: PROD: '' ||
                            COALESCE(TO_VARCHAR(PROD."' || :prod_field || '"), ''NULL'') ||
                            '' AND TEST: '' ||
                            COALESCE(TO_VARCHAR(TEST."' || :test_field || '"), ''NULL'')
                    END  AS VALIDATION
            FROM EdFixed AS PROD
            JOIN '  || :cross_table || '  CRS
            ON PROD."Education_Related_to_Contact__c" = CRS."CONTACT_ID_FONTEVA"
            JOIN '  || :test_table || '  TEST
            ON CRS."ACCOUNT_IND_ID_FULL" = TEST."Account__c"
        )
        SELECT * FROM ValidationResults WHERE VALIDATION NOT IN (''PASS'', ''PASS NULL'')
    ';
    /*Run it and capture the rows*/
    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_txt);
    /*Stream the rows back to Snowsight*/
    RETURN TABLE(rs);
END;


with Dedupe AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY "Education_Related_to_Contact__c" ORDER BY "Date_of_Expected_Graduation__c"::DATE DESC) AS RN,
        "Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_FONTEVA
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C
),
Crosswalk AS (
    SELECT
        A.*,
        A."Account__c" AS NPC_ACCOUNT_ID,
        A."Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_NPC,
        B.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION AS A
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
            ON A."AccountIdMatch" = B.CONTACT_ID_FONTEVA
)
SELECT 
    A.FIXED_GRAD_DATE_FONTEVA,
    B.FIXED_GRAD_DATE_NPC,
    A."Current_Education_Status__c",
    B."Academic_Status__c",
    A."Education_Related_to_Contact__c" AS FONTEVA_CONTACT_ID,
    B.NPC_ACCOUNT_ID
FROM Dedupe AS A
    LEFT JOIN Crosswalk AS B
        ON A."Education_Related_to_Contact__c" = B.CONTACT_ID
WHERE RN = 1
AND A.FIXED_GRAD_DATE_FONTEVA <> B.FIXED_GRAD_DATE_NPC

;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C AS A WHERE A."Education_Related_to_Contact__c" = '0031I00000WsZbCQAV';

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION;


with Dedupe AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY "Education_Related_to_Contact__c" ORDER BY "Date_of_Expected_Graduation__c"::DATE DESC) AS RN,
        "Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_FONTEVA
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C
),
Crosswalk AS (
    SELECT
        A.*,
        A."Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_NPC,
        B.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION AS A
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
            ON A."AccountIdMatch" = B.CONTACT_ID_FONTEVA
)
SELECT 
    A."Current_Education_Status__c",
    B."Academic_Status__c",
    *
FROM Dedupe AS A
    LEFT JOIN Crosswalk AS B
        ON A."Education_Related_to_Contact__c" = B.CONTACT_ID
        AND A.FIXED_GRAD_DATE_FONTEVA = B.FIXED_GRAD_DATE_NPC
WHERE RN = 1
AND A."Current_Education_Status__c" <> B."Academic_Status__c"
;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C AS A WHERE A."Education_Related_to_Contact__c" = '0031I000012D3hcQAC';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C AS A WHERE A."Education_Related_to_Contact__c" = '0031I00000WsZbCQAV';

--	0031I00000WsZbCQAV

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT WHERE "Id" = '001Va00000VhixMIAR';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '003Rm00000J3guEIAR';
--accountid is 001Rm00000GPUBPIA5 here

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS ;


with Dedupe AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY "Education_Related_to_Contact__c" ORDER BY "Date_of_Expected_Graduation__c"::DATE DESC) AS RN,
        "Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_FONTEVA
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C
),
Crosswalk AS (
    SELECT
        A.*,
        A."Date_of_Expected_Graduation__c"::DATE AS FIXED_GRAD_DATE_NPC,
        B.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_EDUCATION AS A
        JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
            ON A."AccountIdMatch" = B.CONTACT_ID_FONTEVA
),
Wonky AS (
    SELECT 
        A."Current_Education_Status__c",
        B."Academic_Status__c",
        B.CONTACT_ID
    FROM Dedupe AS A
        LEFT JOIN Crosswalk AS B
            ON A."Education_Related_to_Contact__c" = B.CONTACT_ID
            AND A.FIXED_GRAD_DATE_FONTEVA = B.FIXED_GRAD_DATE_NPC
    WHERE RN = 1
    AND A."Current_Education_Status__c" <> B."Academic_Status__c"
    ORDER BY 3
)
SELECT 
    E."Education_Related_to_Contact__c",
    E."Date_of_Expected_Graduation__c",
    E."Name",
    E."Current_Education_Status__c",
    E."CreatedDate",
    E."LastModifiedDate",
    E."IsDeleted"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.EDUCATION__C AS E 
WHERE E."Education_Related_to_Contact__c" IN (SELECT CONTACT_ID FROM Wonky) 
ORDER BY E."Education_Related_to_Contact__c", E."Date_of_Expected_Graduation__c" DESC;

SELECT COUNT(*) FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS;

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP;

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL;



SELECT COUNT("Transaction Line ID"), COUNT("Composite Key Demo") FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS;

SELECT DISTINCT "Item Name" FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP;

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP WHERE "Transaction Contact Id" = '0031I00000WsnpiQAB';

--WHERE "Transaction Contact Id" = '0031I00000WsnpiQAB'

CREATE SCHEMA TEST.REPL_RINGGOLD;



--0031I00000WrxdpQAB

SELECT * FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS 
WHERE "Transaction Contact Id" = '0031I00000WrxdpQAB';




/*New starting 6/23*/

SELECT 
    A."Composite Key Demo",
    B."Membership__cMatch",
    A.RECEIPT_PAYMENT_TYPE,
    B."PaymentMethod"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Composite Key Demo" = B."Membership__cMatch"
WHERE A.RECEIPT_PAYMENT_TYPE <> B."PaymentMethod";


SELECT 
    A."Transaction Id",
    B."Legacy_Transaction_ID__c",    
    A.RECEIPT_PAYMENT_TYPE,
    B."PaymentMethod"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Transaction Id" = B."Legacy_Transaction_ID__c"
WHERE A.RECEIPT_PAYMENT_TYPE <> B."PaymentMethod";

SELECT
    A."Composite Key Demo",
    B."Membership__cMatch",
    A.SALES_ORDER_POSTED,
    B."IsPaid"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Composite Key Demo" = B."Membership__cMatch"
WHERE A.SALES_ORDER_POSTED <> B."IsPaid";



SELECT
    A."Composite Key Demo",
    B."Membership__cMatch",
    A."GP_Batch_Number__c",
    B."GP_Batch_ID__c"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Composite Key Demo" = B."Membership__cMatch"
WHERE A."GP_Batch_Number__c" <> B."GP_Batch_ID__c";

SELECT
    A."Composite Key Demo",
    B."Membership__cMatch",
    A."GP_Batch_Number__c",
    B."GP_Batch_ID__c"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Transaction Id" = B."Legacy_Transaction_ID__c"
WHERE A."GP_Batch_Number__c" <> B."GP_Batch_ID__c";

SELECT DISTINCT "Revenue_Line__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION;

SELECT A."DonorId" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION A;

SELECT FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS MT;
--Transaction Contact Id

SELECT
    MPM."Transaction Contact Id",
    MEM."DonorId",

FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS MPM
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS CW
        ON MPM."Transaction Contact Id" = CW.CONTACT_ID_FONTEVA
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS MEM
        ON CW.ACCOUNT_IND_ID_FULL = MEM."DonorId";


with Crosswalk AS (
    SELECT 
        *,
        CW.CONTACT_ID_FONTEVA AS CONTACT_ID
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS CW
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS MEM
            ON CW.ACCOUNT_IND_ID_FULL = MEM."DonorId"
)
SELECT
    *
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS MPM
WHERE MPM."Transaction Contact Id" NOT IN (SELECT CONTACT_ID FROM Crosswalk);


SELECT DISTINCT A.SALES_ORDER_POSTED FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS A;

SELECT
    CONCAT(C."Status", ' - ', B."OrderApi__Is_Posted__c"),
    COUNT(*)
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS B
        ON A."SALES ORDER ID" = B."Id"
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS C
        ON A."Transaction Id" = C."Legacy_Transaction_ID__c"
GROUP BY 1;

SELECT 
    A."Transaction Id",
    B."Legacy_Transaction_ID__c",    
    A.ORIGINAL_AMOUNT,
    B."OriginalAmount"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Transaction Id" = B."Legacy_Transaction_ID__c"
WHERE A.ORIGINAL_AMOUNT <> B."OriginalAmount";

SELECT DISTINCT A."Transaction Id" FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A WHERE A."Transaction Id" NOT IN (SELECT B."Legacy_Transaction_ID__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B)
;

with Crosswalk AS (
    SELECT 
        *,
        CW.ACCOUNT_IND_ID_FULL AS NEW_ID
    FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS MT
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS CW
            ON MT."Transaction Contact Id" = CW.CONTACT_ID_FONTEVA
)
SELECT 
    A.NEW_ID,
    B."DonorId",
    A."Transaction Id",
    B."Legacy_Transaction_ID__c"
FROM Crosswalk AS A
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Transaction Id" = B."Legacy_Transaction_ID__c"
;


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B;


SELECT DISTINCT A."Transaction Id" FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A;



SELECT 
    SUM(O."Amount")
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS O
    LEFT JOIN PRODUCTION.MART_SF_BADGES.BADGES AS C
        ON O."npe01__Contact_Id_for_Role__c" = C."OrderApi__Contact__c"
WHERE C.BADGE_TYPE_NAME LIKE '%Fellow%';

SELECT * FROM PRODUCTION.MART_SF_BADGES.BADGES WHERE BADGE_TYPE_NAME LIKE '%Fellow%'
;



SELECT 
    A."Transaction Id",
    B."Legacy_Transaction_ID__c",    
    A."Transaction Date",
    B."TransactionDate"
FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B
        ON A."Transaction Id" = B."Legacy_Transaction_ID__c"
WHERE A."Transaction Date" <> B."TransactionDate";

SELECT B."DonorId", B."DonorIdMatch", B."Name" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION AS B WHERE B."Name" LIKE ' -%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '003Rm00000lGX1yIAG';




SELECT 
    A."Id",
    B.CONTACT_ID_FONTEVA,
    B.ACCOUNT_IND_ID_FULL,
    C."Account__c"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
        ON A."Id" = B.CONTACT_ID_FONTEVA
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST AS C
        ON B.ACCOUNT_IND_ID_FULL = C."Account__c"
WHERE A."Id" IS NULL
    OR B.CONTACT_ID_FONTEVA IS NULL
    OR B.ACCOUNT_IND_ID_FULL IS NULL
    OR C."Account__c" IS NULL;



SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK
WHERE CONTACT_ID_FONTEVA IN ('0031I000010a2VnQAI', '0031I00000WsBjGQAV', '0031I00000WsCkSQAV', '0031I00000Ws0N8QAJ');



SELECT "Account__c" FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_CONTACT_INTEREST;




SELECT 
    A."Id",
    C."Id",
    A."Date_of_Death__c",
    C."Deceased_Date__c"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS A
    LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK AS B
        ON A."Id" = B.CONTACT_ID_FONTEVA
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS C
        ON B.ACCOUNT_IND_ID_FULL = C."Id"
WHERE A."Date_of_Death__c" <> C."Deceased_Date__c";



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C
WHERE "Id" IN (SELECT B.TRANSACTIONID FROM TEST.TEST_AREA.MEM_TRANS_DISCREPANCIES_63024 AS B);


SELECT 
    * 
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS A
    JOIN TEST.TEST_AREA.MEM_TRANS_DISCREPANCIES_63024 AS B
        ON a."Id" = B.CASESAFEID;

SELECT 
    DISTINCT A."OrderApi__Type__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS A;



SELECT 
    IT."Name",
    COUNT(*) 
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
    JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
        ON TL."OrderApi__Item__c" = IT."Id"
WHERE TL."OrderApi__Transaction__c" IN (SELECT CASESAFEID FROM TEST.TEST_AREA.MEM_TRANS_DISCREPANCIES_63024)
    AND (TL."OrderApi__Credit__c" - TL."OrderApi__Debit__c" > 0)
GROUP BY 1
ORDER BY 2 DESC;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION;

SELECT A."ContactId" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS A;


SELECT * FROM PRODUCTION.MART_COMMITTEES.FINAL_SUBSCRIPTION_COMMITTEE_MEMBERS;



'Student Member',
'Honorary Member',
'Active Member',
'Affiliate Member',
'Associate Member'
;


create or replace view PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP(
	PARTICIPATION_ID,
	PARTICIPATION_NUMBER,
	USER_SEGMENT,
	ALLERGIES,
	EVENT_ID,
	CONTACT_ID,
	EXTERNAL_ATTENDEE_ID,
	SALES_ORDER_ID,
	FOOD_PREFERENCES,
	LEAD_ID,
	PARTICIPANT_STATUS,
	USER_ID,
	VEGETARIAN,
	BYPASS_CONFIRMATION,
	PRE_POST_DOC,
	REFUND_REQUESTED,
	REGISTRATION_DATE,
	GDPR_CERTIFICATION,
	SOURCE_CODE_ID,
	IS_BOOTH_STAFF,
	WORKSHOP_GROUP,
	ADA_ACCOMODATIONS,
	EMERGENCY_CONTACT_EMAIL,
	EMERGENCY_CONTACT_NAME,
	EMERGENCY_CONTACT_PHONE,
	EMERGENCY_CONTACT_RELATIONSHIP,
	JOINT_MEMBER_ID,
	HEALTH_SAFETY_ATTESTATION,
	VIA_MASS_REG,
	REGISTRATION_SOURCE,
	COMP_CODE,
	I_WILL_BE_ATTENDING,
	SALES_ORDER_NUMBER,
	ACCOUNT_ID,
	BILLING_CITY,
	BILLING_CONTACT,
	BILLING_COUNTRY,
	BILLING_POSTAL_CODE,
	BILLING_STATE,
	BILLING_STREET,
	BUSINESS_GROUP_ID,
	CALCULATE_BILLING_DETAILS,
	CALCULATE_SHIPPING_DETAILS,
	CAMPAIGN_PAGE,
	CANCELLED_DATE,
	CLOSED_DATE,
	CLOSED_STATUS,
	CREATED_BY_CUSTOMER,
	CUSTOMER_REFERENCE_NUMBER,
	ORDER_DATE,
	ORDER_DESCRIPTION,
	E_PAYMENT_ID,
	SCHEDULE_END_DATE,
	ENTITY,
	HAS_INSTALLMENTS,
	INVOICE_DATE,
	IS_ACTIVE,
	IS_CANCELLED,
	IS_CLOSED,
	IS_EVERGREEN,
	IS_INVOICED,
	IS_POSTED,
	IS_RECURRING,
	IS_SCHEDULED,
	IS_SUSPENDED,
	JSON_DATA,
	MEMO,
	NUMBER_OF_INSTALLMENTS,
	PAYMENT_GATEWAY_ID,
	PAYMENT_METHOD_ID,
	PAYMENT_TERMS_ID,
	POSTED_DATE,
	POSTING_ENTITY,
	POSTING_STATUS,
	REQUIRE_PAYMENT_METHOD,
	SCHEDULE_FREQUENCY,
	SCHEDULE_STATUS,
	SCHEDULE_TYPE,
	SHIPPING_CITY,
	SHIPPING_CONTACT,
	SHIPPING_COUNTRY,
	SHIPPING_POSTAL_CODE,
	SHIPPING_STATE,
	SHIPPING_STREET,
	SCHEDULED_START_DATE,
	STATUS,
	SUSPENDED_DATE,
	SUSPENDED_REASON,
	SYSTEM_REFRESH,
	VALID_PAYMENT_METHOD,
	ACKNOWLEDGED_DATE,
	ACKNOWLEDGEMENT_METHOD,
	DO_NOT_ACKNOWLEDGE,
	FORECAST_CATEGORY,
	FORECASTED_AMOUNT,
	FORECASTED_CLOSE_DATE,
	GIFT_MATCH_ACCOUNT_ID,
	GIFT_MATCH_AMOUNT,
	GIFT_MATCH_CONTACT_ID,
	GIFT_MATCHING_COMPANY,
	GIFT_TYPE,
	GRANT_LOI_SUBMITTED_DATE,
	GRANT_OPEN_DATE,
	GRANT_PROPOSAL_DEADLINE,
	GRANT_PROPOSAL_RESPONSE_DATE,
	GRANT_PROPOSAL_SUBMITTED_DATE,
	GRANT_TERM_MONTHS,
	IS_ACKNOWLEDGED,
	IS_WON,
	SOURCE,
	STAGE,
	TRIBUTE_HONOREE,
	TRIBUTE_MESSAGE,
	TRIBUTE_NOTIFICATION_CITY,
	TRIBUTE_NOTIFICATION_COUNTRY,
	TRIBUTE_NOTIFICATION_EMAIL,
	TRIBUTE_NOTIFICATION_METHOD,
	TRIBUTE_NOTIFICATION_PHONE,
	TRIBUTE_NOTIFICATION_POSTAL_CODE,
	TRIBUTE_NOTIFICATION_STATE,
	TRIBUTE_NOTIFICATION_STREET,
	TRIBUTE_NOTIFIED,
	TRIBUTE_PERSON_TO_NOTIFY,
	TRIBUTE_TYPE,
	AUTO_SEND_PROFORMA_INVOICE_EMAILS,
	PREFERRED_EMAIL,
	PROFORMA_INVOICE_EMAIL_SENT_DATE,
	PROFORMA_INVOICE_EMAIL_SENT,
	MASS_PROFORMA_EMAIL_SUBJECT,
	PROCESSING_CHANGES,
	IS_PENDING_PAYMENT,
	OPPORTUNITY_ID,
	IS_COMPLIMENTARY_JOURNAL_DECLINED,
	TRANS_NUMBER,
	IS_2_YR,
	PAYMENT_INTENT_ID,
	IS_REINSTATED,
	IS_TRANSFER,
	USER_SEGMENT_NAME,
	USER_SEGMENT_ACTIVE,
	USER_SEGMENT_DESCRIPTION,
	USER_SEGMENT_QUERY,
	USER_SEGMENT_AVAILABLE,
	EVENT_USER_SEGMENT_ID,
	EVENT_USER_SEGMENT_NAME,
	EARLY_BIRD_PRICE_DEADLINE,
	EARLY_BIRD_PRICE,
	EVENT_PREFERENCES_FORM_FIELDSET,
	PRICE,
	PRIORITY,
	ON_DEMAND_PRICE,
	ON_DEMAND_START_DATE,
	ORDER_TOTAL,
	REGISTRATION_PRICE_CATEGORY
) as
SELECT
    BRP."Id" AS  PARTICIPATION_ID,
    BRP."Name" AS  PARTICIPATION_NUMBER,
    BRP."AC_User_Segment__c" AS USER_SEGMENT,
    BRP."Allergies__c" AS ALLERGIES,
    BRP."BR_Event__c" AS EVENT_ID,
    BRP."Contact__c" AS CONTACT_ID,
    BRP."External_Attendee_Id__c" AS EXTERNAL_ATTENDEE_ID,
    BRP."Fon_Sales_Order__c" AS SALES_ORDER_ID,
    BRP."Food_Preferences__c" AS FOOD_PREFERENCES,
    BRP."Lead__c" AS LEAD_ID,
    BRP."Participate__c" AS PARTICIPANT_STATUS,
    BRP."User__c" AS USER_ID,
    BRP."Vegetarian__c" AS VEGETARIAN,
    BRP."Bypass_Confirmation__c" AS BYPASS_CONFIRMATION,
    BRP."Pre_Post_Doc__c" AS PRE_POST_DOC,
    BRP."Refund_Requested__c" AS REFUND_REQUESTED,
    BRP."Registration_Date__c" AS REGISTRATION_DATE,
    BRP."GDPR_Certification__c" AS GDPR_CERTIFICATION,
    BRP."Source_Code__c" AS SOURCE_CODE_ID,
    BRP."Is_Booth_Staff__c" AS IS_BOOTH_STAFF,
    BRP."Workshop_Groups__c" AS WORKSHOP_GROUP,
    BRP."ADA_Accomodations__c" AS ADA_ACCOMODATIONS,
    BRP."Emergency_Contact_Email__c" AS EMERGENCY_CONTACT_EMAIL,
    BRP."Emergency_Contact_Name__c" AS EMERGENCY_CONTACT_NAME,
    BRP."Emergency_Contact_Phone__c" AS EMERGENCY_CONTACT_PHONE,
    BRP."Emergency_Contact_Relationship__c" AS EMERGENCY_CONTACT_RELATIONSHIP,
    BRP."Joint_Member_ID__c" AS JOINT_MEMBER_ID,
    BRP."Health_Safety_Attestation__c" AS HEALTH_SAFETY_ATTESTATION,
    BRP."Via_Mass_Reg__c" AS VIA_MASS_REG,
    BRP."Registration_Source__c" AS REGISTRATION_SOURCE,
    CASE WHEN SO."OrderApi__Source_Code__c" IS NOT NULL THEN SC."Name" ELSE SC2."Name" END AS COMP_CODE,
    BRP."I_will_be_attending__c" AS I_WILL_BE_ATTENDING,
    SO."Name" AS SALES_ORDER_NUMBER,
    SO."OrderApi__Account__c" AS ACCOUNT_ID,
    SO."OrderApi__Billing_City__c" AS BILLING_CITY,
    SO."OrderApi__Billing_Contact__c" AS BILLING_CONTACT,
    SO."OrderApi__Billing_Country__c" AS BILLING_COUNTRY,
    SO."OrderApi__Billing_Postal_Code__c" AS BILLING_POSTAL_CODE,
    SO."OrderApi__Billing_State__c" AS BILLING_STATE,
    SO."OrderApi__Billing_Street__c" AS BILLING_STREET,
    SO."OrderApi__Business_Group__c" AS BUSINESS_GROUP_ID,
    SO."OrderApi__Calculate_Billing_Details__c" AS CALCULATE_BILLING_DETAILS,
    SO."OrderApi__Calculate_Shipping_Details__c" AS CALCULATE_SHIPPING_DETAILS,
    SO."OrderApi__Campaign_Page__c" AS CAMPAIGN_PAGE,
    SO."OrderApi__Cancelled_Date__c" AS CANCELLED_DATE,
    SO."OrderApi__Closed_Date__c" AS CLOSED_DATE,
    SO."OrderApi__Closed_Status__c" AS CLOSED_STATUS,
    SO."OrderApi__Created_by_Customer__c" AS CREATED_BY_CUSTOMER,
    SO."OrderApi__Customer_Reference_Number__c" AS CUSTOMER_REFERENCE_NUMBER,
    SO."OrderApi__Date__c" AS ORDER_DATE,
    SO."OrderApi__Description__c" AS ORDER_DESCRIPTION,
    SO."OrderApi__EPayment__c" AS E_PAYMENT_ID,
    SO."OrderApi__End_Date__c" AS SCHEDULE_END_DATE,
    SO."OrderApi__Entity__c" AS ENTITY,
    SO."OrderApi__Has_Installments__c" AS HAS_INSTALLMENTS,
    SO."OrderApi__Invoice_Date__c" AS INVOICE_DATE,
    SO."OrderApi__Is_Active__c" AS IS_ACTIVE,
    SO."OrderApi__Is_Cancelled__c" AS IS_CANCELLED,
    SO."OrderApi__Is_Closed__c" AS IS_CLOSED,
    SO."OrderApi__Is_Evergreen__c" AS IS_EVERGREEN,
    SO."OrderApi__Is_Invoiced__c" AS IS_INVOICED,
    SO."OrderApi__Is_Posted__c" AS IS_POSTED,
    SO."OrderApi__Is_Recurring__c" AS IS_RECURRING,
    SO."OrderApi__Is_Scheduled__c" AS IS_SCHEDULED,
    SO."OrderApi__Is_Suspended__c" AS IS_SUSPENDED,
    SO."OrderApi__JSON_Data__c" AS JSON_DATA,
    SO."OrderApi__Memo__c" AS MEMO,
    SO."OrderApi__Number_of_Installments__c" AS NUMBER_OF_INSTALLMENTS,
    SO."OrderApi__Payment_Gateway__c" AS PAYMENT_GATEWAY_ID,
    SO."OrderApi__Payment_Method__c" AS PAYMENT_METHOD_ID,
    SO."OrderApi__Payment_Terms__c" AS PAYMENT_TERMS_ID,
    SO."OrderApi__Posted_Date__c" AS POSTED_DATE,
    SO."OrderApi__Posting_Entity__c" AS POSTING_ENTITY,
    SO."OrderApi__Posting_Status__c" AS POSTING_STATUS,
    SO."OrderApi__Require_Payment_Method__c" AS REQUIRE_PAYMENT_METHOD,
    SO."OrderApi__Schedule_Frequency__c" AS SCHEDULE_FREQUENCY,
    SO."OrderApi__Schedule_Status__c" AS SCHEDULE_STATUS,
    SO."OrderApi__Schedule_Type__c" AS SCHEDULE_TYPE,
    SO."OrderApi__Shipping_City__c" AS SHIPPING_CITY,
    SO."OrderApi__Shipping_Contact__c" AS SHIPPING_CONTACT,
    SO."OrderApi__Shipping_Country__c" AS SHIPPING_COUNTRY,
    SO."OrderApi__Shipping_Postal_Code__c" AS SHIPPING_POSTAL_CODE,
    SO."OrderApi__Shipping_State__c" AS SHIPPING_STATE,
    SO."OrderApi__Shipping_Street__c" AS SHIPPING_STREET,
    SO."OrderApi__Start_Date__c" AS SCHEDULED_START_DATE,
    SO."OrderApi__Status__c" AS STATUS,
    SO."OrderApi__Suspended_Date__c" AS SUSPENDED_DATE,
    SO."OrderApi__Suspended_Reason__c" AS SUSPENDED_REASON,
    SO."OrderApi__System_Refresh__c" AS SYSTEM_REFRESH,
    SO."OrderApi__Valid_Payment_Method__c" AS VALID_PAYMENT_METHOD,
    SO."DonorApi__Acknowledged_Date__c" AS ACKNOWLEDGED_DATE,
    SO."DonorApi__Acknowledgement_Method__c" AS ACKNOWLEDGEMENT_METHOD,
    SO."DonorApi__Do_Not_Acknowledge__c" AS DO_NOT_ACKNOWLEDGE,
    SO."DonorApi__Forecast_Category__c" AS FORECAST_CATEGORY,
    SO."DonorApi__Forecasted_Amount__c" AS FORECASTED_AMOUNT,
    SO."DonorApi__Forecasted_Close_Date__c" AS FORECASTED_CLOSE_DATE,
    SO."DonorApi__Gift_Match_Account__c" AS GIFT_MATCH_ACCOUNT_ID,
    SO."DonorApi__Gift_Match_Amount__c" AS GIFT_MATCH_AMOUNT,
    SO."DonorApi__Gift_Match_Contact__c" AS GIFT_MATCH_CONTACT_ID,
    SO."DonorApi__Gift_Match_Account__c" AS GIFT_MATCHING_COMPANY,
    SO."DonorApi__Gift_Type__c" AS GIFT_TYPE,
    SO."DonorApi__Grant_LOI_Submitted__c" AS GRANT_LOI_SUBMITTED_DATE,
    SO."DonorApi__Grant_Open_Date__c" AS GRANT_OPEN_DATE,
    SO."DonorApi__Grant_Proposal_Deadline__c" AS GRANT_PROPOSAL_DEADLINE,
    SO."DonorApi__Grant_Proposal_Response_Date__c" AS GRANT_PROPOSAL_RESPONSE_DATE,
    SO."DonorApi__Grant_Proposal_Submitted__c" AS GRANT_PROPOSAL_SUBMITTED_DATE,
    SO."DonorApi__Grant_Term__c" AS GRANT_TERM_MONTHS,
    SO."DonorApi__Is_Acknowledged__c" AS IS_ACKNOWLEDGED,
    SO."DonorApi__Is_Won__c" AS IS_WON,
    SO."DonorApi__Source__c" AS SOURCE,
    SO."DonorApi__Stage__c" AS STAGE,
    SO."DonorApi__Tribute_Honoree__c" AS TRIBUTE_HONOREE,
    SO."DonorApi__Tribute_Message__c" AS TRIBUTE_MESSAGE,
    SO."DonorApi__Tribute_Notification_City__c" AS  TRIBUTE_NOTIFICATION_CITY,
    SO."DonorApi__Tribute_Notification_Country__c" AS TRIBUTE_NOTIFICATION_COUNTRY,
    SO."DonorApi__Tribute_Notification_Email__c" AS TRIBUTE_NOTIFICATION_EMAIL,
    SO."DonorApi__Tribute_Notification_Method__c" AS TRIBUTE_NOTIFICATION_METHOD,
    SO."DonorApi__Tribute_Notification_Phone__c" AS TRIBUTE_NOTIFICATION_PHONE,
    SO."DonorApi__Tribute_Notification_Postal_Code__c" AS TRIBUTE_NOTIFICATION_POSTAL_CODE,
    SO."DonorApi__Tribute_Notification_State__c" AS TRIBUTE_NOTIFICATION_STATE,
    SO."DonorApi__Tribute_Notification_Street__c" AS TRIBUTE_NOTIFICATION_STREET,
    SO."DonorApi__Tribute_Notified__c" AS TRIBUTE_NOTIFIED,
    SO."DonorApi__Tribute_Person_to_Notify__c" AS TRIBUTE_PERSON_TO_NOTIFY,
    SO."DonorApi__Tribute_Type__c" AS TRIBUTE_TYPE,
    SO."OrderApi__Auto_Send_Proforma_Invoice_Emails__c" AS AUTO_SEND_PROFORMA_INVOICE_EMAILS,
    SO."OrderApi__Preferred_Email__c" AS PREFERRED_EMAIL,
    SO."OrderApi__Proforma_Invoice_Email_Sent_Date__c" AS PROFORMA_INVOICE_EMAIL_SENT_DATE,
    SO."OrderApi__Proforma_Invoice_Email_Sent__c" AS PROFORMA_INVOICE_EMAIL_SENT,
    SO."OrderApi__Mass_Proforma_Email_Subject__c" AS MASS_PROFORMA_EMAIL_SUBJECT,
    SO."OrderApi__Processing_Changes__c" AS PROCESSING_CHANGES,
    SO."OrderApi__Is_Pending_Payment__c" AS IS_PENDING_PAYMENT,
    SO."Opportunity__c" AS OPPORTUNITY_ID,
    SO."Is_Complimentary_Journal_Declined__c" AS IS_COMPLIMENTARY_JOURNAL_DECLINED,
    SO."Trans_Number__c" AS TRANS_NUMBER,
    SO."Is2YR__c" AS IS_2_YR,
    SO."Payment_Intent_Id__c" AS PAYMENT_INTENT_ID,
    SO."Is_Reinstatement__c" AS IS_REINSTATED,
    SO."Is_Transfer__c" AS IS_TRANSFER,
    US."Name" AS USER_SEGMENT_NAME,
    US."Active__c" AS USER_SEGMENT_ACTIVE,
    US."Description__c" AS USER_SEGMENT_DESCRIPTION,
    US."Query__c" AS USER_SEGMENT_QUERY,
    US."Available__c" AS USER_SEGMENT_AVAILABLE,
    EUS."Id" AS EVENT_USER_SEGMENT_ID,
    EUS."Name" AS EVENT_USER_SEGMENT_NAME,
    EUS."Early_Bird_Price_Deadline__c" AS EARLY_BIRD_PRICE_DEADLINE,
    EUS."Early_Bird_Price__c" AS EARLY_BIRD_PRICE,
    EUS."Event_Preferences_Form_Fieldset__c" AS EVENT_PREFERENCES_FORM_FIELDSET,
    EUS."Price__c" AS PRICE,
    EUS."Priority__c" AS PRIORITY,
    EUS."On_Demand_Price__c" AS ON_DEMAND_PRICE,
    EUS."On_Demand_Start_Date__c" AS ON_DEMAND_START_DATE,
    SOL."OrderApi__Total__c" AS ORDER_TOTAL,
    CASE SOL."OrderApi__Total__c" WHEN EUS."Early_Bird_Price__c" THEN 'Advanced' WHEN EUS."Price__c" THEN 'Regular' WHEN EUS."On_Demand_Price__c" THEN 'On Demand' END AS 	REGISTRATION_PRICE_CATEGORY
FROM REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BRP
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON BRP."Fon_Sales_Order__c" = SO."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL
        ON SO."Id" = SOL."OrderApi__Sales_Order__c"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.AC_USER_SEGMENT__C AS US
        ON BRP."AC_User_Segment__c" = US."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.AC_EVENT_USER_SEGMENT__C AS EUS
        ON BRP."BR_Event__c" = EUS."AC_Event__c" AND BRP."AC_User_Segment__c" = EUS."AC_User_Segment__c"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
        ON SO."OrderApi__Source_Code__c" = SC."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC2
        ON BRP."Source_Code__c" = SC2."Id";