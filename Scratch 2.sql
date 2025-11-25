SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Molecular%';


SELECT * FROM PRODUCTION.MART_CTI_MATCHES.SPEAKER_DEMOGRAPHICS;

SELECT * FROM PRODUCTION.MART_CTI_MATCHES.INVITED_SPEAKERS__V WHERE MEETING_NAME LIKE '%2023%';

SELECT 
    DISTINCT MEETING_NAME
FROM PRODUCTION.MART_CTI_MATCHES.INVITED_SPEAKERS__V;

SELECT DISTINCT "Name" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE;

select meeting_name,count(*) from production.repl_cti.invited_speakers_chairs group by meeting_name
order by 1 desc;


SELECT 
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT
WHERE "Salutation" = 'Prince';


SELECT DISTINCT DEMO_ITEM_NAME FROM PRODUCTION.MART_JOURNAL_SALES.JOURNAL_SALES_BACKUP;


SELECT USER_SEGMENT_NAME, COUNT(*) FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP GROUP BY 1 ORDER BY 2 DESC;

SELECT "Name", "Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Pancreatic%';

/*
2023 Translating Cancer Evolution and Data Science: The Next Frontier
a4j8W000000wykmQAA 


random uniqueregsegid

*/

SELECT 
    PARTICIPATION_ID, 
    CONTACT_ID, 
    USER_SEGMENT_NAME, 
    SALES_ORDER_ID, 
    USER_SEGMENT, 
    EVENT_ID, 
    REGISTRATION_PRICE_CATEGORY 
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP 
WHERE EVENT_ID = 'a4j8W000000ZZ22QAG';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_USER_SEGMENT__C;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C;


a4j8W000000OLYLQA4a4i1I000000Y0yjQACR;



SELECT * FROM PRODUCTION.MART_COMMITTEES.COMMITTEES WHERE COMMITTEE_NAME LIKE '%Fellow%';

CREATE OR REPLACE VIEW PRODUCTION.MART_MEETING_REGISTRATION.EVENT_PRICE_RULES
AS;
SELECT 
    EV."Name" AS EVENT_NAME,
    EUS."AC_Event__c" AS EVENT_ID,
    US."Name" AS USER_SEGMENT_NAME,
    US."Id" AS USER_SEGMENT_ID,
    EUS."Early_Bird_Price__c" AS EARLY_BIRD_PRICE,
    EUS."Early_Bird_Price_Deadline__c" AS EARLY_BIRD_PRICE_DEADLINE,
    EUS."Price__c" AS PRICE,
    EUS."On_Demand_Start_Date__c" AS ON_DEMAND_START_DATE,
    EUS."On_Demand_Price__c" AS ON_DEMAND_PRICE
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_EVENT_USER_SEGMENT__C AS EUS
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_USER_SEGMENT__C AS US
        ON EUS."AC_User_Segment__c" = US."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C AS EV
        ON EV."Id" = EUS."AC_Event__c"
WHERE EV."Id" = 'a4j8W0000019qnOQAQ'--search for event here by replacing with event id, or take this out to see all events
ORDER BY 3;


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE USER_SEGMENT IN ('a4i8W0000000g4bQAA', 'a4i8W000000bQOcQAM');


--Is he in (CY) data term? If so, arfe power query filter

SELECT
    *
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM
WHERE "SALESFORCE ID" = '0031I00000WsUkHQAV'
ORDER BY "TERM CREATED DATE" DESC;

--grab everything that is term year, then sort by term created date, then dedupe

CREATE OR REPLACE VIEW PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM
AS
with Latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY "SALESFORCE ID" ORDER BY "TERM CREATED DATE" DESC) AS "rank"
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL
    WHERE "TERM YEAR" <= YEAR(CURRENT_DATE)
)
SELECT * FROM Latest WHERE "rank" = 1;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT LIMIT 20000;



SELECT 
    COUNT(*)
FROM TEST.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM_TEST;
--WHERE "SALESFORCE ID" = '0031I00000WsUkHQAV';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT WHERE "Name" LIKE '%test%' OR "Name" LIKE '%Test%';


SELECT 
    TL.Record_Id AS "Transaction Line ID"
    , TL.Transaction_Line_Id AS "Transaction Line Name"
    , TL.Transaction_Id AS "Transaction Line C"
    , TL.Credit AS "Transaction Line Credit"
    , TL.Debit AS "Transaction Line Debit"
    , TL.GL_Account AS "Transaction Line GL Account"
    , TL.Item_Class_Id AS "Transaction Line Item Class"
    , TL.Memo AS "Tranaction Line Memo"
    , TL.Receipt_Line_Id AS "Transaction Line Receipt Line"
    , TL.Receipt_Id AS "Transaction Line Receipt"
    , TL.Sales_Order_Line_Id AS "Transaction Line SO Line"
    , TL.Subscription_Line_Id AS "Transaction Line Subscription Line"
    , TL.Subscription_Plan_Id AS "Transaction Line Subscription Plan"
    , TR.Record_Id AS "Transaction Id"
    , TR.Transaction_Number AS "Transaction Name"
    , TR.Contact_Id AS "Transaction Contact Id"
    , TR.Transaction_Date AS "Transaction Date"
    , TR.Memo AS "Transaction Memo"
    , TR.Receipt_Id AS "Transaction Receipt"
    , TR.Sales_Order_Id AS "Transaction Sales Order"
    , TR.Subscription_Id AS "Transaction Subscription"
    , CON.Name AS "Contact Name"
    , GL.Description AS "GL Name"
    , IFF(GL.Description LIKE '2500%' OR GL.Description LIKE '2517%', YEAR(SO.Src_Created_Date) + 1, YEAR(SO.Src_Created_Date)) AS "MEMBERSHIP TRANSACTION YEAR"
    , SO.Src_Created_Date AS "SALES ORDER CREATED DATE"
    , Itemn."Name" AS "Item Name"
    , RC."Id" AS "Receipt ID"
    , SO.Id AS "SALES ORDER ID"
    , SO.Description AS "SALES_ORDER_DESCRIPTION"
    , SC."Name" AS "SOURCE_CODE"
FROM REPL_SALESFORCE.Transaction_Lines AS TL
LEFT JOIN REPL_SALESFORCE.Transactions AS TR
    ON TL.Transaction_Id = TR.Record_Id
LEFT JOIN REPL_SALESFORCE.GL_Accounts AS GL
    ON TL.GL_Account = GL.Id
LEFT JOIN REPL_SALESFORCE.Contacts AS CON
    ON TR.Contact_Id = CON.Id
LEFT JOIN REPL_SALESFORCE.Accounts AS ACC
    ON CON.Account_Id = ACC.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
    ON TL.Item_Class_Id = IC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
    ON TL.Receipt_Id = RC."Id"
LEFT JOIN REPL_SALESFORCE.Sales_Orders AS SO
    ON RC."OrderApi__Sales_Order__c" = SO.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
    ON TL.Item_Id = Itemn."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
    ON SO.Source_Code_Id = SC."Id"
WHERE ACC.Name NOT LIKE '%AACR TEST%'
    AND /*IC."Name" = 'Individual Memberships' OR*/ IC."Name" = 'Internal Staff Use Only';


/* */

CREATE OR REPLACE VIEW PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP
AS
SELECT 
    TL."Id" AS "Transaction Line ID"
    , TL."Name" AS "Transaction Line Name"
    , TL."OrderApi__Transaction__c" AS "Transaction Line C"
    , TL."OrderApi__Credit__c" AS "Transaction Line Credit"
    , TL."OrderApi__Debit__c" AS "Transaction Line Debit"
    , TL."OrderApi__GL_Account__c" AS "Transaction Line GL Account"
    , TL."OrderApi__Item_Class__c" AS "Transaction Line Item Class"
    , TL."OrderApi__Memo__c" AS "Tranaction Line Memo"
    , TL."OrderApi__Receipt_Line__c" AS "Transaction Line Receipt Line"
    , TL."OrderApi__Receipt__c" AS "Transaction Line Receipt"
    , TL."OrderApi__Sales_Order_Line__c" AS "Transaction Line SO Line"
    , TL."OrderApi__Subscription_Line__c" AS "Transaction Line Subscription Line"
    , TL."OrderApi__Subscription_Plan__c" AS "Transaction Line Subscription Plan"
    , TR."Id" AS "Transaction Id"
    , TR."Name" AS "Transaction Name"
    , TR."OrderApi__Contact__c" AS "Transaction Contact Id"
    , TR."OrderApi__Date__c" AS "Transaction Date"
    , TR."OrderApi__Memo__c" AS "Transaction Memo"
    , TR."OrderApi__Receipt__c" AS "Transaction Receipt"
    , TR."OrderApi__Sales_Order__c" AS "Transaction Sales Order"
    , TR."OrderApi__Subscription__c" AS "Transaction Subscription"
    , CON.Name AS "Contact Name"
    , GL.Description AS "GL Name"
    , IFF(GL.Description LIKE '2500%' OR GL.Description LIKE '2517%', YEAR(SO."CreatedDate") + 1, YEAR(SO."CreatedDate")) AS "MEMBERSHIP TRANSACTION YEAR"
    , SO."CreatedDate" AS "SALES ORDER CREATED DATE"
    , Itemn."Name" AS "Item Name"
    , RC."Id" AS "Receipt ID"
    , SO."Id" AS "SALES ORDER ID"
    , SO."OrderApi__Description__c" AS "SALES_ORDER_DESCRIPTION"
    , SC."Name" AS "SOURCE_CODE"
FROM REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    ON TL."OrderApi__Transaction__c" = TR."Id"
LEFT JOIN REPL_SALESFORCE.GL_Accounts AS GL
    ON TL."OrderApi__GL_Account__c" = GL.Id
LEFT JOIN REPL_SALESFORCE.Contacts AS CON
    ON TR."OrderApi__Contact__c" = CON.Id
LEFT JOIN REPL_SALESFORCE.Accounts AS ACC
    ON CON.Account_Id = ACC.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
    ON TL."OrderApi__Item_Class__c" = IC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
    ON TL."OrderApi__Receipt__c" = RC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
    ON RC."OrderApi__Sales_Order__c" = SO."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
    ON TL."OrderApi__Item__c" = Itemn."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
    ON SO."OrderApi__Source_Code__c" = SC."Id"
WHERE IC."Name" IN ('Individual Memberships', 'Internal Staff Use Only') AND ACC.Name NOT LIKE '%AACR TEST%';

create or replace view PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE
AS
SELECT 
    TL.Record_Id AS "Transaction Line ID"
    , TL.Transaction_Line_Id AS "Transaction Line Name"
    , TL.Transaction_Id AS "Transaction Line C"
    , TL.Credit AS "Transaction Line Credit"
    , TL.Debit AS "Transaction Line Debit"
    , TL.GL_Account AS "Transaction Line GL Account"
    , TL.Item_Class_Id AS "Transaction Line Item Class"
    , TL.Memo AS "Tranaction Line Memo"
    , TL.Receipt_Line_Id AS "Transaction Line Receipt Line"
    , TL.Receipt_Id AS "Transaction Line Receipt"
    , TL.Sales_Order_Line_Id AS "Transaction Line SO Line"
    , TL.Subscription_Line_Id AS "Transaction Line Subscription Line"
    , TL.Subscription_Plan_Id AS "Transaction Line Subscription Plan"
    , TR.Record_Id AS "Transaction Id"
    , TR.Transaction_Number AS "Transaction Name"
    , TR.Contact_Id AS "Transaction Contact Id"
    , TR.Transaction_Date AS "Transaction Date"
    , TR.Memo AS "Transaction Memo"
    , TR.Receipt_Id AS "Transaction Receipt"
    , TR.Sales_Order_Id AS "Transaction Sales Order"
    , TR.Subscription_Id AS "Transaction Subscription"
    , CON.Name AS "Contact Name"
    , GL.Description AS "GL Name"
    , IFF(GL.Description LIKE '2500%' OR GL.Description LIKE '2517%', YEAR(SO.Src_Created_Date) + 1, YEAR(SO.Src_Created_Date)) AS "MEMBERSHIP TRANSACTION YEAR"
    , SO.Src_Created_Date AS "SALES ORDER CREATED DATE"
    , Itemn."Name" AS "Item Name"
    , RC."Id" AS "Receipt ID"
    , SO.Id AS "SALES ORDER ID"
    , SO.Description AS "SALES_ORDER_DESCRIPTION"
    , SC."Name" AS "SOURCE_CODE"
FROM REPL_SALESFORCE.Transaction_Lines AS TL
LEFT JOIN REPL_SALESFORCE.Transactions AS TR
    ON TL.Transaction_Id = TR.Record_Id
LEFT JOIN REPL_SALESFORCE.GL_Accounts AS GL
    ON TL.GL_Account = GL.Id
LEFT JOIN REPL_SALESFORCE.Contacts AS CON
    ON TR.Contact_Id = CON.Id
LEFT JOIN REPL_SALESFORCE.Accounts AS ACC
    ON CON.Account_Id = ACC.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
    ON TL.Item_Class_Id = IC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
    ON TL.Receipt_Id = RC."Id"
LEFT JOIN REPL_SALESFORCE.Sales_Orders AS SO
    ON RC."OrderApi__Sales_Order__c" = SO.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
    ON TL.Item_Id = Itemn."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
    ON SO.Source_Code_Id = SC."Id"
WHERE ACC.Name NOT LIKE '%AACR TEST%'
    AND IC."Name" IN ('Individual Memberships', 'Internal Staff Use Only');





GRANT usage ON ALL schemas in database PRODUCTION TO ROLE SYSADMIN;
GRANT select ON ALL tables in database PRODUCTION TO ROLE SYSADMIN;
GRANT select ON ALL views in database PRODUCTION TO ROLE SYSADMIN;


SELECT
    *
FROM TEST.TEST_AREA.MEMFLAT
WHERE AACR_ID NOT IN (SELECT AACR_ID FROM TEST.TEST_AREA.MEMOVERVIEW)
ORDER BY PAIDTHRUDATE DESC;


SELECT
    *
FROM TEST.TEST_AREA.MEMFLAT
WHERE CONTACTID NOT IN (SELECT "SALESFORCE ID" FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM)
ORDER BY PAIDTHRUDATE DESC;

SELECT
    *
FROM  PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM
WHERE "SALESFORCE ID" NOT IN (SELECT CONTACTID FROM TEST.TEST_AREA.MEMFLAT)
AND "TERM YEAR" = 2024;


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM WHERE "NAME" LIKE ;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Id" = 'a4jRm000000OhlOIAS';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER WHERE "Id" = '0058W00000DlaNDQAZ';


with CON AS (
    SELECT
        *,
        CASE
            WHEN CONC."iMIS_ID__c" IS NULL THEN CONC."Salesforce_ID__c"
            ELSE CONC."iMIS_ID__c"
        END AS AACR_ID
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CONC
)
SELECT
    COUNT(*)
FROM CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id"
WHERE CON."Membership_Status__c" IN ('Current', 'Suspended')
    AND RT."Name" IN ('Member', 'Prior Member') AND CON."Email" NOT LIKE '%yopmail.com%'
    AND CON."FirstName" <> 'Test' AND CON."LastName" <> 'Test' AND ACC."Name" NOT LIKE '%AACR Test Accounts%'
    AND CON.AACR_ID NOT IN ('88900', '266223', '1087309')
;


SELECT
    MEMBERSHIP.*,
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MEMBERSHIP
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON MEMBERSHIP."SALESFORCE ID" = CON."Id";









SELECT DISTINCT "OrderApi__Country__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__KNOWN_ADDRESS__C ORDER BY 1 ASC;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__KNOWN_ADDRESS__C;


SELECT "Email", COUNT("Id") FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.Contact WHERE "Email" IS NOT NULL GROUP BY "Email" HAVING COUNT("Id") > 1;


SELECT
    "Email",
    "Id",
    "OrderApi__Personal_Email__c",
    "OrderApi__Work_Email__c",
     COUNT(DISTINCT "Id") OVER (PARTITION BY "Email") AS "Count"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT
WHERE "Email" IN (SELECT "Email" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.Contact WHERE "Email" IS NOT NULL GROUP BY "Email" HAVING COUNT("Id") > 1)
ORDER BY 5 DESC, 1;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__KNOWN_ADDRESS__C WHERE "OrderApi__Country__c" = 'MA';




SELECT * ;


GRANT usage ON ALL schemas in database TEST TO ROLE useradmin;
GRANT select ON ALL tables in database TEST TO ROLE useradmin;



create or replace view PRODUCTION.MART_COMMITTEES.COMMITTEES as
SELECT
    CMT."Description__c" AS DESCRIPTION,
    CMT."Established_Date__c" AS ESTABLISHED_DATE,
    CMT."Id" AS COMMITTEE_ID,
    CMT."IsDeleted" AS IS_DELETED,
    CMT."Is_Active__c" AS IS_ACTIVE,
    CMT."Name" AS COMMITTEE_NAME,
    CMT."Type__c" AS COMMITTEE_TYPE,
    US."Name" AS COMMITTEE_OWNER
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS CMT
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US
        ON CMT."OwnerId" = US."Id"
;


create or replace view PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS 
as
SELECT 
    CM."Committee_Member__c" AS MEMBER_ID,
    CM."Name" AS MEMBER_NUMBER,
    CON."NAME" AS MEMBER_NAME,
    US."Name" AS MEMBER_CREATED_BY,
    CM."Committee_Name__c" AS COMMITTEE_ID,
    CM."Start_Date__c" AS MEMBER_START_DATE,
    CM."End_Date__c" AS MEMBER_END_DATE,
    CM."Member_Role__c" AS MEMBER_ROLE,
    CM."CreatedById" AS MEMBER_CREATED_BY_ID,
    US2."Name" AS COMMITTEE_OWNER,
    US3."Name" AS MEMBER_MODIFIED_BY,
    CMT."Name" AS COMMITTEE_NAME,
    CMT."Description__c" AS COMMITTEE_DESCRIPTION,
    CMT."Established_Date__c" AS COMMITTEE_ESTABLISHED_DATE,
    CMT."Is_Active__c" AS COMMITTEE_IS_ACTIVE,
    CMT."OwnerId" AS COMMITTEE_OWNER_ID,
    CMT."Type__c" AS COMMITTEE_TYPE,
    CON."FIRST_NAME" AS MEMBER_FIRST_NAME,
    CON."LAST_NAME" AS MEMBER_LAST_NAME, 
    CON."AACR_ID" AS MEMBER_AACR_ID,
    CON."EMAIL" AS MEMBER_EMAIL,
    CON."MEMBER_TYPE" AS MEMBER_MEMBER_TYPE,
    CON."TITLE" AS MEMBER_TITLE,
    CON."SALUTATION" AS MEMBER_SALUTATION,
    CON."ACCOUNT_NAME" AS MEMBER_ACCOUNT_NAME
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS CM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE.CONTACTS AS CON
        ON CM."Committee_Member__c" = CON.ID
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS CMT
        ON CMT."Id" = CM."Committee_Name__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US
        ON US."Id" = CM."CreatedById"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US2
        ON US2."Id" = CMT."OwnerId"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US3
        ON US3."Id" = CM."LastModifiedById"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE.ACCOUNTS AS ACC
        ON ACC.ID = CON.ACCOUNT_ID;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEES__C;



SELECT CON."Name", ACC."Name" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
WHERE CON."Name" LIKE '%Zamarin';



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEES__C; /*Committee__c is contact id; name is committee name*/

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C;  /*name is committee name */

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C; /*Committee_Member__c is contact id*/


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '0038W00001TSrKrQAL';


GRANT SELECT ON TABLE REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C TO ROLE USERADMIN;

GRANT SELECT ON TABLE REPL_SALESFORCE_OWNBACKUP.COMMITTEES__C TO ROLE USERADMIN;

GRANT SELECT ON TABLE REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C TO ROLE USERADMIN;




SELECT * FROM IMISDATA.DBO.COMMITTEE;







CREATE OR REPLACE VIEW PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS 
AS
SELECT
    CON."AACR_Announcements__c" AS AACR_ANNOUNCEMENTS,
    CON."AACR_Foundation__c" AS AACR_FOUNDATION,
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    --"Account_Name__c" AS ACCOUNT_NAME,
    CON."AccountId" AS ACCOUNT_ID,
    ACC."Name" AS ACCOUNT_NAME,
    CASE
        WHEN ACC."npe01__SYSTEMIsIndividual__c" = true THEN 'Individual'
        ELSE 'Organization'
    END AS "ACCOUNT_TYPE",
    CON."Additional_Research_Areas__c" AS ADDITIONAL_RESEARCH_AREAS,
    CON."Advocacy_Focus__c" AS ADVOCACY_FOCUS,
    CON."Advocate__c" AS ADVOCATE,
    CON."Advocate_Biography__c" AS ADVOCATE_BIOGRAPHY,
    CON."Annual_Meeting__c" AS ANNUAL_MEETING,
    CON."Application_Status__c" AS APPLICATION_STATUS,
    CON."Assistant_1__c" AS ASSISTANT_1,
    CON."AssistantName" AS ASSISTANT_NAME,
    CON."AssistantPhone" AS ASSISTANT_PHONE,
    CON."Awards_Grants__c" AS AWARDS_GRANTS,
    CON."Bad_Journal_Address__c" AS BAD_JOURNAL_ADDRESS,
    CON."Bad_Mailing_Address__c" AS BAD_MAILING_ADDRESS,
    CON."Bad_Other_Address__c" AS BAD_OTHER_ADDRESS,
    CON."Birthdate" AS BIRTHDATE,
    CON."Blacklisted__c" AS BLACKLISTED,
    --"c4g_Contact_Concatenation__c" AS CONTACT_NAME,
    CASE
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Personal' AND CON."OrderApi__Personal_Email_Opt_Out__c" = false THEN CON."OrderApi__Personal_Email__c"
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Work' AND CON."OrderApi__Work_Email_Opt_Out__c" = false THEN CON."OrderApi__Work_Email__c"
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Other' AND CON."OrderApi__Other_Email_Opt_Out__c" = false THEN CON."OrderApi__Other_Email__c"
    END AS "ORDER_PREFERRED_EMAIL",
    CON."c4g_DS_Rating_Level__c" AS DS_RATING_LEVEL,
    CON."c4g_RFM__c" AS RFM,
    CON."c4g_Volunteer_Organization__c" AS VOLUNTEER_ORGANIZATION_2,
    CON."Campaign_Code__c" AS CAMPAIGN_CODE,
    CON."Cancer_Immunology_Group__c" AS CANCER_IMMUNOLOGY_GROUP,
    CON."Cancer_Policy_Advocacy__c" AS CANCER_POLICY_ADVOCACY,
    CON."Cancer_Today_Magazine__c" AS CANCER_TODAY_MAGAZINE,
    CON."CEO_Preference__c" AS CEO_PREFERENCE,
    CON."cfg_Actively_Prospecting_Major_Gift__c" AS ACTIVELY_PROSPECTING_MAJOR_GIFT,
    CON."cfg_Last_Donation_to_Runner_Date__c" AS LAST_DONATION_TO_RUNNER_DATE,
    CON."cfg_Last_Runner_Date__c" AS LAST_RUNNER_DATE,
    CON."cfg_Prospect_Priority__c" AS PROSPECT_PRIORITY,
    CON."Chemistry_in_Cancer_Research__c" AS CHEMISTRY_IN_CANCER_RESEARCH,
    --"Communication_Preferences__c" AS COMMUNICATION_PREFERENCES,
    CON."Complimentary_Journal__c" AS COMPLIMENTARY_JOURNAL,
    CON."Conferences_Workshops__c" AS CONFERENCES_WORKSHOPS,
    --"Contact_ID__c" AS CONTACT_ID,
    CON."Contact_StudentNominatorName__c" AS CONTACT_STUDENT_NOMINATOR_NAME,
    CON."Contact_StudentNominatorTitle__c" AS CONTACT_STUDENT_NOMINATOR_TITLE,
    CON."Contact_StudentNominatorType__c" AS CONTACT_STUDENT_NOMINATOR_TYPE,
    CON."CreatedById" AS CREATED_BY_ID,
    CON."CreatedDate" AS CREATED_DATE,
    CON."Current_Education_Status__c" AS CURRENT_EDUCATION_STATUS,
    CON."Date_of_Death__c" AS DATE_OF_DEATH,
    CON."ddrive__Configuration__c" AS DONORDRIVE_CONFIGURATION,
    CON."ddrive__Created_in_DonorDrive_By__c" AS DONORDRIVE_CREATED_BY,
    CON."ddrive__Created_in_DonorDrive_Relationship__c" AS DONORDRIVE_CREATED_RELATIONSHIP,
    CON."ddrive__Date_Entered__c" AS DONORDRIVE_DATE_ENTERED,
    CON."ddrive__DonorDrive_Company_Name__c" AS DONORDRIVE_COMPANY_NAME,
    CON."ddrive__DonorDrive_ID__c" AS DONORDRIVE_ID,
    CON."ddrive__DonorDrive_Type__c" AS DONORDRIVE_TYPE,
    CON."ddrive__Has_Extranet_Access__c" AS DONORDRIVE_HAS_EXTRANET_ACCESS,
    CON."ddrive__Is_Managed__c" AS DONORDRIVE_IS_MANAGED,
    CON."ddrive__Is_Offline__c" AS DONORDRIVE_IS_OFFLINE,
    CON."ddrive__Languages__c" AS DONORDRIVE_LANGUAGES,
    CON."ddrive__Last_Modified_by_in_DonorDrive__c" AS DONORDRIVE_LAST_MODIFIED_BY,
    CON."ddrive__Last_Modified_in_DonorDrive__c" AS DONORDRIVE_LAST_MODIFIED,
    CON."ddrive__Level__c" AS DONORDRIVE_LEVEL,
    CON."ddrive__Mobile_Carrier__c" AS DONORDRIVE_MOBILE_CARRIER,
    CON."ddrive__Needs_Merge_Review__c" AS DONORDRIVE_NEEDS_MERGE_REVIEW,
    CON."ddrive__Receive_Texts__c" AS DONORDRIVE_RECEIVE_TEXTS,
    CON."Degrees_Held__c" AS DEGREES_HELD,
    CON."Department" AS DEPARTMENT,
    CON."Description" AS DESCRIPTION,
    CON."Dietary_Needs__c" AS DIETARY_NEEDS,
    CON."DirectoryApi__Directory_Opt_Out__c" AS DIRECTORY_OPT_OUT,
    CON."Disability__c" AS DISABILITY,
    --"DNBoptimizer__DnBContactRecord__c" AS DNB_CONTACT_RECORD,
    CON."Do_Not_Display_in_Directory__c" AS DO_NOT_DISPLAY_IN_DIRECTORY,
    CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
    CON."Do_Not_Mail__c" AS DO_NOT_MAIL,
    CON."Do_Not_Trade_Externally__c" AS DO_NOT_TRADE_EXTERNALLY,
    --"Donor_Roll__c" AS DONOR_ROLL,
    CON."Donor_Roll_Legacy__c" AS DONOR_ROLL_LEGACY,
    CON."DonorApi__Active_Recurring_Gift__c" AS DONOR_ACTIVE_RECURRING_GIFT,
    CON."DonorApi__Auto_Calculate_Contact_Greetings__c" AS DONOR_AUTO_CALCULATE_CONTACT_GREETINGS,
    CON."DonorApi__Auto_Calculate_Current_Ask_Amount__c" AS DONOR_AUTO_CALCULATE_CURRENT_ASK_AMOUNT,
    CON."DonorApi__Current_Ask_Amount__c" AS DONOR_CURRENT_ASK_AMOUNT,
    CON."DonorApi__Deceased__c" AS DONOR_DECEASED,
    --"DonorApi__Default_Greeting__c" AS DONOR_DEFAULT_GREETING,
    CON."DonorApi__Default_Greeting_Type__c" AS DONOR_DEFAULT_GREETING_TYPE,
    CON."DonorApi__Direct_Mail_Opt_Out__c" AS DONOR_DIRECT_MAIL_OPT_OUT,
    CON."DonorApi__Do_Not_Solicit__c" AS DONOR_DO_NOT_SOLICIT,
    CON."DonorApi__First_Gift_Date__c" AS DONOR_FIRST_GIFT_DATE,
    CON."DonorApi__Formal_Greeting__c" AS DONOR_FORMAL_GREETING,
    CON."DonorApi__Gift_Opportunities__c" AS DONOR_GIFT_OPPORTUNITIES,
    CON."DonorApi__Gifts_Outstanding__c" AS DONOR_GIFTS_OUTSTANDING,
    CON."DonorApi__Gifts_Received__c" AS DONOR_GIFTS_RECEIVED,
    --"DonorApi__Gifts_Value__c" AS DONOR_GIFTS_VALUE,
    CON."DonorApi__Include_in_Household_Greetings__c" AS DONOR_INCLUDE_IN_HOUSEHOLD_GREETINGS,
    CON."DonorApi__Languages__c" AS DONOR_LANGUAGES,
    CON."DonorApi__Largest_Gift__c" AS DONOR_LARGEST_GIFT,
    CON."DonorApi__Last_Gift_Amount__c" AS DONOR_LAST_GIFT_AMOUNT,
    CON."DonorApi__Last_Gift_Date__c" AS DONOR_LAST_GIFT_DATE,
    CON."DonorApi__Personal_Greeting__c" AS DONOR_PERSONAL_GREETING,
    CON."DonorApi__Personal_Recognition_Name__c" AS DONOR_PERSONAL_RECOGNITION_NAME,
    CON."DonorApi__Primary__c" AS DONOR_PRIMARY,
    CON."DonorApi__Primary_Relationship_Manager__c" AS DONOR_PRIMARY_RELATIONSHIP_MANAGER,
    CON."DonorApi__Spouse__c" AS DONOR_SPOUSE,
    CON."DonorApi__Suffix__c" AS DONOR_SUFFIX,
    CON."DonorDrive_ID_aacr__c" AS DONORDRIVE_ID_AACR,
    CON."DoNotCall" AS DO_NOT_CALL,
    CON."DRCTS__Directory_Preferences__c" AS DIRECTORY_PREFERENCES,
    CON."dtd__company__c" AS COMPANY_MATCHING_GIFTS,
    CON."Email" AS EMAIL,
    CON."EmailBouncedDate" AS EMAIL_BOUNCED_DATE,
    CON."EmailBouncedReason" AS EMAIL_BOUNCED_REASON,
    CON."et4ae5__HasOptedOutOfMobile__c" AS MOBILE_OPT_OUT,
    CON."et4ae5__Mobile_Country_Code__c" AS MOBILE_COUNTRY_CODE,
    CON."Event_Segment__c" AS EVENT_SEGMENT,
    CON."Expected_Completion_Date__c" AS EXPECTED_COMPLETION_DATE,
    CON."Family_Member_with_Cancer__c" AS FAMILY_MEMBER_WITH_CANCER,
    CON."Fax" AS FAX,
    CON."FirstName" AS FIRST_NAME,
    CON."Formerly_Known_As__c" AS FORMERLY_KNOWN_AS,
    CON."Foundation_Do_Not_Solicit__c" AS FOUNDATION_DO_NOT_SOLICIT,
    CON."Gender__c" AS GENDER,
    --"GW_Volunteers__First_Volunteer_Date__c" AS VOLUNTEER_FIRST_DATE,
    --"GW_Volunteers__Last_Volunteer_Date__c" AS VOLUNTEER_LAST_DATE,
    --"GW_Volunteers__Unique_Volunteer_Count__c" AS VOLUNTEER_UNIQUE_COUNT,
    CON."GW_Volunteers__Volunteer_Auto_Reminder_Email_Opt_Out__c" AS VOLUNTEER_AUTO_REMINDER_EMAIL_OPT_OUT,
    CON."GW_Volunteers__Volunteer_Availability__c" AS VOLUNTEER_AVAILABILITY,
    --"GW_Volunteers__Volunteer_Hours__c" AS VOLUNTEER_HOURS,
    CON."GW_Volunteers__Volunteer_Last_Web_Signup_Date__c" AS VOLUNTEER_LAST_WEB_SIGNUP_DATE,
    CON."GW_Volunteers__Volunteer_Manager_Notes__c" AS VOLUNTEER_MANAGER_NOTES,
    CON."GW_Volunteers__Volunteer_Notes__c" AS VOLUNTEER_NOTES,
    CON."GW_Volunteers__Volunteer_Organization__c" AS VOLUNTEER_ORGANIZATION,
    CON."GW_Volunteers__Volunteer_Skills__c" AS VOLUNTEER_SKILLS,
    CON."GW_Volunteers__Volunteer_Status__c" AS VOLUNTEER_STATUS,
    CON."HasOptedOutOfEmail" AS EMAIL_OPT_OUT,
    CON."HasOptedOutOfFax" AS FAX_OPT_OUT,
    CON."Hear_About_AACR__c" AS HEAR_ABOUT_AACR,
    CON."Highest_Degree__c" AS HIGHEST_DEGREE,
    CON."HomePhone" AS HOME_PHONE,
    CON."Id" AS ID,
    CON."iMIS_Created_Date__c" AS IMIS_CREATED_DATE,
    CON."iMIS_ID__c" AS IMIS_ID,
    CON."Income_Level__c" AS INCOME_LEVEL,
    CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE,
    --"Institution_Company_Name__c" AS INSTITUTION_COMPANY_NAME,
    CON."Institution_Type__c" AS INSTITUTION_TYPE,
    CON."is_affiliated_with_self__c" AS IS_AFFILIATED_WITH_SELF,
    CON."Is_Journal_Addr_Same_As_Preferred_Addr__c" AS IS_JOURNAL_ADDR_SAME_AS_PREFERRED_ADDR,
    CON."IsDeleted" AS IS_DELETED,
    CON."IsEmailBounced" AS IS_EMAIL_BOUNCED,
    CON."Jigsaw" AS JIGSAW_KEY,
    CON."JigsawContactId" AS JIGSAW_CONTACT_ID,
    CON."Journal_City__c" AS JOURNAL_CITY,
    CON."Journal_Country__c" AS JOURNAL_COUNTRY,
    CON."Journal_Email__c" AS JOURNAL_EMAIL,
    CON."Journal_Email_Opt_Out__c" AS JOURNAL_EMAIL_OPT_OUT,
    CON."Journal_State_Province__c" AS JOURNAL_STATE_PROVINCE,
    CON."Journal_Street__c" AS JOURNAL_STREET,
    CON."Journal_Zip_Postal_Code__c" AS JOURNAL_ZIP_POSTAL_CODE,
    CON."LastActivityDate" AS LAST_ACTIVITY_DATE,
    CON."LastCURequestDate" AS LAST_CU_REQUEST_DATE,
    CON."LastCUUpdateDate" AS LAST_CU_UPDATE_DATE,
    CON."LastModifiedById" AS LAST_MODIFIED_BY_ID,
    CON."LastModifiedDate" AS LAST_MODIFIED_DATE,
    CON."LastName" AS LAST_NAME,
    --"LastReferencedDate" AS LAST_REFERENCED_DATE,
    --"LastViewedDate" AS LAST_VIEWED_DATE,
    CON."Lead_List_Reason__c" AS LEAD_LIST_REASON,
    CON."Lead_List_Requestor__c" AS LEAD_LIST_REQUESTOR,
    CON."LeadSource" AS LEAD_SOURCE,
    CON."Maiden_Name__c" AS MAIDEN_NAME,
    CON."MailingCity" AS MAILING_CITY,
    CON."MailingCountry" AS MAILING_COUNTRY,
    CON."MailingCountryCode" AS MAILING_COUNTRY_CODE,
    CON."MailingGeocodeAccuracy" AS MAILING_GEOCODE_ACCURACY,
    CON."MailingLatitude" AS MAILING_LATITUDE,
    CON."MailingLongitude" AS MAILING_LONGITUDE,
    CON."MailingPostalCode" AS MAILING_POSTAL_CODE,
    CON."MailingState" AS MAILING_STATE,
    CON."MailingStateCode" AS MAILING_STATE_CODE,
    CON."MailingStreet" AS MAILING_STREET,
    CON."Major_Focus__c" AS MAJOR_FOCUS,
    CON."Manual_Segment_Override__c" AS MANUAL_SEGMENT_OVERRIDE,
    --"Marketing_Preferences__c" AS MARKETING_PREFERENCES,
    CON."MasterRecordId" AS MASTER_RECORD_ID,
    CON."Member_Type__c" AS MEMBER_TYPE,
    CON."Membership_Id__c" AS MEMBERSHIP_ID,
    CON."Membership_Information__c" AS MEMBERSHIP_INFORMATION,
    CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
    CON."MiddleName" AS MIDDLE_NAME,
    CON."Minorities_in_Cancer_Research__c" AS MINORITIES_IN_CANCER_RESEARCH,
    CON."Minority_Institution__c" AS MINORITY_INSTITUTION,
    CON."MobilePhone" AS MOBILE_PHONE,
    --"Modify_Groups__c" AS MODIFY_GROUPS,
    --"Modify_Institution__c" AS MODIFY_INSTITUTION,
    CON."Molecular_Epidemiology__c" AS MOLECULAR_EPIDEMIOLOGY,
    CON."Name" AS NAME,
    CON."Nickname__c" AS NICKNAME,
    CON."npe01__AlternateEmail__c" AS NPE_ALTERNATE_EMAIL,
    --"npe01__Home_Address__c" AS NPE_HOME_ADDRESS,
    --"npe01__HomeEmail__c" AS NPE_HOME_EMAIL,
    --"npe01__Organization_Type__c" AS NPE_ORGANIZATION_TYPE,
    --"npe01__Other_Address__c" AS NPE_OTHER_ADDRESS,
    --"npe01__Preferred_Email__c" AS NPE_PREFERRED_EMAIL,
    --"npe01__PreferredPhone__c" AS NPE_PREFERRED_PHONE,
    --"npe01__Primary_Address_Type__c" AS NPE_PRIMARY_ADDRESS_TYPE,
    --"npe01__Private__c" AS NPE_PRIVATE,
    --"npe01__Secondary_Address_Type__c" AS NPE_SECONDARY_ADDRESS_TYPE,
    --"npe01__SystemAccountProcessor__c" AS NPE_SYSTEM_ACCOUNT_PROCESSOR,
    --"npe01__Type_of_Account__c" AS NPE_TYPE_OF_ACCOUNT,
    --"npe01__Work_Address__c" AS NPE_WORK_ADDRESS,
    --"npe01__WorkEmail__c" AS NPE_WORK_EMAIL,
    --"npe01__WorkPhone__c" AS NPE_WORK_PHONE,
    CON."npo02__AverageAmount__c" AS NPO_AVERAGE_AMOUNT,
    CON."npo02__Best_Gift_Year__c" AS NPO_BEST_GIFT_YEAR,
    CON."npo02__Best_Gift_Year_Total__c" AS NPO_BEST_GIFT_YEAR_TOTAL,
    CON."npo02__FirstCloseDate__c" AS NPO_FIRST_CLOSE_DATE,
    --"npo02__Formula_HouseholdMailingAddress__c" AS NPE_HOUSEHOLD_MAILING_ADDRESS,
    --"npo02__Formula_HouseholdPhone__c" AS NPE_HOUSEHOLD_PHONE,
    --"npo02__Household__c" AS NPO_HOUSEHOLD,
    --"npo02__Household_Naming_Order__c" AS NPO_HOUSEHOLD_NAMING_ORDER,
    --"npo02__LargestAmount__c" AS NPO_LARGEST_AMOUNT,
    --"npo02__LastCloseDate__c" AS NPO_LAST_CLOSE_DATE,
    --"npo02__LastCloseDateHH__c" AS NPO_LAST_HOUSEHOLD_CLOSE_DATE,
    --"npo02__LastMembershipAmount__c" AS NPO_LAST_MEMBERSHIP_AMOUNT,
    --"npo02__LastMembershipDate__c" AS NPO_LAST_MEMBERSHIP_DATE,
    --"npo02__LastMembershipLevel__c" AS NPO_LAST_MEMBERSHIP_LEVEL,
    --"npo02__LastMembershipOrigin__c" AS NPO_LAST_MEMBERSHIP_ORIGIN,
    --"npo02__LastOppAmount__c" AS NPO_LAST_OPP_AMOUNT,
    --"npo02__MembershipEndDate__c" AS NPO_MEMBERSHIP_END_DATE,
    --"npo02__MembershipJoinDate__c" AS NPO_MEMBERSHIP_JOIN_DATE,
    --"npo02__Naming_Exclusions__c" AS NPO_NAMING_EXCLUSIONS,
    --"npo02__NumberOfClosedOpps__c" AS NPO_NUMBER_OF_CLOSED_OPPS,
    --"npo02__NumberOfMembershipOpps__c" AS NPO_NUMBER_OF_MEMBERSHIP_OPPS,
    --"npo02__OppAmount2YearsAgo__c" AS NPO_OPP_AMOUNT_2_YEARS_AGO,
    --"npo02__OppAmountLastNDays__c" AS NPO_OPP_AMOUNT_LAST_N_DAYS,
    --"npo02__OppAmountLastYear__c" AS NPO_OPP_AMOUNT_LAST_YEAR,
    --"npo02__OppAmountLastYearHH__c" AS NPO_HOUSEHOLD_OPP_AMOUNT_LAST_YEAR,
    --"npo02__OppAmountThisYear__c" AS NPO_OPP_AMOUNT_THIS_YEAR,
    --"npo02__OppAmountThisYearHH__c" AS NPO_HOUSEHOLD_OPP_AMOUNT_THIS_YEAR,
    --"npo02__OppsClosed2YearsAgo__c" AS NPO_OPPS_CLOSED_2_YEARS_AGO,
    --"npo02__OppsClosedLastNDays__c" AS NPO_OPPS_CLOSED_LAST_N_DAYS,
    --"npo02__OppsClosedLastYear__c" AS NPO_OPPS_CLOSED_LAST_YEAR,
    --"npo02__OppsClosedThisYear__c" AS NPO_OPPS_CLOSED_THIS_YEAR,
    --"npo02__SmallestAmount__c" AS NPO_SMALLEST_AMOUNT,
    --"npo02__Soft_Credit_Last_Year__c" AS NPO_SOFT_CREDIT_LAST_YEAR,
    --"npo02__Soft_Credit_This_Year__c" AS NPO_SOFT_CREDIT_THIS_YEAR,
    --"npo02__Soft_Credit_Total__c" AS NPO_SOFT_CREDIT_TOTAL,
    --"npo02__Soft_Credit_Two_Years_Ago__c" AS NPO_SOFT_CREDIT_TWO_YEARS_AGO,
    --"npo02__SystemHouseholdProcessor__c" AS NPO_SYSTEM_HOUSEHOLD_PROCESSOR,
    --"npo02__Total_Household_Gifts__c" AS NPO_TOTAL_HOUSEHOLD_GIFTS,
    --"npo02__TotalMembershipOppAmount__c" AS NPO_TOTAL_MEMBERSHIP_OPP_AMOUNT,
    --"npo02__TotalOppAmount__c" AS NPO_TOTAL_OPP_AMOUNT,
    --"npsp__Address_Verification_Status__c" AS NPSP_ADDRESS_VERIFICATION_STATUS,
    --"npsp__Batch__c" AS NPSP_BATCH,
    --"npsp__Current_Address__c" AS NPSP_CURRENT_ADDRESS,
    --"npsp__CustomizableRollups_UseSkewMode__c" AS NPSP_CUSTOMIZABLE_ROLLUPS_USE_SKEW_MODE,
    --"npsp__Deceased__c" AS NPSP_DECEASED,
    --"npsp__Do_Not_Contact__c" AS NPSP_DO_NOT_CONTACT,
    --"npsp__Exclude_from_Household_Formal_Greeting__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_FORMAL_GREETING,
    --"npsp__Exclude_from_Household_Informal_Greeting__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_INFORMAL_GREETING,
    --"npsp__Exclude_from_Household_Name__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_NAME,
    --"npsp__First_Soft_Credit_Amount__c" AS NPSP_FIRST_SOFT_CREDIT_AMOUNT,
    --"npsp__First_Soft_Credit_Date__c" AS NPSP_FIRST_SOFT_CREDIT_DATE,
    --"npsp__HHId__c" AS NPSP_HOUSEHOLD_ID,
    --"npsp__is_Address_Override__c" AS NPSP_IS_ADDRESS_OVERRIDE,
    --"npsp__Largest_Soft_Credit_Amount__c" AS NPSP_LARGEST_SOFT_CREDIT_AMOUNT,
    --"npsp__Largest_Soft_Credit_Date__c" AS NPSP_LARGEST_SOFT_CREDIT_DATE,
    --"npsp__Last_Soft_Credit_Amount__c" AS NPSP_LAST_SOFT_CREDIT_AMOUNT,
    --"npsp__Last_Soft_Credit_Date__c" AS NPSP_LAST_SOFT_CREDIT_DATE,
    --"npsp__Number_of_Soft_Credits__c" AS NPSP_NUMBER_OF_SOFT_CREDITS,
    --"npsp__Number_of_Soft_Credits_Last_N_Days__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_LAST_N_DAYS,
    --"npsp__Number_of_Soft_Credits_Last_Year__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_LAST_YEAR,
    --"npsp__Number_of_Soft_Credits_This_Year__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_THIS_YEAR,
    --"npsp__Number_of_Soft_Credits_Two_Years_Ago__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_TWO_YEARS_AGO,
    --"npsp__Primary_Affiliation__c" AS NPSP_PRIMARY_AFFILIATION,
    --"npsp__Primary_Contact__c" AS NPSP_PRIMARY_CONTACT,
    --"npsp__Soft_Credit_Last_N_Days__c" AS NPSP_SOFT_CREDIT_LAST_N_DAYS,
    --"npsp__Sustainer__c" AS NPSP_SUSTAINER,
    --"npsp__Undeliverable_Address__c" AS NPSP_UNDELIVERABLE_ADDRESS,
    --"Omit_Household_Label__c" AS OMIT_HOUSEHOLD_LABEL,
    CON."OrderApi__Annual_Engagement_Score__c" AS ORDER_ANNUAL_ENGAGEMENT_SCORE,
    CON."OrderApi__Assistant_Do_Not_Call__c" AS ORDER_ASSISTANT_DO_NOT_CALL,
    CON."OrderApi__Assistant_Email__c" AS ORDER_ASSISTANT_EMAIL,
    CON."OrderApi__Assistant_Email_Opt_Out__c" AS ORDER_ASSISTANT_EMAIL_OPT_OUT,
    CON."OrderApi__Badges__c" AS ORDER_BADGES,
    CON."OrderApi__Home_Do_Not_Call__c" AS ORDER_HOME_DO_NOT_CALL,
    --"OrderApi__Is_Primary_Contact__c" AS ORDER_IS_PRIMARY_CONTACT,
    CON."OrderApi__Lifetime_Engagement_Score__c" AS ORDER_LIFETIME_ENGAGEMENT_SCORE,
    CON."OrderApi__Mobile_Do_Not_Call__c" AS ORDER_MOBILE_DO_NOT_CALL,
    CON."OrderApi__Other_Do_Not_Call__c" AS ORDER_OTHER_DO_NOT_CALL,
    CON."OrderApi__Other_Email__c" AS ORDER_OTHER_EMAIL,
    CON."OrderApi__Other_Email_Opt_Out__c" AS ORDER_OTHER_EMAIL_OPT_OUT,
    CON."OrderApi__Outstanding_Credits__c" AS ORDER_OUTSTANDING_CREDITS,
    CON."OrderApi__Personal_Email__c" AS ORDER_PERSONAL_EMAIL,
    CON."OrderApi__Personal_Email_Opt_Out__c" AS ORDER_PERSONAL_EMAIL_OPT_OUT,
    CON."OrderApi__Potential_Duplicate_Contacts__c" AS ORDER_POTENTIAL_DUPLICATE_CONTACTS,
    --"OrderApi__Preferred_Email__c" AS ORDER_PREFERRED_EMAIL,
    --"OrderApi__Preferred_Email_Type__c" AS ORDER_PREFERRED_EMAIL_TYPE,
    --"OrderApi__Preferred_Phone__c" AS ORDER_PREFERRED_PHONE,
    --"OrderApi__Preferred_Phone_Type__c" AS ORDER_PREFERRED_PHONE_TYPE,
    CON."OrderApi__Price_Rules_Usages__c" AS ORDER_PRICE_RULES_USAGES,
    CON."OrderApi__Privacy_Settings__c" AS ORDER_PRIVACY_SETTINGS,
    CON."OrderApi__Sync_Address_Billing__c" AS ORDER_SYNC_ADDRESS_BILLING,
    CON."OrderApi__Sync_Address_Shipping__c" AS ORDER_SYNC_ADDRESS_SHIPPING,
    CON."OrderApi__Sync_Email__c" AS ORDER_SYNC_EMAIL,
    CON."OrderApi__Sync_Phone__c" AS ORDER_SYNC_PHONE,
    CON."OrderApi__Work_Do_Not_Call__c" AS ORDER_WORK_DO_NOT_CALL,
    CON."OrderApi__Work_Email__c" AS ORDER_WORK_EMAIL,
    CON."OrderApi__Work_Email_Opt_Out__c" AS ORDER_WORK_EMAIL_OPT_OUT,
    CON."OrderApi__Work_Phone__c" AS ORDER_WORK_PHONE,
    CON."Organ_Sites__c" AS ORGAN_SITES,
    CON."Other_Dietary_Needs__c" AS OTHER_DIETARY_NEEDS,
    CON."Other_Research_Areas__c" AS OTHER_RESEARCH_AREAS,
    CON."OtherCity" AS OTHER_CITY,
    CON."OtherCountry" AS OTHER_COUNTRY,
    CON."OtherCountryCode" AS OTHER_COUNTRY_CODE,
    CON."OtherGeocodeAccuracy" AS OTHER_GEOCODE_ACCURACY,
    CON."OtherLatitude" AS OTHER_LATITUDE,
    CON."OtherLongitude" AS OTHER_LONGITUDE,
    CON."OtherPhone" AS OTHER_PHONE,
    CON."OtherPostalCode" AS OTHER_POSTAL_CODE,
    CON."OtherState" AS OTHER_STATE,
    CON."OtherStateCode" AS OTHER_STATE_CODE,
    CON."OtherStreet" AS OTHER_STREET,
    CON."Override_By__c" AS OVERRIDE_BY,
    CON."Override_Date__c" AS OVERRIDE_DATE,
    CON."Override_Reason__c" AS OVERRIDE_REASON,
    CON."OwnerId" AS OWNER_ID,
    CON."PagesApi__Cookie_Usage_Accepted__c" AS PAGES_COOKIE_USAGE_ACCEPTED,
    CON."PagesApi__Site__c" AS PAGES_SITE,
    CON."Paid_thru_date__c" AS PAID_THRU_DATE,
    CON."Personal_Email_Bounced__c" AS PERSONAL_EMAIL_BOUNCED,
    CON."Phone" AS PHONE,
    --"PhotoUrl" AS PHOTO_URL,
    CON."Pre_Post_Doc__c" AS PRE_POST_DOC,
    CON."Preferred_Address__c" AS PREFERRED_ADDRESS,
    CON."Primary_Research_Area_of_Expertise__c" AS PRIMARY_RESEARCH_AREA_OF_EXPERTISE,
    CON."Primary_Stakeholder__c" AS PRIMARY_STAKEHOLDER,
    CON."Primary_Stakeholder_Other__c" AS PRIMARY_STAKEHOLDER_OTHER,
    CON."Prior_Member_Status__c" AS PRIOR_MEMBER_STATUS,
    CON."Professional_Role__c" AS PROFESSIONAL_ROLE,
    CON."Race__c" AS RACE,
    CON."RecordTypeId" AS RECORD_TYPE_ID,
    RT."Name" AS RECORD_TYPE_NAME,
    CON."ReportsToId" AS REPORTS_TO_ID,
    --"RT_ByName__c" AS RECORD_TYPE_NAME,
    CON."Salesforce_ID__c" AS SALESFORCE_ID,
    CON."Salutation" AS SALUTATION,
    --"Scientific_Interests__c" AS SCIENTIFIC_INTERESTS,
    CON."Secondary_Stakeholder__c" AS SECONDARY_STAKEHOLDER,
    CON."Secondary_Stakeholder_Other__c" AS SECONDARY_STAKEHOLDER_OTHER,
    CON."smagicinteract__SMSOptOut__c" AS SMS_OPT_OUT,
    CON."Specific_Research_Areas__c" AS SPECIFIC_RESEARCH_AREAS,
    CON."SSP_Title__c" AS SSP_TITLE,
    CON."Stand_Up_2_Cancer__c" AS STAND_UP_2_CANCER,
    --"Suffix_Text__c" AS SUFFIX_TEXT,
    CON."Survivor__c" AS SURVIVOR,
    CON."Survivor_Advocacy__c" AS SURVIVOR_ADVOCACY,
    CON."SystemModstamp" AS SYSTEM_MODSTAMP,
    CON."Title" AS TITLE,
    CON."Tumor_Microenvironment__c" AS TUMOR_MICROENVIRONMENT,
    --"Unique_Contact_Counter__c" AS UNIQUE_CONTACT_COUNTER,
    CON."Unverified_Email_Address__c" AS UNVERIFIED_EMAIL_ADDRESS,
    --"Verify_ORCID__c" AS VERIFY_ORCID,
    CON."Volunteer_For__c" AS VOLUNTEER_FOR,
    CON."WBIL_Override_Reason__c" AS WBIL_OVERRIDE_REASON,
    CON."Women_in_Cancer_Research__c" AS WOMEN_IN_CANCER_RESEARCH,
    CON."Work_Email_Bounced__c" AS WORK_EMAIL_BOUNCED,
    CON."Work_Setting__c" AS WORK_SETTING,
    CON."Working_Groups__c" AS WORKING_GROUPS,
    --"Source_System" AS SRC_SYS_ID,
    --"ID" AS SRC_RECORD_ID,
    CON."CreatedDate" AS SRC_CREATED_DATE,
    CON."LastModifiedDate" AS SRC_LAST_UPDATED_DATE
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id";



SELECT "Email", COUNT(*) FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT GROUP BY 1 HAVING COUNT("Email") > 1;



SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_USER;

SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSALCONTACT;

SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSAL;


CREATE OR REPLACE SCHEMA TEST.MART_PROPOSAL_CENTRAL;
/*
CREATE OR REPLACE VIEW TEST.MART_PROPOSAL_CENTRAL.GRANTEE_TRACKING
AS
SELECT
    PROG.*,
    PROP.PROPOSALGITITLE,
    PROP.PROPOSALPITITLE,
    PROP.PROPOSALPIEMAIL,
    PROP.PROPOSALINSTNAME,
    PROP.PROPOSALPICOUNTRY,
    PROP.PROPOSALPISTATE,
    PROP.PROPOSALID,
    PROP.CYCLE,
    PROP.PROPOSALAWARDSTATUS,
    AWD.AWARDID,
    AWD.AWARDEEFIRSTNAME,
    AWD.AWARDEELASTNAME,
    AWD.AWARDAMOUNT,
    AWD.AWARDSTARTDATE,
    AWD.AWARDENDDATE,
    PROP.PROPOSALPI1,
    PROP.PROPOSALPI2,
    PROP.PROPOSALDATE
FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_USER AS US
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSALCONTACT AS PCON
        ON US.USERID = PCON.USERID
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSAL AS PROP
        ON PCON.PROPOSALID = PROP.PROPOSALID
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSALSTATUS AS PROPST
        ON PROP.PROPOSALID = PROPST.PROPOSALID
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROGRAMS AS PROG
        ON PROG.PROGRAMID = PROP.PROGRAMID
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_AWARDS AS AWD
        ON AWD.PROPOSALID = PROP.PROPOSALID;*/




--CREATE SCHEMA PRODUCTION.MART_PROPOSAL_CENTRAL;

CREATE OR REPLACE VIEW TEST.MART_PROPOSAL_CENTRAL.GRANTEE_TRACKING
AS
SELECT
    PROG.*,
    PROP.PROPOSALGITITLE,
    PROP.PROPOSALPITITLE,
    PROP.PROPOSALPIEMAIL,
    PROP.PROPOSALINSTNAME,
    PROP.PROPOSALPICOUNTRY,
    PROP.PROPOSALPISTATE,
    PROP.PROPOSALID,
    PROP.CYCLE,
    PROP.PROPOSALAWARDSTATUS,
    AWD.AWARDID,
    AWD.AWARDEEFIRSTNAME,
    AWD.AWARDEELASTNAME,
    AWD.AWARDAMOUNT,
    AWD.AWARDSTARTDATE,
    AWD.AWARDENDDATE,
    PROP.PROPOSALPI1,
    PROP.PROPOSALPI2,
    PROP.PROPOSALDATE
FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_AWARDS AS AWD
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROPOSAL AS PROP
        ON AWD.PROPOSALID = PROP.PROPOSALID
    LEFT JOIN TEST.REPL_PROPOSAL_CENTRAL.DBO_PROGRAMS AS PROG
        ON PROG.PROGRAMID = PROP.PROGRAMID;

CREATE TABLE TEST.MART_PROPOSAL_CENTRAL.GLOBAL_IMPACT_TRACKER
AS
;


--Grant Sean 

GRANT SELECT ON VIEW TEST.MART_PROPOSAL_CENTRAL.GRANTEE_TRACKING TO ROLE USERADMIN;


--1007584










SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_PROGRAMS WHERE GMFUNDINGMECHANISMNAME <> GMPROGRAMNAME;













SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_AWARDPAYMENTSTATUS;

SELECT MEETING_ID, MEETING_NAME FROM PRODUCTION.REPL_CTI.ABSTRACT_PRESENTERS WHERE CATEGORY LIKE '%Immunotherapy%';

SELECT DISTINCT MEETING_NAME FROM PRODUCTION.MART_CTI_MATCHES.ABSTRACT_PRESENTERS__V;



SELECT DISTINCT "Name" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Translation%';

SELECT "ContactId" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "ContactId" <> "npe01__Contact_Id_for_Role__c";

SELECT * FROM PRODUCTION.MART_SF_BADGES.BADGES;

SELECT * FROM PRODUCTION.MART_CONTACT_DIMENSION_TAGS.CONTACT_DIMENSION_TAGS_BACKUP


CREATE OR REPLACE VIEW TEST.TEST_AREA.CONTACT_OPPORTUNITIES_MERGE
AS
with RecentGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
GROUP BY 1
),
Recent AS (
SELECT
    "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
    "CloseDate" DATE_OF_MOST_RECENT_DONATION,
    ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
    "Amount" AS MOST_RECENT_DONATION_AMOUNT
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY),
OPP AS (
    SELECT 
        Recent.CONTACT_ID,
        Recent.MOST_RECENT_DONATION_AMOUNT AS MOST_RECENT_DONATION_AMOUNT,
        Recent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
        RecentGift.TOTAL_AMOUNT AS TOTAL_DONATION_AMOUNT,
        RecentGift.TOTAL_OPPORTUNITIES AS TOTAL_OPPORTUNITIES
    FROM RecentGift JOIN Recent ON RecentGift.CONTACT_ID1 = Recent.CONTACT_ID
    WHERE Recent.ROW_ORDER = 1
)
SELECT
    CON."Id",
    OPP.MOST_RECENT_DONATION_AMOUNT,
    OPP.DATE_OF_MOST_RECENT_DONATION,
    OPP.TOTAL_DONATION_AMOUNT,
    OPP.TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN OPP
       ON OPP.CONTACT_ID = CON."Id";




CREATE OR REPLACE VIEW TEST.MART_SALESFORCE_OBJECTS.CONTACT_OPPORTUNITIES_MERGE
AS
with RecentGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
GROUP BY 1
),
Recent AS (
SELECT
    "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
    "CloseDate" DATE_OF_MOST_RECENT_DONATION,
    ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
    "Amount" AS MOST_RECENT_DONATION_AMOUNT
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY),
OPP AS (
    SELECT 
        Recent.CONTACT_ID,
        Recent.MOST_RECENT_DONATION_AMOUNT AS MOST_RECENT_DONATION_AMOUNT,
        Recent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
        RecentGift.TOTAL_AMOUNT AS TOTAL_DONATION_AMOUNT,
        RecentGift.TOTAL_OPPORTUNITIES AS TOTAL_OPPORTUNITIES
    FROM RecentGift JOIN Recent ON RecentGift.CONTACT_ID1 = Recent.CONTACT_ID
    WHERE Recent.ROW_ORDER = 1
)
SELECT
    CON.*,
    OPP.MOST_RECENT_DONATION_AMOUNT,
    OPP.DATE_OF_MOST_RECENT_DONATION,
    OPP.TOTAL_DONATION_AMOUNT,
    OPP.TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN OPP
       ON OPP.CONTACT_ID = CON."Id";









SELECT * FROM TEST.REPL_PROPOSAL_CENTRAL.DBO_GRANTMAKER;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C WHERE "OrderApi__Badge_Type__c" = 'a0k1I00000umaWLQAY' AND "OrderApi__Is_Active__c" = true;

SELECT 
    CON."Id" AS CONTACT_ID,
    CON."AccountId" AS ACCOUNT_ID,
    CON."LastName",
    CON."FirstName",
    CON."Salutation",
    CON."Name",
    CON."MailingStreet",
    CON."MailingCity",
    CON."MailingState",
    CON."MailingCountry",
    CON."MailingPostalCode",
    CON."Phone",
    CON."Email" AS CONTACT_EMAIL,
    CON."OrderApi__Personal_Email__c" AS PERSONAL_EMAIL,
    CON."Title" AS CONTACT_TITLE,
    BADGE."OrderApi__Badge_Type__c" AS BADGE_TYPE,
    BADGE."Id" AS BADGE_ID,
    BADGE."OrderApi__Is_Active__c" AS ACTIVE
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C AS BADGE
        ON CON."Id" = BADGE."OrderApi__Contact__c"
WHERE BADGE."OrderApi__Badge_Type__c" = 'a0k1I00000umaWLQAY' AND BADGE."OrderApi__Is_Active__c" = true
ORDER BY 6;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C AS BT WHERE BT."Id" = 'a0k1I00000umaWLQAY';


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Immun%';


0038W00001SJJgsQAH;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C WHERE "OrderApi__Contact__c" = '0038W00001SJJgsQAH' AND "OrderApi__Badge_Type__c" = 'a0k1I00000umaWLQAY';



SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE EVENT_ID = 'a4j8W000000ZZ22QAG' AND PARTICIPANT_STATUS = 'Yes';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Pancreatic%';


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C WHERE "Registration_Date__c" = '2024-08-25' AND "BR_Event__c" = 'a4j8W000000ZZ22QAG';



--2024 Tummor Immunology: a4j8W000000ZZ27QAG

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BRP WHERE BRP."BR_Event__c" = 'a4j8W000000ZZ27QAG' AND BRP."Participate__c" = 'Yes';


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER;



CREATE ROLE INTERN;

GRANT usage ON ALL schemas in database TEST TO ROLE INTERN;
GRANT select ON ALL tables in database TEST TO ROLE INTERN;


GRANT usage ON ALL schemas in database PRODUCTION TO ROLE SYSADMIN;
GRANT select ON ALL tables in database PRODUCTION TO ROLE SYSADMIN;





GRANT USAGE ON DATABASE TEST TO ROLE INTERN;
GRANT usage ON ALL schemas in database TEST TO ROLE INTERN;
GRANT select ON ALL tables in database TEST TO ROLE INTERN;
GRANT select ON ALL views in database TEST TO ROLE INTERN;

GRANT USAGE ON DATABASE PRODUCTION TO ROLE INTERN;
GRANT usage ON ALL schemas in database PRODUCTION TO ROLE INTERN;
GRANT select ON ALL tables in database PRODUCTION TO ROLE INTERN;
GRANT select ON ALL views in database PRODUCTION TO ROLE INTERN;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE INTERN;


/*
MDM
Snowflake, AWS
SQL
STMs
Data Governance/compliance
AI
GitHub
Coop leadership
Data Marts (saved money from consultants)
Power BI, certification, training
Salesforce (migration/integrations into Snowflake), Attain bullshit
Stood up entire data infrastructure
Stood up a reporting infrastructure, led trainings, center of excellence


 */


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE CONTACT_ID = '003Rm00000I9Sz0IAF';


--In opportunity object, description contains method of payment
--payment_method is inconsistent
--Appeal code is also important

--DAF's and gifts of stock (stock donation), QCD is another (qualified charitable distribution)


SELECT "Description" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "Description" LIKE '%DAF%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP;


SELECT COUNT(DISTINCT "Id"), "Email" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT GROUP BY 2 ORDER BY 1 DESC;
/*
Disparities: a4jRm000000ChSbIAK
RNA's: a4jRm000000ZabhIAC
Therapeutic: a4jRm000000kV7FIAU
 */

SELECT 
    * 
FROM PRODUCTION.MART_MEETING_REGISTRATION.EVENT_PRICE_RULES 
WHERE EVENT_ID = --insert your event id here;




create or replace view TEST.CONTACT_USER_MERGE.CONTACT_USER_MERGE
as
with PART_COUNT AS (
        SELECT
        BRP."Contact__c",
        COUNT(*) AS PARTICIPATION_COUNT,
        BRP."User__c" AS USER_ID
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BRP
    WHERE BRP."Contact__c" IS NOT NULL AND BRP."User__c" IS NOT NULL
    GROUP BY 1, 3
)
SELECT
    CON.*,
    US."Id" AS USER_ID,
    RT."Id" AS RECORD_TYPE_ID,
    RT."Name" AS RECORD_TYPE_NAME,
    PART_COUNT.PARTICIPATION_COUNT AS PARTICIPATION_COUNT
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US
        ON CON."Id" = US."ContactId"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id"
    LEFT JOIN PART_COUNT
        ON US."Id" = PART_COUNT.USER_ID;




SELECT * FROM TEST.MART_PROPOSAL_CENTRAL.GRANTEE_TRACKING WHERE TRY_CAST(PROPOSALPI1 AS DECIMAL) IS NOT NULL OR TRY_CAST(PROPOSALPI2 AS DECIMAL) IS NOT NULL
UNION ALL ;
SELECT * FROM TEST.MART_PROPOSAL_CENTRAL.GRANTEE_TRACKING WHERE PROPOSALPI1 IS NULL AND PROPOSALPI2 IS NULL;


/*
High Level Engagement Calculations (not just for members but also non-members). I'd like to make the argument that some of our non-members are more engaged that some of our "members"
Required:

Last Login Data: Within last 365 days > Within Last 2 Years > More than 2 Years Ago > Never
Membership Status: Current > Suspended > Suspended (Delinquent) > No status (i.e. non-member)
Attended Events Recently: Within last 365 days > Within Last 2 Years > More than 2 Years Ago > Never

Nice-to-Haves
Opened Email Recently: Last 3 months > last 6 months > Last 12 months > More than 365 days ago
Donated Recently: Within last 365 days > Within Last 2 Years > More than 2 Years Ago > Never


Some sort of scoring where if someone logged in within the last 365 days, is a current member, attended an event recently gets 100 and a never, no status, never gets a zero
Then deduct points for Bad Address and Bad Email
Thus making negative scores possible for the never logged ins

*/
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT; --LastActivityDate: most recent activity (login?)

SELECT DISTINCT MEMBERSHIP_STATUS FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS;

SELECT DISTINCT MEMBER_TYPE FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS;







SELECT * FROM PRODUCTION.REPL_SENDGRID.MESSAGES; --Weird example: MESSAGE_ID = 'BHuE8L9cR4StW8XgfrT6kw.filterdrecv-86675bc4b9-kpnln-1-660BD74F-33.33' has 0 OPEN and 41 CLICKs?

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON JOIN PRODUCTION.REPL_SENDGRID.MESSAGES AS SG ON CON."Email" = SG.TO_EMAIL;


with Emailed AS (
    SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON JOIN PRODUCTION.REPL_SENDGRID.MESSAGES AS SG ON CON."Email" = SG.TO_EMAIL
)
SELECT
    "Id",
    "Name",
    COUNT(*)
FROM Emailed
GROUP BY 1,2
ORDER BY 3 DESC;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '0038W00001zhteLQAQ';


SELECT
    "EMAIL",
    COUNT(*)
FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS
GROUP BY 1
ORDER BY 2 DESC;

SELECT
    *
FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS
WHERE "EMAIL" = 'tracey.husted@bio-techne.com';


SELECT * FROM PRODUCTION.REPL_SENDGRID.MESSAGES;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C;


SELECT "NAME", "AACR_ID" FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS WHERE "LAST_NAME" = 'Gelber';


SELECT BADGE."Id", BADGETYPE."Id", BADGETYPE."Name" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C AS BADGE
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C AS BADGETYPE
        ON BADGE."OrderApi__Badge_Type__c" = BADGETYPE."Id"
WHERE BADGETYPE."Name" LIKE '%Fellow%';




SELECT * FROM TEST.TEST_AREA.INPUTPREP;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C;

SELECT RC."OrderApi__Is_Refund__c",* FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
        ON SO."Id" = RC."OrderApi__Sales_Order__c"
WHERE SO."Name"= '001504737';



SELECT 
    RC."OrderApi__Is_Refund__c" AS IS_REFUND,
    MR.*
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR 
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
        ON MR.SALES_ORDER_ID = RC."OrderApi__Sales_Order__c"
WHERE MR.SALES_ORDER_NUMBER = '001504737';

SELECT * FROM TEST.TEST_AREA.INPUTPREP;

SELECT * FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS WHERE LAST_NAME = 'Werlin';


SELECT COUNT(*), "TERM YEAR" FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL GROUP BY 2 ORDER BY 2 DESC;


SELECT DISTINCT "TERM YEAR" FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM ORDER BY 1 DESC;


with Latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY "SALESFORCE ID" ORDER BY "TERM CREATED DATE" DESC) AS "rank"
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL
    WHERE "TERM YEAR" <= YEAR(CURRENT_DATE) + 1
),
Final AS (
SELECT * FROM Latest WHERE "rank" = 1)

SELECT * FROM Final WHERE "CONTACT NAME" = 'David Wright';




CREATE OR REPLACE VIEW PRODUCTION.MART_MEMBERSHIP_HISTORY.NY_MEMBERSHIP_DATA_TERM
AS
SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP WHERE "TERM YEAR" = YEAR(CURRENT_DATE) + 1;


SELECT * FROM TEST.MART_SENDGRID.SENDGRID_EMAILS ORDER BY MOST_RECENT_OPEN_TIME;

SELECT 
    * 
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    JOIN TEST.MART_SENDGRID.SENDGRID_EMAILS AS SG
        ON CON."Email" = SG.TO_EMAIL
WHERE SG.MOST_RECENT_CLICK_TIME IS NOT NULL;

SELECT COUNT(*) FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS;

SELECT TO_EMAIL FROM TEST.MART_SENDGRID.SENDGRID_EMAILS GROUP BY 1 HAVING COUNT(*) > 1;

SELECT * FROM TEST.MART_SENDGRID.SENDGRID_EMAILS WHERE TO_EMAIL = 'xrick.lu@mail.utoronto.ca';

SELECT DISTINCT * FROM TEST.MART_SENDGRID.SENDGRID_EMAILS;


CREATE OR REPLACE TABLE TEST.TEST_AREA.SENDGRID_FINAL
AS 
SELECT DISTINCT * FROM TEST.MART_SENDGRID.SENDGRID_EMAILS;

SELECT TO_EMAIL FROM TEST.TEST_AREA.SENDGRID_FINAL GROUP BY 1 HAVING COUNT(*) > 1;

with testing AS(
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY TO_EMAIL ORDER BY TO_EMAIL) AS RN
FROM TEST.MART_SENDGRID.SENDGRID_EMAILS
),
final_test AS (
SELECT * FROM testing WHERE RN = 1
)
SELECT TO_EMAIL FROM final_test GROUP BY 1 HAVING COUNT(*) > 1;



--contact

SELECT
    DISTINCT CON."OrderApi__Price_Rules_Usages__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__PRICE_RULE__C;




--In opportunity object, description contains method of payment
--payment_method is inconsistent
--Appeal code is also important

--DAF's and gifts of stock (stock donation), QCD is another (qualified charitable distribution)


SELECT "Description" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY 
WHERE "Description" LIKE '%DAF%' OR "Description" LIKE '%Donor Advised Fund%' OR "Description" LIKE '%donor advised fund%';

SELECT "Description", OPP."c4g_Payment_Method__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
WHERE "Description" LIKE '%Stock%' OR "Description" LIKE '%stock%';

SELECT "Description", OPP."c4g_Payment_Method__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
WHERE "Description" LIKE '%QCD%' OR "Description" LIKE '%Qualified Charitable Contribution%' OR "Description" LIKE '%qualified charitable contribution%';

--Else c4g_Payment_Method

CREATE OR REPLACE VIEW TEST.TEST_AREA.OPPORTUNITY_PAYMENT_DETAILS
AS
SELECT 
    *,
    CASE
        WHEN "Description" LIKE '%DAF%' OR "Description" LIKE '%Donor Advised Fund%' OR "Description" LIKE '%donor advised fund%' THEN 'Donor Advised Fund'
        WHEN "Description" LIKE '%Stock%' AND "c4g_Payment_Method__c" = 'Other' THEN 'Gifts of Stock'
        WHEN "Description" LIKE '%QCD%' OR "Description" LIKE '%Qualified Charitable Contribution%' OR "Description" LIKE '%qualified charitable contribution%' THEN 'Qualified Charitable Contribution'
        ELSE "c4g_Payment_Method__c" 
    END AS "PAYMENT_METHOD"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;


DROP VIEW TEST.TEST_AREA.OPPORTUNITY_PAYMENT_DETAILS;




SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C WHERE "Name" LIKE '%Fellow%';


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE PARTICIPATION_NUMBER IN ('P-0147806','P-0147739');



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CASE;


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP WHERE "TERM YEAR" = 2024 AND "ITEM NAME" = 'Associate Membership';
SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP WHERE "TERM YEAR" = 2025 AND "ITEM NAME" = 'Associate Membership';










SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP 
WHERE "TERM YEAR" = 2024 
    AND "ITEM NAME" = 'Associate Membership' 
    AND "TERM ID" NOT IN (SELECT DISTINCT "TERM ID" FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP WHERE "TERM YEAR" = 2025 
                                    AND "ITEM NAME" = 'Associate Membership');



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%IO%';
SELECT DISTINCT CON.Email FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;



SELECT * FROM PRODUCTION.MART_SPEAKERS.SPEAKERS;


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP;


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.EVENT_PRICE_RULES WHERE EVENT_NAME LIKE '%Disparities%';

SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.EVENT_PRICE_RULES WHERE EVENT_ID = 'a4jRm000000a60TIAQ';




SELECT COM."Name", COUNT(*)
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS MEM
JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS COM ON COM."Id" = MEM."Committee_Name__c"
WHERE 
     MEM."Committee_Name__c" IN (SELECT "Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C WHERE "Type__c" = 'Scientific Working Group')
   AND ("End_Date__c" > '2024-11-26' OR "End_Date__c" = null)
GROUP BY COM."Name";



with CON AS (
    SELECT
        *,
        CASE
            WHEN CONC."iMIS_ID__c" IS NULL THEN CONC."Salesforce_ID__c"
            ELSE CONC."iMIS_ID__c"
        END AS AACR_ID
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CONC
)
SELECT SAB.*, CON.AACR_ID FROM TEST.TEST_AREA.SABCS2024 SAB LEFT JOIN CON ON SAB.EMAIL = CON."Email";



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C;

SELECT * FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS;

SELECT
    CASE 
        WHEN MEMBER_END_DATE > CURRENT_DATE THEN 'Active'
        ELSE 'Inactive'
    END AS IS_ACTIVE,
    *
FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS
;

create or replace view PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS
as
SELECT 
    CM."Committee_Member__c" AS MEMBER_ID,
    CM."Name" AS MEMBER_NUMBER,
    CON."NAME" AS MEMBER_NAME,
    US."Name" AS MEMBER_CREATED_BY,
    CM."Committee_Name__c" AS COMMITTEE_ID,
    CM."Start_Date__c" AS MEMBER_START_DATE,
    CM."End_Date__c" AS MEMBER_END_DATE,
    CASE 
        WHEN MEMBER_END_DATE > CURRENT_DATE THEN 'Active'
        ELSE 'Inactive'
    END AS CURRENT_COMMITTEE_MEMBER,
    CM."Member_Role__c" AS MEMBER_ROLE,
    CM."CreatedById" AS MEMBER_CREATED_BY_ID,
    US2."Name" AS COMMITTEE_OWNER,
    US3."Name" AS MEMBER_MODIFIED_BY,
    CMT."Name" AS COMMITTEE_NAME,
    CMT."Description__c" AS COMMITTEE_DESCRIPTION,
    CMT."Established_Date__c" AS COMMITTEE_ESTABLISHED_DATE,
    CMT."Is_Active__c" AS COMMITTEE_IS_ACTIVE,
    CMT."OwnerId" AS COMMITTEE_OWNER_ID,
    CMT."Type__c" AS COMMITTEE_TYPE,
    CON."FIRST_NAME" AS MEMBER_FIRST_NAME,
    CON."LAST_NAME" AS MEMBER_LAST_NAME, 
    CON."AACR_ID" AS MEMBER_AACR_ID,
    CON."EMAIL" AS MEMBER_EMAIL,
    CON."MEMBER_TYPE" AS MEMBER_MEMBER_TYPE,
    CON."TITLE" AS MEMBER_TITLE,
    CON."SALUTATION" AS MEMBER_SALUTATION,
    CON."ACCOUNT_NAME" AS MEMBER_ACCOUNT_NAME
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS CM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE.CONTACTS AS CON
        ON CM."Committee_Member__c" = CON.ID
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS CMT
        ON CMT."Id" = CM."Committee_Name__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US
        ON US."Id" = CM."CreatedById"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US2
        ON US2."Id" = CMT."OwnerId"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.USER AS US3
        ON US3."Id" = CM."LastModifiedById"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE.ACCOUNTS AS ACC
        ON ACC.ID = CON.ACCOUNT_ID;




ALTER VIEW PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS ADD COLUMN "CURRENT_COMMITTEE_MEMBER" VARCHAR;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" = '003Rm00000Ho7UMIAZ';

CREATE SCHEMA TEST.CONTACT_SNAPSHOTS;

SELECT * FROM PRODUCTION.MART_MARKETING.EMAILS;

ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS DROP COLUMN IF EXISTS PERCENT_DELIVERED;
ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS DROP COLUMN IF EXISTS PERCENT_SENT;
ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS DROP COLUMN IF EXISTS PERCENT_CLICKED;
ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS DROP COLUMN IF EXISTS PERCENT_OPENED;


ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS ADD COLUMN PERCENT_DELIVERED FLOAT;
UPDATE PRODUCTION.MART_MARKETING.EMAILS
SET PERCENT_DELIVERED = TOTAL_DELIVERED / TOTAL_SENT;

ALTER TABLE PRODUCTION.MART_MARKETING.EMAILS ADD COLUMN PERCENT_OPENED FLOAT;
UPDATE PRODUCTION.MART_MARKETING.EMAILS
SET PERCENT_DELIVERED = UNIQUE_OPENS / TOTAL_DELIVERED;






with Employee_Counts AS (
    SELECT
        e.employee_id,
        COUNT(q.query_id) AS query_count,
        q.query_starttime
    FROM employees AS e
        LEFT JOIN queries AS q
    GROUP BY 1
)
SELECT
    --COUNT THE COUNTS
    query_count AS "number of queries",
    COUNT(query_count) as "number of emplpoyees"
FROM Employee_Counts
GROUP BY 1
ORDER BY 1;
--filter by date WHERE query_starttime = july through sep;


with event_count AS (
    SELECT
        CON."Id",
        COUNT(MR.PARTICIPATION_ID) AS PART_COUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        LEFT JOIN PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
            ON CON."Id" = MR.CONTACT_ID
    GROUP BY 1
)
SELECT
    PART_COUNT,
    COUNT(PART_COUNT)
FROM event_count
GROUP BY 1
ORDER BY 1;

SELECT DISTINCT "Type" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "Type" IN ('Individual', 'Induvidual');




CREATE OR REPLACE VIEW TEST.TEST_AREA.TEN_YEAR_GIVING_HISTORY
AS
with filtered AS (
    SELECT 
        OPP."Id" AS OPPORTUNITY_ID,
        OPP."npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        CON."FirstName" AS FIRST_NAME,
        CON."LastName" AS LAST_NAME,
        CON."Salutation" AS SALUTATION,
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        OPP."Name" AS OPPORTUNITY_NAME,
        OPP."Description" AS DESCRIPTION,
        OPP."StageName" AS STAGE_NAME,
        OPP."Type" AS "TYPE",
        OPP."Amount" AS AMOUNT,
        OPP."CloseDate" AS CLOSE_DATE,
        YEAR(OPP."CloseDate") AS CLOSE_YEAR,
        CON."wf_net_worth__c" AS WF_NET_WORTH
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON OPP."npe01__Contact_Id_for_Role__c" = CON."Id"
    --WHERE YEAR(OPP."CloseDate") BETWEEN 2014 AND 2024
    WHERE YEAR(OPP."CloseDate") > 2013
),
year_count AS (
    SELECT 
        CONTACT_ID AS "CON_ID"
    FROM filtered
    GROUP BY 1 
    HAVING COUNT(DISTINCT CLOSE_YEAR) >= 11
)
SELECT 
    *
FROM filtered
WHERE "CONTACT_ID" IN (SELECT "CON_ID" FROM year_count)
ORDER BY CONTACT_ID, CLOSE_DATE
;


SELECT 
    "npe01__Contact_Id_for_Role__c", 
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY 
WHERE YEAR("CloseDate") > 2013 --AND "Type" IN ('Individual', 'Induvidual')
GROUP BY 1 
HAVING COUNT(DISTINCT YEAR("CloseDate")) >= 11;


SELECT
    "Id",
    "npe01__Contact_Id_for_Role__c",
    "Type",
    "CloseDate",
    YEAR("CloseDate"),
    "Amount"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
WHERE "npe01__Contact_Id_for_Role__c" = '0031I00000WrwQLQAZ'
;



SELECT DISTINCT YEAR("CloseDate") FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "npe01__Contact_Id_for_Role__c" = '0031I00000WrtLIQAZ' AND YEAR("CloseDate") > 2013 ORDER BY 1;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "npe01__Contact_Id_for_Role__c" = '0031I00000WrtLIQAZ' AND YEAR("CloseDate") > 2013;



SELECT MIN("CloseDate") FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

SELECT DISTINCT "Type" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

SELECT * FROM PRODUCTION.MART_OPPORTUNITY_DIMENSION_TAGS.OPPORTUNITY_DIMENSION_TAGS_BACKUP;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C WHERE "Name" LIKE '%Chemistry%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C WHERE "Committee_Name__c" = 'a4Q8W0000004sLqUAI';


SELECT * FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS WHERE COMMITTEE_ID = 'a4Q8W0000004sLqUAI';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEES__C;

SELECT * FROM PRODUCTION.MART_MARKETING.EMAILS;

SELECT COUNT(*) FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE "Id" = '0038W00001XCyvzQAD';


SELECT CON."OrderApi__Work_Email_Opt_Out__c" FROM TEST.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE "Id" = '0031I000017VRfDQAW';

SELECT "OrderApi__Work_Email_Opt_Out__c" FROM TEST.TEST_AREA.OWNBACKUP_MIGRATION_COPY_CONTACT WHERE "Id" = '0031I000017VRfDQAW';



with conval AS(
    SELECT 
        VAL."ID_Validation",
        SUBSTR(VAL."ID_Validation", 13, 18) AS CONTACT_ID,
        VAL."Do_Not_Mail__c_Validation" AS VALIDATION_COLUMN
    FROM TEST.TEST_AREA.OWNBACKUP_MIGRATION_VALIDATION_RESULTS AS VAL 
)  
SELECT
    conval.*,
    CON."LastModifiedDate"
FROM conval
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CON."Id" = conval.CONTACT_ID
WHERE VALIDATION_COLUMN NOT IN ('PASS', 'PASS NULL')
ORDER BY 4 DESC;



SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.



;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C;

with Subscriptions AS (
    SELECT
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        CON."Id" AS CONTACT_ID,
        CON."FirstName" AS FIRST_NAME,
        CON."LastName" AS LAST_NAME,
        CON."RecordTypeId" AS RECORD_TYPE_ID,
        CON."Member_Type__c" AS MEMBER_TYPE,
        CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
        CON."Paid_thru_date__c" AS PAID_THRU_DATE,
        CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE, 
        CON."AccountId" AS ACCOUNT_ID,
        SUB."Id" AS SUBSCRIPTION_ID,
        SUB."OrderApi__Current_Term_Start_Date__c" AS TERM_START_DATE,
        SUB."OrderApi__Current_Term_End_Date__c" AS TERM_END_DATE,
        SUB."OrderApi__Status__c" AS STATUS,
        SUB."OrderApi__Is_Active__c" AS IS_ACTIVE,
        SUB."OrderApi__Is_Expired__c" AS IS_EXPIRED,
        SUB."OrderApi__Item__c" AS ITEM,
        IT."Name" AS ITEM_NAME,
        SUB."OrderApi__Item_Class__c" AS ITEM_CLASS,
        ITC."Name" AS ITEM_CLASS_NAME,
        SUB."CreatedById" AS CREATED_BY,
        SUB."CreatedDate" AS CREATED_DATE,
        CON."Income_Level__c" AS WB_INCOME_LEVEL,
        CON."MailingCountry" AS MAILING_COUNTRY,
        CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
        CON."Gender__c" AS GENDER
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON SUB."OrderApi__Contact__c" = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
            ON SUB."OrderApi__Item__c" = IT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
            ON SUB."OrderApi__Item_Class__c" = ITC."Id"
),
filtered AS (      
    SELECT
        *,
        CONCAT(CONTACT_ID, ITEM_NAME) AS UNIQUE_ID
    FROM Subscriptions
    WHERE (MEMBERSHIP_STATUS IN ('Current', 'Suspended'))
        AND 
        (ITEM_NAME LIKE '%PICR%' OR ITEM_NAME LIKE '%HMWG%' OR ITEM_NAME LIKE '%WICR%' OR ITEM_NAME LIKE '%MICR%' OR ITEM_NAME LIKE '%PSWG%' 
        OR ITEM_NAME LIKE '%TME%' OR ITEM_NAME LIKE '%RSM%' OR ITEM_NAME LIKE '%CIMM%' OR ITEM_NAME LIKE '%CICR%' OR ITEM_NAME LIKE '%PCWG%' 
        OR ITEM_NAME LIKE '%CPWG%' OR ITEM_NAME LIKE '%CEWG%')
),
rn AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY UNIQUE_ID ORDER BY PAID_THRU_DATE DESC) AS ROW_NUM
    FROM filtered 
)
SELECT * FROM rn WHERE ROW_NUM = 1;


with Subscriptions AS (
    SELECT
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        CON."Id" AS CONTACT_ID,
        CON."FirstName" AS FIRST_NAME,
        CON."LastName" AS LAST_NAME,
        ACC."Name" AS ACCOUNT_NAME,
        CON."RecordTypeId" AS RECORD_TYPE_ID,
        RT."Name" AS RECORD_TYPE_NAME,
        CON."Member_Type__c" AS MEMBER_TYPE,
        CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
        CON."Paid_thru_date__c" AS PAID_THRU_DATE,
        CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE, 
        CON."AccountId" AS ACCOUNT_ID,
        SUB."Id" AS SUBSCRIPTION_ID,
        SUB."OrderApi__Current_Term_Start_Date__c" AS TERM_START_DATE,
        SUB."OrderApi__Current_Term_End_Date__c" AS TERM_END_DATE,
        SUB."OrderApi__Status__c" AS STATUS,
        SUB."OrderApi__Is_Active__c" AS IS_ACTIVE,
        SUB."OrderApi__Is_Expired__c" AS IS_EXPIRED,
        SUB."OrderApi__Item__c" AS ITEM,
        IT."Name" AS ITEM_NAME,
        SUB."OrderApi__Item_Class__c" AS ITEM_CLASS,
        ITC."Name" AS ITEM_CLASS_NAME,
        SUB."CreatedById" AS CREATED_BY,
        SUB."CreatedDate" AS CREATED_DATE,
        CON."Income_Level__c" AS WB_INCOME_LEVEL,
        CON."MailingCountry" AS MAILING_COUNTRY,
        CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
        CON."Gender__c" AS GENDER
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON SUB."OrderApi__Contact__c" = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
            ON CON."AccountId" = ACC."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
            ON SUB."OrderApi__Item__c" = IT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
            ON SUB."OrderApi__Item_Class__c" = ITC."Id"
),
filtered AS (      
    SELECT
        *,
        CONCAT(CONTACT_ID, ITEM_NAME) AS UNIQUE_ID
    FROM Subscriptions
    WHERE (MEMBERSHIP_STATUS IN ('Current', 'Suspended'))
    AND (ITEM_NAME LIKE '%CICR%')
    AND ACCOUNT_NAME NOT LIKE '%AACR Test Account%'
),
rn AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY UNIQUE_ID ORDER BY TERM_END_DATE DESC) AS ROW_NUM
    FROM filtered 
),
finalmembership AS(
    SELECT * FROM rn WHERE ROW_NUM = 1 AND TERM_END_DATE >= CURRENT_DATE
),
exec AS (
    SELECT 
        CM.*
    FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS AS CM
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON CM.MEMBER_ID = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
    WHERE CM.COMMITTEE_NAME = 'AACR Chemistry in Cancer Research Working Group'
    AND CON."Membership_Status__c" IN ('Current', 'Suspended')
    AND CURRENT_COMMITTEE_MEMBER = 'Active'
    AND MEMBER_ACCOUNT_NAME NOT LIKE '%AACR Test Account%'
)
SELECT
    * 
FROM exec
WHERE MEMBER_AACR_ID NOT IN (SELECT AACR_ID FROM finalmembership)
;


CREATE OR REPLACE VIEW PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES
AS
SELECT
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    CON."Id" AS CONTACT_ID,
    CON."FirstName" AS FIRST_NAME,
    CON."LastName" AS LAST_NAME,
    ACC."Name" AS ACCOUNT_NAME,
    CON."RecordTypeId" AS RECORD_TYPE_ID,
    RT."Name" AS RECORD_TYPE_NAME,
    CON."Member_Type__c" AS MEMBER_TYPE,
    CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
    CON."Paid_thru_date__c" AS PAID_THRU_DATE,
    CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE, 
    CON."AccountId" AS ACCOUNT_ID,
    SUB."Id" AS SUBSCRIPTION_ID,
    SUB."OrderApi__Current_Term_Start_Date__c" AS TERM_START_DATE,
    SUB."OrderApi__Current_Term_End_Date__c" AS TERM_END_DATE,
    SUB."OrderApi__Status__c" AS STATUS,
    SUB."OrderApi__Is_Active__c" AS IS_ACTIVE,
    SUB."OrderApi__Is_Expired__c" AS IS_EXPIRED,
    SUB."OrderApi__Item__c" AS ITEM,
    IT."Name" AS ITEM_NAME,
    SUB."OrderApi__Item_Class__c" AS ITEM_CLASS,
    ITC."Name" AS ITEM_CLASS_NAME,
    SUB."CreatedById" AS CREATED_BY,
    SUB."CreatedDate" AS CREATED_DATE,
    CON."Income_Level__c" AS WB_INCOME_LEVEL,
    CON."MailingCountry" AS MAILING_COUNTRY,
    CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
    CON."Gender__c" AS GENDER
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON SUB."OrderApi__Contact__c" = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
        ON SUB."OrderApi__Item__c" = IT."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
        ON SUB."OrderApi__Item_Class__c" = ITC."Id";








SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Generation%';

with blah AS (
SELECT 
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    MR.*
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON MR.CONTACT_ID = CON."Id"
)
SELECT
    *
FROM blah
WHERE EVENT_ID = 'a4jRm000000rifdIAA' AND PARTICIPANT_STATUS = 'Yes'
AND 
AACR_ID NOT IN (
    '1050553',
    '1093160',
    '1101388',
    '1107230',
    '1112774',
    '1123357',
    '1123719',
    '1134269',
    '1138422',
    '1139137',
    '1140189',
    '1151333',
    '1166072',
    '1196159',
    '1198624',
    '1207258',
    '1207312',
    '1241842',
    '1267462',
    '1270735',
    '1277710',
    '1295828',
    '1296143',
    '1302576',
    '1309666',
    '1309875',
    '1311784',
    '1311871',
    '1315002',
    '1321738',
    '1323742',
    '1323902',
    '1324522',
    '1324559',
    '1325827',
    '1328008',
    '1352145',
    '1353467',
    '1354683',
    '1362763',
    '1368147',
    '1408479',
    '1418560',
    '1418603',
    '1418657',
    '1418710',
    '1418721',
    '1418761',
    '1418767',
    '1418803',
    '1419761',
    '211900',
    '284397',
    '332769',
    '368920',
    '446634',
    '452612',
    '1138756',
    '1153054',
    '1164637',
    '1240272',
    '1250199',
    '1282532',
    '1351268',
    '1353245',
    '1368281',
    '1397997',
    '1398021',
    '1398461',
    '1398498',
    '1400811',
    '1418844',
    '1419377',
    '1108701',
    '1120160',
    '1123667',
    '1123677',
    '1123739',
    '1141483',
    '1214691',
    '1237445',
    '1239088',
    '1240480',
    '1292521',
    '1316298',
    '1317165',
    '1322526',
    '1325251',
    '1355144',
    '1392955',
    '1398504',
    '1401033',
    '1418762',
    '283639',
    '304202',
    '328848',
    '368295',
    '439464',
    '470275',
    '1152916',
    '1245130',
    '1309910',
    '1318104',
    '1354764',
    '1418477',
    '1132957',
    '1322519',
    '1418904',
    '1107445',
    '1148485',
    '1207361',
    '1234059',
    '1260106',
    '1272025',
    '1398015',
    '1398497',
    '1399008',
    '1418871',
    '1419374',
    '55079',
    '1101438',
    '1102079',
    '1147902',
    '1400907',
    '1070099',
    '1155424',
    '1274810',
    '1354798',
    '1399383',
    '1399385',
    '1399386',
    '1400824',
    '460339',
    '465218',
    '1214615',
    '1215613',
    '1216551',
    '1219593',
    '1223734',
    '1224381',
    '1226373',
    '1239071',
    '1239093',
    '1239095',
    '1239778',
    '1246412',
    '1264247',
    '1264447',
    '1264769',
    '1273951',
    '1274505',
    '1274507',
    '1274598',
    '1275176',
    '1275591',
    '1276093',
    '1291475',
    '1317849',
    '1318263',
    '1318947',
    '1324810',
    '1327203',
    '1351280',
    '1353307',
    '1353421',
    '1397980',
    '1398507',
    '1398508',
    '1398536',
    '1398553',
    '1398743',
    '1398914',
    '1399007',
    '1399019',
    '1399308',
    '1399349',
    '1399350',
    '1399395',
    '1399400',
    '1399403',
    '1399406',
    '1399488',
    '1399504',
    '1400821',
    '1400973',
    '1401083',
    '1401084',
    '1418858',
    '1418884',
    '1418921',
    '1418960',
    '1418971',
    '1419120',
    '1419156',
    '1419172',
    '1419379',
    '1419387',
    '1419394',
    '1419395',
    '1419447',
    '1419547',
    '1419768',
    '1419845',
    '1420213',
    '188902',
    '377307',
    '1120777',
    '1123579',
    '1149702',
    '1206613',
    '1221518',
    '1241837',
    '1241838',
    '1246435',
    '1251235',
    '1251351',
    '1264077',
    '1268177',
    '1276977',
    '1306434',
    '1308359',
    '1309936',
    '1311708',
    '1353395',
    '1355456',
    '1356089',
    '1356090',
    '1356094',
    '1356095',
    '1356096',
    '1393798',
    '1398034',
    '1398562',
    '1400825',
    '1400863',
    '1400890',
    '1401044',
    '1418852',
    '1418922',
    '1418972',
    '451484',
    '1112370',
    '1353903',
    '1060731',
    '1114525',
    '1118572',
    '1238826',
    '1264278',
    '1355286',
    '1399512',
    '1419462',
    '1420286',
    '1237469',
    '1400868',
    '1400909',
    '1401076',
    '1205627',
    '1237600',
    '10208',
    '1308730',
    '135281',
    '283241',
    '319018',
    '44057',
    '6611',
    '77372',
    '77374',
    '147059',
    '51779',
    '7232',
    '148174',
    '248604',
    '156736',
    '57070',
    '275561',
    '1114695',
    '131541',
    '15747',
    '220013',
    '221713',
    '260686',
    '283500',
    '320576',
    '9613',
    '24770',
    '251083',
    '13359',
    '155166',
    '1055296',
    '1267698',
    '1311861',
    '1311872',
    '1311876',
    '148734',
    '265941',
    '373616',
    '1029767',
    '1097436',
    '47474',
    '1030472',
    '1032288',
    '1033238',
    '1055499',
    '107841',
    '1266296',
    '156930',
    '271687',
    '331370',
    '361484',
    '364133',
    '368630',
    '458052',
    '63827',
    '1089752',
    '1207340',
    '229490',
    '276114',
    '451751',
    '1207392',
    '1266726',
    '1032980',
    '1033240',
    '1089750',
    '1241017',
    '1257049',
    '1266720',
    '126955',
    '1288409',
    '1314025',
    '274651',
    '348346',
    '352923',
    '443628',
    '446824',
    '460871',
    '1109961',
    '1266731',
    '1274587',
    '1400931',
    '267989',
    '317659',
    '400250',
    '270951',
    '1130445',
    '1275734',
    '153293',
    '159430',
    '235433',
    '350868',
    '359077',
    '1139725',
    '1089754',
    '114706',
    '122324',
    '1228529',
    '1238779',
    '1264207',
    '251715',
    '293211',
    '338584',
    '79144',
    '244450',
    '1123058',
    '1265368',
    '1307652',
    '1325233',
    '455505',
    '1276561',
    '1314953',
    '250661',
    '1058870',
    '14318',
    '4028',
    '123426',
    '6663',
    '132051',
    '328786',
    '1318959'
)
;

with blah AS (
SELECT 
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    MR.*
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON MR.CONTACT_ID = CON."Id"
)
SELECT
    *
FROM blah
WHERE EVENT_ID = 'a4jRm000000rifdIAA' AND PARTICIPANT_STATUS = 'Yes'
AND 
AACR_ID IS NULL;

SELECT
    BRP."Contact__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BRP
WHERE BRP."Contact__c" IN (
    '003Rm00000Z3vXOIAZ',
    '003Rm00000Z3iOqIAJ',
    '003Rm00000Z50gWIAR',
    '003Rm00000Z5lN8IAJ',
    '003Rm00000Z6GSdIAN',
    '003Rm00000Z3xFmIAJ',
    '003Rm00000Z5YhdIAF',
    '003Rm00000Z5uNBIAZ',
    '003Rm00000Z5NRMIA3',
    '003Rm00000Z5WjFIAV',
    '003Rm00000Z5xHpIAJ',
    '003Rm00000Z4K0nIAF'
);


--IF(((CONTACT[Membership_Status__c] = "Current") || (CONTACT[Membership_Status__c] = "Suspended")) 
--&& ((CONTACT[RECORDTYPE.Name] = "Member") || (CONTACT[RECORDTYPE.Name] = "Prior Member")), "Operating Member","Non-operating Member")

SELECT 
    *
FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS AS CMM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CMM.MEMBER_ID = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id"
WHERE (CON."Membership_Status__c" IN ('Current', 'Suspended') AND RT."Name" IN ('Member', 'Prior Member'))
    AND CMM.COMMITTEE_NAME = 'AACR Chemistry in Cancer Research Working Group'
    AND CMM.CURRENT_COMMITTEE_MEMBER = 'Active'
;


--badges = fellow
with SFFellows AS (
    SELECT 
        BG."Id",
        CON."Name",
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        BT."Name"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C AS BG
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C AS BT
            ON BG."OrderApi__Badge_Type__c" = BT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON BG."OrderApi__Contact__c" = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
            ON CON."AccountId" = ACC."Id"
    WHERE BT."Name" LIKE '%Fellow%' AND ACC."Name" NOT LIKE '%AACR Test Accounts%'
)
SELECT 
    SF.AACR_ID,
    FF.AACR_ID
FROM SFFellows AS SF
    FULL OUTER JOIN TEST.TEST_AREA.FELLOWSALL AS FF
        ON SF.AACR_ID = FF.AACR_ID
WHERE SF.AACR_ID IS NULL OR FF.AACR_ID IS NULL
;


with SFFellows AS (
    SELECT 
        BG."Id",
        CON."Name",
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        BT."Name"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE__C AS BG
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C AS BT
            ON BG."OrderApi__Badge_Type__c" = BT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON BG."OrderApi__Contact__c" = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
            ON CON."AccountId" = ACC."Id"
    WHERE BT."Name" LIKE '%Fellow%' AND ACC."Name" NOT LIKE '%AACR Test Accounts%'
)
SELECT 
    COUNT(*)
FROM SFFellows;



SELECT COUNT(DISTINCT AACR_ID) FROM TEST.TEST_AREA.FELLOWSALL;

SELECT COUNT(*) FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS;

SELECT * FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS ORDER BY RANDOM() LIMIT 1000;

SELECT DISTINCT SNAPSHOT_DATE FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS;

SELECT TRY_TO_TIMESTAMP(SNAPSHOT_DATE::VARCHAR) FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS;

SELECT TRY_CAST(SNAPSHOT_DATE AS VARCHAR) FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS;;

SELECT COUNT(*) FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%JCA%';

--2025 AACR IO: a4jRm000000a60TIAQ
--2025 JCA: a4j8W0000019qnOQAQ

SELECT
    *
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE EVENT_ID = 'a4jRm000000a60TIAQ' AND PARTICIPANT_STATUS = 'Yes' AND SALES_ORDER_ID IS NULL;

with CONT AS (
    SELECT
        *,    
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
)
SELECT
    CONT.AACR_ID,
    MR.COMP_CODE,
    MR.*
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
    LEFT JOIN CONT
        ON MR.CONTACT_ID = CONT."Id"
WHERE MR.EVENT_ID = 'a4j8W0000019qnOQAQ' AND MR.PARTICIPANT_STATUS = 'Yes'
AND CONT.AACR_ID NOT IN (
    '159262',
    '263045',
    '263949',
    '117591',
    '101545',
    '15351',
    '222972',
    '341966',
    '124732',
    '365305',
    '374513',
    '262952',
    '15224',
    '14462',
    '1033755',
    '16012',
    '1088472',
    '156377',
    '236035',
    '294048',
    '146766',
    '74703',
    '144512',
    '77372',
    '230786',
    '145047',
    '151278',
    '13635',
    '246806',
    '288151',
    '46239',
    '1245209',
    '155153',
    '111874',
    '79243',
    '272943',
    '86152',
    '110554',
    '87547',
    '14065',
    '7231',
    '84550',
    '72897',
    '1398661',
    '318520',
    '231914',
    '1393147',
    '159025',
    '14203',
    '1116064',
    '1156239',
    '1199432',
    '1223226',
    '13429',
    '138641',
    '14081',
    '240947',
    '29763',
    '345544',
    '37280',
    '470270',
    '52328',
    '12023',
    '119008',
    '446798',
    '321588',
    '326180',
    '13940',
    '77243',
    '73748',
    '337603',
    '1308009',
    '1394214',
    '1221112',
    '1311124',
    '1087370',
    '1392559',
    '445021',
    '1221881',
    '447873',
    '1229102',
    '1299769',
    '462768',
    '1214286',
    '1245424',
    '1298115',
    '1369294',
    '1220394',
    '1304170',
    '1239829',
    '271747',
    '1360508',
    '1360977',
    '447218',
    '1024809',
    '1299507',
    '243421',
    '328906',
    '1299341',
    '1241060',
    '1357473',
    '1034593',
    '1355234',
    '1398649',
    '1396088',
    '1392650',
    '1211924',
    '1369038',
    '1368038',
    '1369208',
    '1393237',
    '1393463',
    '1393721',
    '1398283',
    '348627',
    '1364444',
    '1397885',
    '17175',
    '285990',
    '454705',
    '1360893',
    '284228',
    '1359973',
    '1359500',
    '1398237',
    '1369209',
    '1395774',
    '1364663',
    '1364685',
    '1159003',
    '1398699',
    '1268476',
    '1398298',
    '1392545',
    '1360958',
    '1364447',
    '1392632',
    '1276091',
    '1392682',
    '1364676',
    '296846',
    '1369106',
    '1369133',
    '1392628',
    '1392556',
    '1398655',
    '1369134',
    '1395683',
    '1360439',
    '1398677',
    '1392665',
    '331651',
    '1302365',
    '1392579',
    '266548',
    '154679',
    '469179',
    '1138887',
    '128032',
    '446208',
    '16021',
    '358042',
    '119205',
    '1074050',
    '273430',
    '382939',
    '1052605',
    '1227010',
    '455687',
    '1357601',
    '1361135',
    '451757',
    '1398182',
    '1242862',
    '1419818',
    '1369170',
    '1369053',
    '1266341',
    '1249499',
    '13945',
    '1419819',
    '469299',
    '1090362',
    '1419713',
    '1419714',
    '1300681',
    '1420226',
    '1419709',
    '1246986',
    '1223278',
    '1392566',
    '1364788',
    '17556',
    '237832',
    '246596',
    '347378',
    '63544',
    '14705',
    '11379',
    '151039',
    '359437',
    '369531',
    '110370',
    '288354',
    '232323',
    '275622',
    '89380',
    '268615',
    '29244',
    '79149',
    '406794',
    '116090',
    '151329',
    '299750',
    '391768',
    '14987',
    '223568',
    '317529',
    '1369331',
    '317344',
    '85506',
    '447008',
    '1152013',
    '230763',
    '273786',
    '364175',
    '235958',
    '1150898',
    '1420509',
    '469319',
    '1123561',
    '1123615',
    '1123579'

); --JCA attendees who are not in SC report


SELECT 
    *
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE CONTACT_ID = '0031I00000WruezQAB' AND EVENT_ID = 'a4j8W0000019qnOQAQ';
    
SELECT 
    EV."Name" AS EVENT_NAME,
    EUS."AC_Event__c" AS EVENT_ID,
    TO_DATE(EV."Start_Date__c") AS EVENT_START_DATE,
    US."Name" AS USER_SEGMENT_NAME,
    US."Id" AS USER_SEGMENT_ID,
    EUS."Early_Bird_Price__c" AS EARLY_BIRD_PRICE,
    EUS."Early_Bird_Price_Deadline__c" AS EARLY_BIRD_PRICE_DEADLINE,
    EUS."Price__c" AS PRICE,
    EUS."On_Demand_Start_Date__c" AS ON_DEMAND_START_DATE,
    EUS."On_Demand_Price__c" AS ON_DEMAND_PRICE
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_EVENT_USER_SEGMENT__C AS EUS
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_USER_SEGMENT__C AS US
        ON EUS."AC_User_Segment__c" = US."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C AS EV
        ON EV."Id" = EUS."AC_Event__c"
        --search for event here by replacing with event id, or take this out to see all events
WHERE EUS."Id" = 'a4h8W000000rW4ZQAU'
ORDER BY 4;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C AS EV
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_EVENT_USER_SEGMENT__C AS EUS
        ON EV."Id" = EUS."AC_Event__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.AC_USER_SEGMENT__C AS US
        ON US."Id" = EUS."AC_User_Segment__c"
WHERE EUS."Id" = 'a4h8W000000rW4ZQAU';

SELECT "LastModifiedDate", "CreatedDate", * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Id" IN (
    '003Rm00000SeccxIAB',
    '0031I00000Ws9irQAB',
    '0038W00001VQeQIQA1',
    '0038W00001TTWPBQA5',
    '003Rm00000TBky6IAD',
    '003Rm00000PlHVFIA3',
    '003Rm00000R7ztgIAB',
    '003Rm00000TC87cIAD',
    '003Rm00000VThm9IAD',
    '003Rm00000SeDY8IAN',
    '003Rm00000RCgaJIAT',
    '003Rm00000VXqz0IAD',
    '0031I00000WsSNJQA3',
    '003Rm00000Rjk2LIAR',
    '003Rm00000SxsIoIAJ',
    '003Rm00000SXNyvIAH',
    '003Rm00000SkJsLIAV',
    '0031I00000WsKlGQAV',
    '003Rm00000WFPRwIAP',
    '003Rm00000SqusQIAR',
    '0031I0000198evdQAA',
    '003Rm00000SSNhbIAH',
    '003Rm00000SwQDRIA3',
    '003Rm00000RCgqQIAT',
    '003Rm00000TBLq7IAH',
    '003Rm00000P4RCvIAN',
    '003Rm00000RK52RIAT',
    '003Rm00000SZFU2IAP',
    '003Rm00000RdX1aIAF',
    '003Rm00000Rq57KIAR',
    '003Rm00000SYOStIAP',
    '0031I00000Ws8YJQAZ',
    '0031I00000Wrvi9QAB',
    '0031I00000WsDaRQAV',
    '003Rm00000Sw94gIAB',
    '0031I00000Wsll4QAB',
    '003Rm00000T1gW5IAJ',
    '003Rm00000WgbSQIAZ',
    '0031I00000WruezQAB',
    '0031I00000Ws8JiQAJ',
    '003Rm00000RXRX8IAP',
    '003Rm00000YXmYgIAL',
    '0038W00001SGu2hQAD',
    '003Rm00000SpDeDIAV',
    '0031I00001964ekQAA',
    '0031I00000ZECdxQAH',
    '0031I00001MmB4qQAF',
    '0038W00001kKrFUQA0',
    '0038W00001VTwbjQAD',
    '0038W00001uDmpdQAC',
    '003Rm00000SwRe7IAF',
    '003Rm00000Qw7QVIAZ',
    '0038W00001qojZSQAY',
    '003Rm00000QjWpZIAV',
    '0031I00000Ws5l5QAB',
    '0031I00000WsKweQAF',
    '0031I000016IDoSQAW',
    '003Rm00000SxBiTIAV',
    '003Rm00000Tt9QBIAZ',
    '0031I00000Wrv4aQAB',
    '0031I00000ZH0hwQAD',
    '0031I00000WrwSzQAJ',
    '0031I00000WsLpQQAV',
    '0031I00000WrtlNQAR'
);



SELECT SNAPSHOT_DATE FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS;


SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.EVENT_PRICE_RULES WHERE EVENT_ID = 'a4j8W0000019qnOQAQ';



SELECT NOT NULL CON.phone FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;


SELECT
    *
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE EVENT_ID = 'a4jRm000000a60TIAQ' AND PARTICIPANT_STATUS = 'Yes' AND SALES_ORDER_ID IS NULL;


--UniqueRegSegID


with conval AS(
    SELECT 
        VAL."ID_Validation",
        SUBSTR(VAL."ID_Validation", 13, 18) AS CONTACT_ID,
        VAL."Do_Not_Email__c_Validation" AS VALIDATION_COLUMN
    FROM TEST.TEST_AREA.OWNBACKUP_MIGRATION_VALIDATION_RESULTS AS VAL 
)  
SELECT
    conval.*,
    CON."LastModifiedDate"
FROM conval
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CON."Id" = conval.CONTACT_ID
WHERE VALIDATION_COLUMN NOT IN ('PASS', 'PASS NULL')
ORDER BY 4 DESC;

SELECT 
    *
FROM TEST.TEST_AREA.OWNBACKUP_MIGRATION_VALIDATION_RESULTS AS VAL;

/*
select
"BillingStreet",
count( "BillingStreet") as num
from ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_ACCOUNT
group by "BillingStreet"
;

select
"Account_Email__c",
count( "Account_Email__c") as num
from ATTAIN_MIGRATION.MIGRATION_TO_NPC.ACCOUNT
group by "Account_Email__c"
;
*/

with VALIDATION_TEST AS (
    SELECT
        FC."DonorApi__Direct_Mail_Opt_Out__c" AS C,
        AC."Direct_Mail_Opt_Out__c" AS A
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS FC
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.ACCOUNT AS AC
            ON FC."Id" = AC."Fonteva_Contact_ID__c"
)
SELECT
    *
FROM VALIDATION_TEST
WHERE C <> A;

with VALIDATION_TEST AS (
    SELECT
        FC."DonorApi__Direct_Mail_Opt_Out__c" AS C,
        AC."Direct_Mail_Opt_Out__c" AS A
    FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS FC
        LEFT JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.ACCOUNT AS AC
            ON FC."Id" = AC."Fonteva_Contact_ID__c"
)
SELECT
    *
FROM VALIDATION_TEST
WHERE C IS NOT NULL AND A IS NOT NULL;


SELECT * FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS WHERE "MEMBER_MEMBER_TYPE" IS NULL;



SELECT * FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES;



with Subscriptions AS (
    SELECT
        CASE
            WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
            ELSE CON."iMIS_ID__c"
        END AS AACR_ID,
        CON."Id" AS CONTACT_ID,
        CON."FirstName" AS FIRST_NAME,
        CON."LastName" AS LAST_NAME,
        ACC."Name" AS ACCOUNT_NAME,
        CON."RecordTypeId" AS RECORD_TYPE_ID,
        RT."Name" AS RECORD_TYPE_NAME,
        CON."Member_Type__c" AS MEMBER_TYPE,
        CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
        CON."Paid_thru_date__c" AS PAID_THRU_DATE,
        CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE, 
        CON."AccountId" AS ACCOUNT_ID,
        SUB."Id" AS SUBSCRIPTION_ID,
        SUB."OrderApi__Current_Term_Start_Date__c" AS TERM_START_DATE,
        SUB."OrderApi__Current_Term_End_Date__c" AS TERM_END_DATE,
        SUB."OrderApi__Status__c" AS STATUS,
        SUB."OrderApi__Is_Active__c" AS IS_ACTIVE,
        SUB."OrderApi__Is_Expired__c" AS IS_EXPIRED,
        SUB."OrderApi__Item__c" AS ITEM,
        IT."Name" AS ITEM_NAME,
        SUB."OrderApi__Item_Class__c" AS ITEM_CLASS,
        ITC."Name" AS ITEM_CLASS_NAME,
        SUB."CreatedById" AS CREATED_BY,
        SUB."CreatedDate" AS CREATED_DATE,
        CON."Income_Level__c" AS WB_INCOME_LEVEL,
        CON."MailingCountry" AS MAILING_COUNTRY,
        CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
        CON."Gender__c" AS GENDER
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON SUB."OrderApi__Contact__c" = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
            ON CON."AccountId" = ACC."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
            ON SUB."OrderApi__Item__c" = IT."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
            ON SUB."OrderApi__Item_Class__c" = ITC."Id"
),
filtered AS (      
    SELECT
        *,
        CONCAT(CONTACT_ID, ITEM_NAME) AS UNIQUE_ID
    FROM Subscriptions
    WHERE (MEMBERSHIP_STATUS IN ('Current', 'Suspended'))
    AND (ITEM_NAME LIKE '%CICR%')
    AND ACCOUNT_NAME NOT LIKE '%AACR Test Account%'
),
rn AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY UNIQUE_ID ORDER BY TERM_END_DATE DESC) AS ROW_NUM
    FROM filtered 
),
finalmembership AS(
    SELECT * FROM rn WHERE ROW_NUM = 1 AND TERM_END_DATE >= CURRENT_DATE
),
exec AS (
    SELECT 
        CM.*
    FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS AS CM
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON CM.MEMBER_ID = CON."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
    WHERE CM.COMMITTEE_NAME = 'AACR Chemistry in Cancer Research Working Group'
    AND CON."Membership_Status__c" IN ('Current', 'Suspended')
    AND CURRENT_COMMITTEE_MEMBER = 'Active'
    AND MEMBER_ACCOUNT_NAME NOT LIKE '%AACR Test Account%'
)
SELECT
    * 
FROM exec
WHERE MEMBER_AACR_ID NOT IN (SELECT AACR_ID FROM finalmembership)
;



SELECT * FROM PRODUCTION.MART_SPEAKERS.SPEAKERS;


SELECT * FROM PRODUCTION.MART_SPEAKERS.SPEAKERS ORDER BY RANDOM();

SELECT DISTINCT SNAPSHOT_DATE FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS ORDER BY RANDOM() LIMIT 1000;

SELECT 
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
WHERE "npe01__Contact_Id_for_Role__c" = '0031I00000Wsm9XQAR'

;



/*
TotalSpend = 
var Total = CALCULATE(SUM('OPPORTUNITIES (2)'[AMOUNT]), ALLEXCEPT('OPPORTUNITIES (2)', 'OPPORTUNITIES (2)'[NPE_CONTACT_ID_FOR_ROLE])) + 0
RETURN
    IF(ISBLANK(Total), 0, Total)


 */


SELECT 
    SUM("Amount")
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
WHERE "npe01__Contact_Id_for_Role__c" = '0031I00000WryIfQAJ' AND "CloseDate" BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY "npe01__Contact_Id_for_Role__c";

SELECT 
    "npe01__Contact_Id_for_Role__c",
    "CloseDate",
    "Amount"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;


CREATE OR REPLACE VIEW TEST.TEST_AREA."24DonationsTotal"
AS
with OPP AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM("Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    WHERE "CloseDate" BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY "npe01__Contact_Id_for_Role__c"
),
TOTAL AS (
    SELECT 
        *
     FROM OPP WHERE TOTAL_AMOUNT > 0 AND CONTACT_ID IS NOT NULL
)
SELECT
    CONTACT_ID, 
    TOTAL_AMOUNT,
    'Yes' AS HIGH_DONOR_STATUS
FROM TOTAL;

with OPP AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM("Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    WHERE "CloseDate" BETWEEN '2024-01-01' AND '2024-12-31' AND "npe01__Contact_Id_for_Role__c" IS NOT NULL
    GROUP BY "npe01__Contact_Id_for_Role__c"
    HAVING SUM("Amount") >=500
)
SELECT 
    OPPR."Amount",
    OPPR."Id",
    ROW_NUMBER() OVER(PARTITION BY OPPR."Id" ORDER BY CON."Id"),
    CON."Id",
    CON."Name",
    OPP.TOTAL_AMOUNT,
    EV."Name"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPPR
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON OPPR."npe01__Contact_Id_for_Role__c" = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C BRP
        ON CON."Id" = BRP."Contact__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C AS EV
        ON BRP."BR_Event__c" = EV."Id"
    INNER JOIN OPP
        ON CON."Id" = OPP.CONTACT_ID
WHERE EV."Name" LIKE '%Annual Meeting 2025%';

SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM("Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    WHERE "CloseDate" BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY "npe01__Contact_Id_for_Role__c"
    HAVING SUM("Amount") >=500;


SELECT SNAPSHOT_DATE, DATEADD(S, SNAPSHOT_DATE/1000000000, '1970-01-01') FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS LIMIT 10;

SELECT SNAPSHOT_DATE, TO_TIMESTAMP(SNAPSHOT_DATE) FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS LIMIT 1000;

CREATE OR REPLACE TABLE TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL
AS
with DC AS (
    SELECT
        *,
        DATEADD(S, SNAPSHOT_DATE/1000000000, '1970-01-01') AS CONVERTED_SNAPSHOT_DATE
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS
),
Dupes AS (
    SELECT
        *,
        CONCAT("Id", CONVERTED_SNAPSHOT_DATE) AS ID_KEY
    FROM DC
),
RN AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY ID_KEY ORDER BY CONVERTED_SNAPSHOT_DATE) AS ID_DUPE
    FROM Dupes 
)
SELECT * FROM RN WHERE ID_DUPE = 1;




SELECT * FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL ORDER BY RANDOM() LIMIT 1000;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT WHERE "Name" NOT LIKE '%Individual%';


GRANT CREATE TABLE ON SCHEMA TEST.CONTACT_SNAPSHOTS TO ROLE SYSADMIN;
GRANT MODIFY ON ALL TABLES IN SCHEMA TEST.CONTACT_SNAPSHOTS TO ROLE SYSADMIN;

GRANT SELECT ON ALL TABLES IN SCHEMA PRODUCTION.REPL_RINGGOLD TO ROLE SYSADMIN;

GRANT USAGE ON ALL TABLES IN SCHEMA PRODUCTION.REPL_RINGGOLD TO ROLE SYSADMIN;




SELECT c."Id", rl."OrderApi__Line_Description__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT c
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C r on (c."Id" = r."OrderApi__Contact__c")
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT_LINE__C rl on (r."Id" = rl."OrderApi__Receipt__c");

/*
--CY Record Pushed = IF(ISBLANK(CONTACTS[CY MEMBERSHIP_DATA_TERM.Transaction Id]),"Record Pushed","Member Interaction")
--CY Sale Price = CONTACTS[CY MEMBERSHIP_DATA_TERM.Transaction Line Credit] - CONTACTS[CY MEMBERSHIP_DATA_TERM.Transaction Line Debit] +0

--CY Membership Status = IF(CONTACTS[CY Sale Price] >0, "Paid",
--  IF((((CONTACTS[CY Sale Price]<1 && CONTACTS[CY Record Pushed] = "Record Pushed") 
        || CONTACTS[Member_Type__c] in {"Associate Member", "Student Member"}) && CONTACTS[Membership_Status__c] = "Current"),"Comped",
            CONTACTS[CY MEMBERSHIP_DATA_TERM.SOURCE_CODE]))


--If cy sale price > 0 -> paid
--Else 
--  If (sale price < 1 AND record pushed = 'record pushed') OR (member type is associate, student AND mem status = current) -> comped, else source_code
--Else null

*/
with RP AS (
    SELECT
        *,
        CASE 
            WHEN "Transaction Id" IS NULL THEN 'Record Pushed'
            ELSE 'Member Interaction'
        END AS RECORD_PUSHED
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM
),
SP AS (
    SELECT
        *,
        "Transaction Line Credit" - "Transaction Line Debit" AS CY_SALE_PRICE
    FROM RP
),
final AS (
    SELECT 
        SP.*,
        CASE
            WHEN SP.CY_SALE_PRICE > 0 THEN 'Paid'
            WHEN (SP.CY_SALE_PRICE < 1 AND SP.RECORD_PUSHED = 'Record Pushed') 
                OR (CON."Member_Type__c" IN ('Associate Member', 'Student Member') AND CON."Membership_Status__c" = 'Current') THEN 'Comped'
            ELSE SP.SOURCE_CODE
        END AS CY_MEMBERSHIP_STATUS
    FROM SP
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
            ON SP."SALESFORCE ID" = CON."Id"
)
SELECT * FROM FINAL WHERE CY_MEMBERSHIP_STATUS IS NULL;



SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM WHERE SOURCE_CODE IS NULL;



SELECT
    CYM.*
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM AS CYM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CYM."SALESFORCE ID" = CON."Id"
WHERE "SALESFORCE ID" IN (SELECT SALESFORCEID FROM TEST.TEST_AREA.MEMDISC217)
AND "TERM YEAR" = 2025;

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM WHERE SOURCE_CODE IS NULL AND "TERM YEAR" = 2025;


SELECT * FROM TEST.TEST_AREA.MEMDISC217;


--Sales order id for this is a1FRm000001r1F7MAI
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL WHERE SOL."OrderApi__Sales_Order__c" = 'a1FRm000001r1F7MAI';--"Name" = '0006361305';


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL WHERE 'SALESFORCE ID' = '0031I00000Wrt4XQAR';

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM WHERE 'SALESFORCE ID' = '0031I00000Wrt4XQAR';


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP WHERE "Transaction Line SO Line" = 'a1ERm000003NtDNMA0';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL WHERE TL. = 'a1FRm000001r1F7MAI';



SELECT 
    IC."Name",
    *
FROM REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    ON TL."OrderApi__Transaction__c" = TR."Id"
LEFT JOIN REPL_SALESFORCE.GL_Accounts AS GL
    ON TL."OrderApi__GL_Account__c" = GL.Id
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
    ON TL."OrderApi__Item_Class__c" = IC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
    ON TL."OrderApi__Receipt__c" = RC."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
    ON RC."OrderApi__Sales_Order__c" = SO."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
    ON TL."OrderApi__Item__c" = Itemn."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
    ON SO."OrderApi__Source_Code__c" = SC."Id"
WHERE SO."Id" = 'a1FRm000001r1F7MAI'; --transaction line id is a1QRm000001cKy2MAE

--From this person's sales order, this is the transaction line for the active membership but credit is only 175
SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP AS TLM WHERE TLM."Transaction Line ID" = 'a1QRm000001cKy2MAE';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL WHERE TL."Id" = 'a1QRm000001cKy2MAE';

--From same sales order, a1QRm000001cKy1MAE is transaction line with null item class name but 315 in debit
SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP AS TLM WHERE TLM."Transaction Line ID" = 'a1QRm000001cKy1MAE'; --doesn't exist because null item class

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL WHERE TL."Id" = 'a1QRm000001cKy1MAE';


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.DEMOS_BACKUP WHERE "SALES ORDER" = 'a1FRm000001r1F7MAI';





SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C WHERE "Id" = 'a1FRm000001r1F7MAI';

SELECT SOL."Trans_Line_Number__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL WHERE SOL."OrderApi__Sales_Order__c" = 'a1FRm000001r1F7MAI';

SELECT SOL."Trans_Line_Number__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL;


SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MH WHERE MH."SALESFORCE ID" = '0031I00000Ws6RAQAZ';


--a1FRm000001wogbMAA
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR WHERE TR."OrderApi__Sales_Order__c" = 'a1FRm000001wogbMAA';--Pulls to transaction id a1RRm000002Y6jNMAS

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL WHERE TL."OrderApi__Transaction__c" = 'a1RRm000002Y6jNMAS';

 --mem transaction year is wrong??
SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP AS MEM WHERE MEM."Transaction Id" = 'a1RRm000002Y6jNMAS'; --the sales order id is a1FRm000001wogbMAA

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.DEMOS_BACKUP AS DB WHERE DB."SALES ORDER" = 'a1FRm000001wogbMAA';

--MEMBERSHIP TRANSACTION YEAR!!!!!


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C WHERE "Id" = 'a1FRm000001wogbMAA'; --in sf the created date is 12/31 and in snowflake it's 1-1-25, wtf??
--for pete: is ownbackup doing something weird with the timezones?




SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP AS MEM WHERE MEM."MEMBERSHIP TRANSACTION YEAR" = 2026;

with Transactions AS (
SELECT 
    MEM."Transaction Id",
    MEM."Receipt ID",
    MEM."SALES ORDER ID"
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP AS MEM
WHERE MEM."SALES ORDER ID" IN (
    'a1FRm000001wmuvMAA',
    'a1FRm000001wo5VMAQ',
    'a1FRm000001woYXMAY',
    'a1FRm000001y2JJMAY',
    'a1FRm000001yBzJMAU',
    'a1FRm000001wnB3MAI',
    'a1FRm000001wnvpMAA',
    'a1FRm000001xxt7MAA',
    'a1FRm000001wp4nMAA',
    'a1FRm000001wnHVMAY',
    'a1FRm000001woNFMAY',
    'a1FRm000001wogbMAA',
    'a1FRm000001wpCrMAI',
    'a1FRm000001wpftMAA',
    'a1FRm000001xLkbMAE',
    'a1FRm0000022O3VMAU',
    'a1FRm000002IYy9MAG',
    'a1FRm000002IZFtMAO'
)
ORDER BY 3
)
SELECT * FROM Transactions JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
    ON Transactions."SALES ORDER ID" = SO."Id" ;



SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C
WHERE "Id" IN (
    'a1FRm000001wmuvMAA',
    'a1FRm000001wo5VMAQ',
    'a1FRm000001woYXMAY',
    'a1FRm000001y2JJMAY',
    'a1FRm000001yBzJMAU',
    'a1FRm000001wnB3MAI',
    'a1FRm000001wnvpMAA',
    'a1FRm000001xxt7MAA',
    'a1FRm000001wp4nMAA',
    'a1FRm000001wnHVMAY',
    'a1FRm000001woNFMAY',
    'a1FRm000001wogbMAA',
    'a1FRm000001wpCrMAI',
    'a1FRm000001wpftMAA',
    'a1FRm000001xLkbMAE',
    'a1FRm0000022O3VMAU',
    'a1FRm000002IYy9MAG',
    'a1FRm000002IZFtMAO'
);


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS RN WHERE RN."OrderApi__Sales_Order__c" = 'a1FRm000001wmuvMAA';


with Debs AS (
    SELECT
        *,
        CY."Transaction Line Credit" - CY."Transaction Line Debit" AS TOTAL_OWE
    FROM TEST.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM AS CY
)
SELECT 
    Debs.*,
    CON."Id" 
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON 
    LEFT JOIN Debs
        ON CON."Id" = Debs."SALESFORCE ID"
WHERE TOTAL_OWE > 0
AND "TERM YEAR" = YEAR(CURRENT_DATE());











SELECT DISTINCT "TERM YEAR" FROM TEST.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM ORDER BY 1 DESC;

SELECT * FROM TEST.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C WHERE YEAR("CreatedDate") >= 2024;










SELECT
    SOURCE_CODE,
    COUNT(*)
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM
WHERE "SALESFORCE ID" IN (SELECT SALESFORCEID FROM TEST.TEST_AREA.MEMDISC217)
GROUP BY 1;

with SO_BLANKS AS (
SELECT
    "SALES ORDER ID" AS SOID
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.CY_MEMBERSHIP_DATA_TERM
WHERE "SALESFORCE ID" IN (SELECT SALESFORCEID FROM TEST.TEST_AREA.MEMDISC217)
)
SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C WHERE "Id" IN (SELECT SOID FROM SO_BLANKS)
;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C WHERE "Name" LIKE '2500%' OR "Name" LIKE '2517%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE.GL_ACCOUNTS AS GLA
JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C AS GLB
ON GLA.ID = GLB."Id"
AND GLA.DESCRIPTION <> GLB."Name";


SELECT CON.Mailing FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;

SELECT
    GEOCODE("MailingStreet" || ', ' || "MailingCity" || ', ' || "MailingState" || ', ' || "MailingCountry") AS Geocode_Result
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT;

SHOW PARAMETERS LIKE 'external_access_enabled';

with Contacts AS (
    SELECT
        CON."Id", CON."MailingStreet", CON."MailingCity", CON."MailingState", CON."MailingCountry", RT."Name"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
    WHERE RT."Name" = 'Nonmember'
),
Opps AS (
    SELECT 
        OPP."npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM(OPP."Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    GROUP BY 1
    HAVING SUM(OPP."Amount") >= 500
)
SELECT * FROM Contacts JOIN Opps ON Contacts."Id" = Opps.CONTACT_ID;

with Contacts AS (
    SELECT
        CON."Id", CON."MailingStreet", CON."MailingCity", CON."MailingState", CON."MailingCountry", RT."Name"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
    WHERE RT."Name" = 'Nonmember'
),
Opps AS (
    SELECT
        OPP."npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM(OPP."Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    GROUP BY 1
    HAVING SUM(OPP."Amount") >= 500
)
SELECT * FROM Contacts JOIN Opps ON Contacts."Id" = Opps.CONTACT_ID
WHERE Contacts."MailingCountry" = 'United States';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__KNOWN_ADDRESS__C;



/*

DECLARE @McCormickPlace GEOGRAPHY = GEOGRAPHY::Point(41.8515, -87.6169, 4326);

SELECT ID, MailingStreet, MailingCity, MailingState, MailingCountry
FROM CONTACT
WHERE GEOGRAPHY::Point(Latitude, Longitude, 4326).STDistance(@McCormickPlace) <= 80467;

TEST.TEST_AREA.GEOCODED_ADDRESSES


 */



--
 SELECT CONVERTED_SNAPSHOT_DATE, COUNT("Id") FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL GROUP BY 1 ORDER BY 1;




SELECT * FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL ORDER BY RANDOM() LIMIT 100;


--CREATE OR REPLACE TABLE TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL
--AS
with DC AS (
    SELECT
        *,
        DATEADD(S, SNAPSHOT_DATE/1000000000, '1970-01-01') AS CONVERTED_SNAPSHOT_DATE
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS
),
Dupes AS (
    SELECT
        *,
        CONCAT("Id", CONVERTED_SNAPSHOT_DATE) AS ID_KEY
    FROM DC
),
RN AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY ID_KEY ORDER BY CONVERTED_SNAPSHOT_DATE) AS ID_DUPE
    FROM Dupes 
)
SELECT * FROM RN WHERE ID_DUPE = 1;


SELECT DISTINCT CONVERTED_SNAPSHOT_DATE FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL;









with KR AS (
SELECT *, CONCAT(CONTACT_ID, ITEMPRIMARY) AS UID FROM TEST.TEST_AREA.WGCHECK --kenan's report
),
MR AS (
SELECT *, CONCAT(MEMBER_ID, COMMITTEE_NAME) AS UID FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS --this has more than kenan's report
)
SELECT 
    MR.MEMBER_AACR_ID,
    KR.AACR_ID,
    MR.MEMBER_NAME,
    CONCAT(KR.FIRSTNAME, ' ', KR.LASTNAME),
    MR.COMMITTEE_NAME,
    KR.ITEMPRIMARY
FROM MR
    LEFT JOIN KR
        ON MR.MEMBER_AACR_ID = KR.AACR_ID;

SELECT *, CONCAT(CONTACT_ID, ITEMPRIMARY) AS UID FROM TEST.TEST_AREA.WGCHECK;


SELECT *, CONCAT(MEMBER_ID, COMMITTEE_NAME) AS UID FROM PRODUCTION.MART_COMMITTEES.COMMITTEE_MEMBERS;



with KR AS (
SELECT *, CONCAT(CONTACT_ID, ITEMPRIMARY) AS UID FROM TEST.TEST_AREA.WGCHECK --kenan's report
),
MR AS (
SELECT *, CONCAT(MEMBER_ID, COMMITTEE_NAME) AS UID FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES --this has more than kenan's report
)
SELECT 
    MR.MEMBER_AACR_ID,
    KR.AACR_ID,
    MR.MEMBER_NAME,
    CONCAT(KR.FIRSTNAME, ' ', KR.LASTNAME),
    MR.COMMITTEE_NAME,
    KR.ITEMPRIMARY
FROM MR
    LEFT JOIN KR
        ON MR.MEMBER_AACR_ID = KR.AACR_ID;

with MR AS (
SELECT *, CONCAT(FIRST_NAME, ' ', LAST_NAME) AS "MEMBER_NAME", CONCAT(AACR_ID, SUBSCRIPTION_NUMBER) AS UID FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES WHERE ITEM_CLASS_NAME = 'Working Groups'
),
KR AS (
    SELECT *, CONCAT(AACR_ID, SUBSCRIPTIONID) AS UID FROM TEST.TEST_AREA.WGCHECK
)
SELECT 
    MR.AACR_ID,
    KR.AACR_ID,
    MR.MEMBER_NAME,
    CONCAT(KR.FIRSTNAME, ' ', KR.LASTNAME),
    MR.ITEM_NAME,
    KR.ITEMPRIMARY,
    MR.SUBSCRIPTION_NUMBER,
    KR.SUBSCRIPTIONID
FROM MR
    LEFT JOIN KR
        ON MR.SUBSCRIPTION_NUMBER = KR.SUBSCRIPTIONID
WHERE MR.MEMBER_NAME = 'Ying Han' OR KR.AACR_ID = '1211903';


SELECT *, CONCAT(AACR_ID, SUBSCRIPTIONID) AS UID FROM TEST.TEST_AREA.WGCHECK WHERE AACR_ID = '1211903';

SELECT *, CONCAT(AACR_ID, SUBSCRIPTION_NUMBER) AS UID FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES;


SELECT *, CONCAT(CONTACT_ID, ITEMPRIMARY) AS UID FROM TEST.TEST_AREA.WGCHECK;

create or replace view PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES
as
SELECT
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    CON."Id" AS CONTACT_ID,
    CON."FirstName" AS FIRST_NAME,
    CON."LastName" AS LAST_NAME,
    ACC."Name" AS ACCOUNT_NAME,
    CON."RecordTypeId" AS RECORD_TYPE_ID,
    RT."Name" AS RECORD_TYPE_NAME,
    CON."Member_Type__c" AS MEMBER_TYPE,
    CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
    CON."Paid_thru_date__c" AS PAID_THRU_DATE,
    CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE, 
    CON."AccountId" AS ACCOUNT_ID,
    SUB."Id" AS SUBSCRIPTION_ID,
    SUB."Name" AS SUBSCRITON_NUMBER,
    SUB."OrderApi__Current_Term_Start_Date__c" AS TERM_START_DATE,
    SUB."OrderApi__Current_Term_End_Date__c" AS TERM_END_DATE,
    SUB."OrderApi__Status__c" AS STATUS,
    SUB."OrderApi__Is_Active__c" AS IS_ACTIVE,
    SUB."OrderApi__Is_Expired__c" AS IS_EXPIRED,
    SUB."OrderApi__Item__c" AS ITEM,
    IT."Name" AS ITEM_NAME,
    SUB."OrderApi__Item_Class__c" AS ITEM_CLASS,
    ITC."Name" AS ITEM_CLASS_NAME,
    SUB."CreatedById" AS CREATED_BY,
    SUB."CreatedDate" AS CREATED_DATE,
    CON."Income_Level__c" AS WB_INCOME_LEVEL,
    CON."MailingCountry" AS MAILING_COUNTRY,
    CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
    CON."Gender__c" AS GENDER
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON SUB."OrderApi__Contact__c" = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
        ON SUB."OrderApi__Item__c" = IT."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
        ON SUB."OrderApi__Item_Class__c" = ITC."Id"
WHERE ITC."Name" = 'Working Groups';


SELECT "Member_Role__c", COUNT(*) FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C GROUP BY 1;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS A WHERE A."Committee_Member__c" = '0031I00000ShzglQAB';


with COMMS AS (
    SELECT
        CM.*,
        COMM."Name" AS COMMITTEE_NAME
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS CM
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS COMM
            ON CM."Committee_Name__c" = COMM."Id"
)
SELECT 
    A.ITEM_NAME,
    B.COMMITTEE_NAME,
    B."Member_Role__c",
    A.*
FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES AS A
    LEFT JOIN COMMS AS B
        ON A.CONTACT_ID = B."Committee_Member__c"
            AND A.ITEM_NAME = RIGHT(COMMITTEE_NAME, LENGTH(COMMITTEE_NAME)-5)--A.ITEM_NAME = B.COMMITTEE_NAME
                AND A.TERM_START_DATE = B."Start_Date__c"
ORDER BY A.CONTACT_ID;



SELECT
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS CM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS COMM
        ON CM."Committee_Name__c" = COMM."Id";SELECT AACR_ID, COUNT(*) FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES;


SELECT B."Name", A.* FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE_MEMBER__C AS A 
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS B
        ON A."Committee_Name__c" = B."Id"
WHERE A."Member_Role__c" <> 'Member' AND A."End_Date__c" IS NOT NULL ORDER BY A."End_Date__c" DESC;

/*
--DA interviews
--GENIE
--Membership troubleshooting
--Google Maps API/Geocoding

1220917

 */





SELECT COUNT(DISTINCT AACR_ID) FROM PRODUCTION.MART_COMMITTEES.ACTIVE_SUBSCRIPTION_COMMITTEE_MEMBERS;




WHERE STATUS = 'Active'
GROUP BY 1
ORDER BY 2 DESC
--WHERE AACR_ID = '1398522'
;

SELECT AACR_ID, COUNT(*), COUNT(DISTINCT ITEM_NAME) FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES
WHERE STATUS = 'Active'
GROUP BY 1
HAVING COUNT(*) <> COUNT(DISTINCT ITEM_NAME)
ORDER BY 2 DESC
--WHERE AACR_ID = '1398522'
;




SELECT * FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES
WHERE STATUS = 'Active'
AND AACR_ID = '1398522';

/*
Problems (multiple subscriptions for each committee, AACR IDs):

SELECT AACR_ID, COUNT(*), COUNT(DISTINCT ITEM_NAME) FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES
WHERE STATUS = 'Active'
GROUP BY 1
HAVING COUNT(*) <> COUNT(DISTINCT ITEM_NAME)
ORDER BY 2 DESC

 */

SELECT DISTINCT ITEM_NAME FROM PRODUCTION.MART_COMMITTEES.SUBSCRIPTION_COMMITTEES;

/*
CASE
    WHEN ITEM_NAME = 'Cancer Immunology (CIMM)' THEN 'Cancer Immunology Working Group'
    WHEN ITEM_NAME = 'Pathology in Cancer Research Working Group (PICR)' THEN 'Pathology in Cancer Research Working Group'
    WHEN ITEM_NAME = 'Minorities in Cancer Research (MICR)' THEN 'Minorities in Cancer Research'
    WHEN ITEM_NAME = 'Population Sciences (PSWG)' THEN 'Population Sciences Working Group'
    WHEN ITEM_NAME = 'Cancer Evolution (CEWG)' THEN 'Cancer Evolution Working Group'
    WHEN ITEM_NAME = 'Hematologic Malignancies Working Group (HMWG)' THEN 'Hematologic Malignancies Working Group'
    WHEN ITEM_NAME = 'Chemistry in Cancer Research (CICR)' THEN Chemistry in Cancer Research
    WHEN ITEM_NAME = 'Tumor Microenvironment (TME)' THEN Tumor Microenvironment
    WHEN ITEM_NAME = 'Pediatric Cancer (PCWG)' THEN Pediatric Cancer
    WHEN ITEM_NAME = 'Radiation Science and Medicine (RSM)' THEN Radiation Science and Medicine
    WHEN ITEM_NAME = 'Cancer Prevention (CPWG)' THEN Cancer Prevention
    WHEN ITEM_NAME = 'Women in Cancer Research (WICR)' THEN Women in Cancer Research
    WHEN ITEM_NAME = 'Behavioral Science in Cancer Research (BSCR)' THEN Behavioral Science in Cancer Research




 */


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.COMMITTEE__C AS COMM WHERE COMM."Name" LIKE '%Minorities%';



SELECT * FROM TEST.TEST_AREA.WGCHECK WHERE ITEMPRIMARY LIKE '%Tumor Micro%' AND ISACTIVE = true;


SELECT * FROM PRODUCTION.MART_COMMITTEES.ACTIVE_SUBSCRIPTION_COMMITTEE_MEMBERS 
WHERE ITEM_NAME LIKE '%Tumor Micro%' 
    AND MEMBERSHIP_STATUS IN ('Current', 'Suspended') 
    AND AACR_ID NOT IN (SELECT AACR_ID FROM TEST.TEST_AREA.WGCHECK WHERE ITEMPRIMARY LIKE '%Tumor Micro%' AND ISACTIVE = true);

--1421015
SELECT * FROM PRODUCTION.MART_COMMITTEES.ACTIVE_SUBSCRIPTION_COMMITTEE_MEMBERS WHERE AACR_ID = '1421015';


SELECT 
    CON."Id",
    CON."Name",
    CM.CONTACT_ID,
    CM.FIRST_NAME,
    CM.LAST_NAME
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.MART_COMMITTEES.ACTIVE_SUBSCRIPTION_COMMITTEE_MEMBERS AS CM
        ON CON."Id" = CM.CONTACT_ID
WHERE CON."Id" = '003Rm00000ZYHLgIAP';





with Contacts AS (
    SELECT
        CON."Id", CON."MailingStreet", CON."MailingCity", CON."MailingState", CON."MailingCountry", RT."Name"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
            ON CON."RecordTypeId" = RT."Id"
    WHERE RT."Name" = 'Nonmember'
),
Opps AS (
    SELECT
        OPP."npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        SUM(OPP."Amount") AS TOTAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    GROUP BY 1
    HAVING SUM(OPP."Amount") >= 500
)
SELECT * FROM Contacts JOIN Opps ON Contacts."Id" = Opps.CONTACT_ID
WHERE Contacts."MailingCountry" = 'United States';





SELECT
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    CON."FirstName",
    CON."LastName",
    CON."Email",
    CON."Phone",
    CON."Id" AS CONTACT_ID,
    CON."MailingStreet",
    CON."MailingCity",
    CON."MailingState",
    CON."MailingCountry",
    CON."MailingPostalCode",
    CON."Member_Type__c" AS MEMBER_TYPE, 
    CON."Membership_Status__c" AS MEMBERHSIP_STATUS,
    OPP."Amount" AS DONATION_AMOUNT,
    OPP."CloseDate" AS CLOSE_DATE,
    OPP."Id" AS OPPORTUNITY_ID
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON OPP."npe01__Contact_Id_for_Role__c" = CON."Id"
WHERE YEAR(OPP."CloseDate") >= 2023 AND OPP."Amount" >= 100 AND CON."MailingCountry" = 'United States';




/*
DECLARE @McCormickPlace GEOGRAPHY = GEOGRAPHY::Point(41.8515, -87.6169, 4326);

SELECT ID, MailingStreet, MailingCity, MailingState, MailingCountry
FROM CONTACT
WHERE GEOGRAPHY::Point(Latitude, Longitude, 4326).STDistance(@McCormickPlace) <= 80467;

TEST.TEST_AREA.GEOCODED_ADDRESSES
 */

WITH Constants AS (
    SELECT 
        41.8511 AS ref_lat,   -- McCormick Place Latitude
        -87.6167 AS ref_lon,  -- McCormick Place Longitude
        3958.8 AS earth_radius -- Radius of Earth in miles
)
SELECT 
    ga.*,  -- Select all columns from the geocoded table
    (constants.earth_radius * 
        ACOS(
            COS(RADIANS(constants.ref_lat)) * COS(RADIANS(ga.latitude)) * 
            COS(RADIANS(ga.longitude) - RADIANS(constants.ref_lon)) + 
            SIN(RADIANS(constants.ref_lat)) * SIN(RADIANS(ga.latitude))
        )
    ) AS distance_miles
FROM TEST.TEST_AREA.GEOCODED_ADDRESSES_NEW ga
CROSS JOIN Constants
WHERE 
    (constants.earth_radius * 
        ACOS(
            COS(RADIANS(constants.ref_lat)) * COS(RADIANS(ga.latitude)) * 
            COS(RADIANS(ga.longitude) - RADIANS(constants.ref_lon)) + 
            SIN(RADIANS(constants.ref_lat)) * SIN(RADIANS(ga.latitude))
        )
    ) <= 26
ORDER BY CONTACT_ID, CLOSE_DATE;



with Recent AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        "CloseDate" DATE_OF_MOST_RECENT_DONATION,
        ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
        "Amount" AS MOST_RECENT_DONATION_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
),
MostRecent AS (
    SELECT * FROM Recent WHERE ROW_ORDER = 1
),
Constants AS (
    SELECT 
        41.8511 AS ref_lat,   -- McCormick Place Latitude
        -87.6167 AS ref_lon,  -- McCormick Place Longitude
        3958.8 AS earth_radius -- Radius of Earth in miles
),
CloseCons AS (
    SELECT 
        ga.*,  -- Select all columns from the geocoded table
        (constants.earth_radius * 
            ACOS(
                COS(RADIANS(constants.ref_lat)) * COS(RADIANS(ga.latitude)) * 
                COS(RADIANS(ga.longitude) - RADIANS(constants.ref_lon)) + 
                SIN(RADIANS(constants.ref_lat)) * SIN(RADIANS(ga.latitude))
            )
        ) AS distance_miles
    FROM TEST.TEST_AREA.GEOCODED_ADDRESSES_NEW ga
    CROSS JOIN Constants
    WHERE 
        (constants.earth_radius * 
            ACOS(
                COS(RADIANS(constants.ref_lat)) * COS(RADIANS(ga.latitude)) * 
                COS(RADIANS(ga.longitude) - RADIANS(constants.ref_lon)) + 
                SIN(RADIANS(constants.ref_lat)) * SIN(RADIANS(ga.latitude))
            )
        ) <= 26
    ORDER BY DISTANCE_MILES
)
SELECT * FROM CloseCons LEFT JOIN MostRecent ON CloseCons.CONTACT_ID = MostRecent.CONTACT_ID;




SELECT * FROM PRODUCTION.MART_CTI_MATCHES.INVITED_SPEAKERS__V ORDER BY MEETING_YEAR DESC;

with ConEmails AS (
SELECT
    CONCAT(CON."Email", CON."Journal_Email__c", CON."OrderApi__Work_Email__c", CON."OrderApi__Other_Email__c", CON."OrderApi__Personal_Email__c", CON."npe01__Preferred_Email__c") AS EMAILSTRING
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
)
SELECT * FROM ConEmails WHERE EMAILSTRING IS NOT NULL;


SELECT CON."Email"::VARCHAR, CON."Journal_Email__c", CON."OrderApi__Work_Email__c", CON."OrderApi__Other_Email__c", CON."OrderApi__Personal_Email__c", CON."npe01__Preferred_Email__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;

SELECT * FROM TEST.CTI_2025_EXTRACTS.INVITED_SPEAKERS_CHAIRS;


--SELECT * FROM table WHERE 123 IN(col1, col2, col3, col4); test value: patricia.lorusso@yale.edu

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE 'patricia.lorusso@yale.edu' IN (CON."Email", CON."Journal_Email__c", CON."OrderApi__Work_Email__c", CON."OrderApi__Other_Email__c", CON."OrderApi__Personal_Email__c", CON."npe01__Preferred_Email__c")

;


/*
SELECT 
    s.*, 
    c.*
FROM SPEAKERS s
LEFT JOIN CONTACT c
    ON s.email = c.personal_email
    OR s.email = c.work_email
    OR s.email = c.other_email;

 */


SELECT DISTINCT CONVERTED_SNAPSHOT_DATE FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL ORDER BY 1;


--How many people have duplicate subscriptions for membership?
SELECT 
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C

;


CREATE OR REPLACE VIEW TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOTS
AS 
SELECT 
    CS."Id",
	CS."IsDeleted",
	CS."MasterRecordId",
	CS."AccountId",
	CS."LastName",
	CS."FirstName",
	CS."Salutation",
	CS."MiddleName",
	CS."Name",
	CS."RecordTypeId",
	CS."Title",
	CS."Birthdate",
	CS."CreatedDate",
	CS."LastModifiedDate",
	CS."LastModifiedById",
	CS."LastActivityDate",
	CS."OrderApi__Badges__c",
	CS."DonorApi__Deceased__c",
	CS."iMIS_Created_Date__c",
	CS."Application_Status__c",
	CS."Degrees_Held__c",
	CS."Formerly_Known_As__c",
	CS."Gender__c",
	CS."Income_Level__c",
	CS."Initial_Join_Date__c",
	CS."Maiden_Name__c",
	CS."Member_Type__c",
	CS."Membership_Id__c",
	CS."Membership_Information__c",
	CS."Membership_Status__c",
	CS."Paid_thru_date__c",
	CS."Race__c",
	CS."Salesforce_ID__c",
	CS."iMIS_ID__c",
	CS."Prior_Member_Status__c",
	CS."Highest_Degree__c",
	CS."Date_of_Death__c",
	CS."Pre_Post_Doc__c",
	CS."Event_Segment__c",
	CS."Manual_Segment_Override__c",
	CS."Override_By__c",
	CS."Override_Date__c",
	CS."Override_Reason__c",
	CS.CONVERTED_SNAPSHOT_DATE,
	CS.ID_KEY,
	CS.ID_DUPE,
    RT."Name" AS RECORD_TYPE_NAME,
    ACC."Name" AS ACCOUNT_NAME
FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL AS CS
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CS."RecordTypeId" = RT."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CS."AccountId" = ACC."Id";


SELECT * FROM TEST.CTI_2025_EXTRACTS.INVITED_SPEAKERS_CHAIRS WHERE SESSION_TYPE IS NULL;

SELECT COUNT(*) FROM TEST.CTI_2025_EXTRACTS.INVITED_SPEAKERS_CHAIRS;

SELECT * FROM TEST.CTI_2025_EXTRACTS.INVITED_SPEAKERS_CHAIRS WHERE PRIMARYAUTHOR_FIRSTINSTITUTIONNAME IS NULL;

--filter out session type and number being null, also filter out exhibitor spotlight, CME specification? 
--Called CME designation at session level, add explanation of counts whether they're distinct, where data comes from





--Join contact snapshot to mem data mart, need to make sure time lines up between snapshot date and transaction date

/*
SELECT cs.*, m.*
FROM CONTACT_SNAPSHOTS cs
JOIN MEMBERSHIP m 
    ON cs.CONTACT_ID = m.CONTACT_ID
    AND YEAR(cs.SNAPSHOT_DATE) = YEAR(m.TRANSACTION_DATE)
    AND MONTH(cs.SNAPSHOT_DATE) = MONTH(m.TRANSACTION_DATE);
 */

CREATE OR REPLACE VIEW TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE
AS
SELECT
    CS.*,
    MEM.*
FROM TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM AS CS
    LEFT JOIN PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MEM
        ON CS."Id" = MEM."SALESFORCE ID"
        AND YEAR(CS.CONVERTED_SNAPSHOT_DATE) = YEAR(MEM."Transaction Date")
        AND MONTH(CS.CONVERTED_SNAPSHOT_DATE) = MONTH(MEM."Transaction Date")
        AND ;

--trans date 7/27; snapshot date happened before, could happen 1st or 2nd of month or be before
/*Final*/

CREATE OR REPLACE VIEW TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE
AS
WITH AdjustedMembership AS (
    SELECT 
        m.*,
        -- Adjust month forward if transaction is after the snapshot date in the same month
        CASE 
            WHEN m."Transaction Date" > cs.CONVERTED_SNAPSHOT_DATE
            THEN DATEADD(MONTH, 1, m."Transaction Date")
            ELSE m."Transaction Date"
        END AS ADJUSTED_DATE
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL m
        LEFT JOIN TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM cs 
            ON m."SALESFORCE ID" = cs."Id"
            AND DATE_TRUNC('MONTH', m."Transaction Date") = DATE_TRUNC('MONTH', cs.CONVERTED_SNAPSHOT_DATE)
)
SELECT cs.*, am.*
FROM TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM AS cs
    LEFT JOIN AdjustedMembership am 
        ON cs."Id" = am."SALESFORCE ID"
        AND DATE_TRUNC('MONTH', cs.CONVERTED_SNAPSHOT_DATE) = DATE_TRUNC('MONTH', am.ADJUSTED_DATE);


SELECT * FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE WHERE "Id" = '0031I000017TITFQA4';

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL WHERE "SALESFORCE ID" = '0031I000017TITFQA4'; --transaction dates are null?

SELECT 
    COUNT(*),
    SUM(CASE WHEN "SALES ORDER" IS NULL THEN 1 ELSE 0 END) SO_NULL_COUNT,
    SUM(CASE WHEN "TERM CREATED DATE" IS NULL THEN 1 ELSE 0 END) TERM_CREATED_DATE_NULL_COUNT,
    SUM(CASE WHEN "Transaction Date" IS NULL THEN 1 ELSE 0 END) TRANS_DATE_NULL_COUNT
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL
WHERE "ITEM NAME" = 'Active Membership';





















SELECT 
    MEM."CONTACT NAME",
    MEM."SALESFORCE ID",
    COUNT(MEM."TERM ID")
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MEM
GROUP BY 2, 1
;

--0031I00000Wrud1QAB

SELECT 
    *
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MEM
WHERE MEM."SALESFORCE ID" = '0031I00000Wrud1QAB';


select table_schema, table_name, column_name
from snowflake.account_usage.columns
where column_name LIKE '%Gender%';


SELECT C., "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS AS C
WHERE COLUMN_NAME LIKE '%Gender%'
ORDER BY 1, 2;


SELECT DISTINCT "MailingCountry" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT ORDER BY 1 DESC;


--application status, whaty if approved or pending and moved to approved
--people who are asking for self-generating reports; 

SELECT DISTINCT CON."Application_Status__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Application_Status__c" = 'Deferred';

with Lag_Status AS (
    SELECT 
        CS."Id",
        CS.SNAPSHOT_DATE,
        LAG(CS."Application_Status__c") OVER(PARTITION BY CS."Id" ORDER BY SNAPSHOT_DATE) AS STARTING_STATUS,
        CS."Application_Status__c" AS APPLICATION_STATUS,
        LEAD(CS."Application_Status__c") OVER(PARTITION BY CS."Id" ORDER BY SNAPSHOT_DATE) AS NEXT_STATUS,
        LEAD(CS."Application_Status__c", 2) OVER(PARTITION BY CS."Id" ORDER BY SNAPSHOT_DATE) AS NEXT_STATUS
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL AS CS
)
SELECT
    *
FROM Lag_Status
WHERE STARTING_STATUS = 'Deferred'
AND APPLICATION_STATUS <> 'Deferred';

with Deferred_Start AS (
    SELECT 
        CS."Id",
        CS."Application_Status__c" AS APPLICATION_STATUS,
        ROW_NUMBER() OVER(PARTITION BY CS."Id" ORDER BY SNAPSHOT_DATE)
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL AS CS
);



/*
WITH FirstDeferred AS (
    SELECT 
        CONTACT_ID, 
        MIN(SNAPSHOT_DATE) AS First_Deferred_Date
    FROM your_table
    WHERE Application_Status = 'Deferred'
    GROUP BY CONTACT_ID
),
AcceptedLater AS (
    SELECT DISTINCT 
        t.CONTACT_ID
    FROM your_table t
    JOIN FirstDeferred fd 
        ON t.CONTACT_ID = fd.CONTACT_ID 
        AND t.SNAPSHOT_DATE > fd.First_Deferred_Date
    WHERE t.Application_Status = 'Accepted'
)
SELECT DISTINCT fd.CONTACT_ID
FROM FirstDeferred fd
JOIN AcceptedLater al 
    ON fd.CONTACT_ID = al.CONTACT_ID;
 */


with FirstDeferred AS (
    SELECT 
        CS."Id" AS CONTACT_ID,
        MIN(SNAPSHOT_DATE) AS FIRST_DEFERRED_DATE
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL AS CS
    WHERE CS."Application_Status__c" = 'Deferred'
    GROUP BY 1
 ),
AcceptedLater AS (
    SELECT 
        DISTINCT CS2."Id" AS CONTACT_ID2
    FROM TEST.CONTACT_SNAPSHOTS.COMBINED_CONTACT_SNAPSHOTS_FINAL AS CS2
        JOIN FirstDeferred AS FD
            ON FD.CONTACT_ID = CS2."Id"
            AND CS2.SNAPSHOT_DATE > FD.FIRST_DEFERRED_DATE
    WHERE CS2."Application_Status__c" = 'Accepted'
)
SELECT
    DISTINCT FD.CONTACT_ID
FROM FirstDeferred AS FD
    JOIN AcceptedLater AS AL
        ON FD.CONTACT_ID = AL.CONTACT_ID2;

SELECT * FROM TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM ORDER BY RANDOM() LIMIT 100;

/*
Id
IsDeleted
MasterRecordId
AccountId
LastName
FirstName
Salutation
MiddleName
Name
RecordTypeId
Title
Birthdate
CreatedDate
LastModifiedDate
LastModifiedById
LastActivityDate
OrderApi__Badges__c
DonorApi__Deceased__c
iMIS_Created_Date__c
Application_Status__c
Degrees_Held__c
Formerly_Known_As__c
Gender__c
Income_Level__c
Initial_Join_Date__c
Maiden_Name__c
Member_Type__c
Membership_Id__c
Membership_Information__c
Membership_Status__c
Paid_thru_date__c
Race__c
Salesforce_ID__c
iMIS_ID__c
Prior_Member_Status__c
Highest_Degree__c
Date_of_Death__c
Pre_Post_Doc__c
Event_Segment__c
Manual_Segment_Override__c
Override_By__c
Override_Date__c
Override_Reason__c
CONVERTED_SNAPSHOT_DATE
ID_KEY
ID_DUPE
RECORD_TYPE_NAME
ACCOUNT_NAME
Final_Membership_Status


status_score = SWITCH
	(TRUE(), 
    CONTACTS[Membership_Status__c] = "Current", 100,
	CONTACTS[Membership_Status__c] = "Suspended", 75,
    CONTACTS[Membership_Status__c] = "Suspended (Delinquent)", 50,
	0)

login_score = SWITCH
	(TRUE(), 
	ISBLANK(CONTACTS[LastActivityDate]), 0,
	DATEDIFF(CONTACTS[LastActivityDate], TODAY(), DAY) <= 365 && DATEDIFF(CONTACTS[LastActivityDate], TODAY(), DAY) > 0, 100,
	DATEDIFF(CONTACTS[LastActivityDate], TODAY(), DAY) > 365 && DATEDIFF(CONTACTS[LastActivityDate], TODAY(), DAY) <= 730, 75,
    50)

opportunity_score = SWITCH
	(TRUE(), 
	ISBLANK(CONTACTS[DATE_OF_MOST_RECENT_DONATION]), 0,
	DATEDIFF(CONTACTS[DATE_OF_MOST_RECENT_DONATION], TODAY(), DAY) <= 365 && DATEDIFF(CONTACTS[DATE_OF_MOST_RECENT_DONATION], TODAY(), DAY) > 0, 100,
	DATEDIFF(CONTACTS[DATE_OF_MOST_RECENT_DONATION], TODAY(), DAY) > 365 && DATEDIFF(CONTACTS[DATE_OF_MOST_RECENT_DONATION], TODAY(), DAY) <= 730, 75,
    50)

email_score = SWITCH
	(TRUE(), 
	ISBLANK(CONTACTS[SENDGRID_EMAILS.MOST_RECENT_CLICK_TIME]), 0,
	DATEDIFF(CONTACTS[SENDGRID_EMAILS.MOST_RECENT_CLICK_TIME], TODAY(), DAY) <= 365 && DATEDIFF(CONTACTS[SENDGRID_EMAILS.MOST_RECENT_CLICK_TIME], TODAY(), DAY) > 0, 100,
	DATEDIFF(CONTACTS[SENDGRID_EMAILS.MOST_RECENT_CLICK_TIME], TODAY(), DAY) > 365 && DATEDIFF(CONTACTS[SENDGRID_EMAILS.MOST_RECENT_CLICK_TIME], TODAY(), DAY) <= 730, 75,
    50)



engagement_score = (CONTACTS[status_score] + CONTACTS[login_score] + CONTACTS[opportunity_score] + CONTACTS[email_score])/4



 */






GRANT USAGE ON TEST.MART_SALESFORCE_OBJECTS.CONTACT_OPPORTUNITIES_MERGE;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY;

SELECT 
    YEAR("CloseDate"),
    SUM("Amount")
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
GROUP BY 1
ORDER BY 1;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE CON."wf_net_worth__c" IS NOT NULL;





--CREATE OR REPLACE VIEW TEST.MART_SALESFORCE_OBJECTS.CONTACT_OPPORTUNITIES_MERGE
--AS
with RecentGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
GROUP BY 1
),
LastYearGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID2,
        SUM("Amount") AS TOTAL_AMOUNT_LAST_YEAR,
        COUNT("Id") AS TOTAL_OPPORTUNITIES_LAST_YEAR
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
WHERE YEAR("CloseDate") = YEAR(CURRENT_DATE()) - 1
GROUP BY 1
),
Recent AS (
SELECT
    "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
    "CloseDate" DATE_OF_MOST_RECENT_DONATION,
    ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
    "Amount" AS MOST_RECENT_DONATION_AMOUNT
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY),
OPP AS (
    SELECT 
        Recent.CONTACT_ID,
        Recent.MOST_RECENT_DONATION_AMOUNT AS MOST_RECENT_DONATION_AMOUNT,
        Recent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
        RecentGift.TOTAL_AMOUNT AS TOTAL_DONATION_AMOUNT,
        RecentGift.TOTAL_OPPORTUNITIES AS TOTAL_OPPORTUNITIES,
        LastYearGift.TOTAL_AMOUNT_LAST_YEAR,
        LastYearGift.TOTAL_OPPORTUNITIES_LAST_YEAR
    FROM RecentGift 
        JOIN Recent 
            ON RecentGift.CONTACT_ID1 = Recent.CONTACT_ID
        JOIN LastYearGift
            ON Recent.CONTACT_ID = LastYearGift.CONTACT_ID2
        
    WHERE Recent.ROW_ORDER = 1
)
SELECT
    CON.*,
    OPP.MOST_RECENT_DONATION_AMOUNT,
    OPP.DATE_OF_MOST_RECENT_DONATION,
    OPP.TOTAL_AMOUNT_LAST_YEAR,
    OPP.TOTAL_OPPORTUNITIES_LAST_YEAR,
    OPP.TOTAL_DONATION_AMOUNT,
    OPP.TOTAL_OPPORTUNITIES
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN OPP
       ON OPP.CONTACT_ID = CON."Id";
--WHERE CON."wf_net_worth__c" IS NOT NULL;

/*
To add:

Total donated (past year)

Donation frequency

Recency (days since last gift)

Time since first gift

Average donation amount


 */


    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES,
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
WHERE YEAR("CloseDate") = YEAR(CURRENT_DATE()) - 1
GROUP BY 1
;



with RecentMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID1,
        SUM(ORDER_TOTAL) AS TOTAL_AMOUNT,
        COUNT(PARTICIPATION_ID) AS TOTAL_EVENTS
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE PARTICIPANT_STATUS = 'Yes'
GROUP BY 1
),
LastYearMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID2,
        SUM(ORDER_TOTAL) AS TOTAL_AMOUNT_LAST_YEAR,
        COUNT(PARTICIPATION_ID) AS TOTAL_MEETING_LAST_YEAR
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE YEAR(REGISTRATION_DATE) = YEAR(CURRENT_DATE()) - 1
    AND PARTICIPANT_STATUS = 'Yes'
GROUP BY 1
),
Recent AS (
SELECT
    CONTACT_ID AS CONTACT_ID,
    MAX(REGISTRATION_DATE) AS MOST_RECENT_EVENT
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
WHERE PARTICIPANT_STATUS = 'Yes'
GROUP BY 1),
OPP AS (
    SELECT 
        Recent.CONTACT_ID,
        Recent.MOST_RECENT_EVENT AS DATE_OF_MOST_RECENT_EVENT,
        RecentMeeting.TOTAL_AMOUNT AS TOTAL_MEETING_PAID_AMOUNT,
        RecentMeeting.TOTAL_EVENTS AS TOTAL_NUMBER_OF_EVENTS_ATTENDED,
        LastYearMeeting.TOTAL_AMOUNT_LAST_YEAR AS TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR,
        LastYearMeeting.TOTAL_MEETING_LAST_YEAR AS TOTAL_EVENTS_LAST_YEAR
    FROM RecentMeeting 
        JOIN Recent 
            ON RecentMeeting.CONTACT_ID1 = Recent.CONTACT_ID
        JOIN LastYearMeeting
            ON Recent.CONTACT_ID = LastYearMeeting.CONTACT_ID2
)
SELECT
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN OPP
       ON OPP.CONTACT_ID = CON."Id";



SELECT
    CONTACT_ID AS CONTACT_ID,
    MAX(REGISTRATION_DATE) AS MOST_RECENT_EVENT
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
GROUP BY 1;

SELECT * FROM IMISDATA.DBO.ACTIVITY AS ACT WHERE ACT.ID = '2184';


SELECT * FROM IMISDATA.DBO.ACTIVITY WHERE "ID" = '1000000';


SELECT
    COUNT(*)
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
WHERE ACC."Name" LIKE '%AACR Test Account%';




/*
* Research Applications
* Dataset Integration
* Novel Insights
* Emerging Trends
* Frequently Used Features
* Systematic Biases
* Data Quality and Preprocessing
* Validation and Replicability
* Analytical Methods
* Comparative Analysis
* Integration with Technologies
* Clinical Impact
* Dataset Evolution
* Future Directions
* Dataset Improvement

 */


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C WHERE "BR_Event__c" = 'a4jRm0000012E9eIAE';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Id" = 'a4jRm0000012E9eIAE';

/*
PBI Defaults:

Memory Limit (KB): 1,048,576
Timeout (s): 225


 */

SELECT 
    CON."Name",
    CON."Id",
    CON."wf_net_worth__c",
    CON.TOTAL_OPPORTUNITIES,
    CON.TOTAL_DONATION_AMOUNT,
    CON.DATE_OF_MOST_RECENT_DONATION
FROM TEST.TEST_AREA.CONTACT_ACTIVITY AS CON 
WHERE CON."wf_net_worth__c" IS NOT NULL
ORDER BY 3;

with Buckets AS (
    SELECT
        *,
        CASE 
            WHEN CON."wf_net_worth__c" < 10000000 THEN '1 - Standard'
            WHEN CON."wf_net_worth__c" < 100000000 THEN '2 - High'
            WHEN CON."wf_net_worth__c" < 1000000000 THEN '3 - Very High'
            WHEN CON."wf_net_worth__c" >= 1000000000 THEN '4 - Ultra High'
        END AS "NET_WORTH_CATEGORY"
    FROM TEST.TEST_AREA.CONTACT_ACTIVITY AS CON
    WHERE "wf_net_worth__c" IS NOT NULL
)
SELECT 
    "NET_WORTH_CATEGORY",
    SUM(TOTAL_DONATION_AMOUNT),
    AVG(TOTAL_DONATION_AMOUNT)
FROM Buckets
GROUP BY 1
ORDER BY 1;


SELECT
    OPP."npe01__Contact_Id_for_Role__c",
    SUM("Amount")
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
WHERE OPP."npe01__Contact_Id_for_Role__c" IN (SELECT CON."Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE CON."wf_net_worth__c" IS NOT NULL)
GROUP BY 1
ORDER BY 2 DESC;

SELECT * FROM TEST.TEST_AREA.CONTACT_ACTIVITY WHERE "Id" = '0031I00000WsO7aQAF';

--1,180,000
--65,922,586.18

--1,564,164.89 among people with non-null NW

SELECT
    CA."Name",
    CA.AACR_ID,
    TOTAL_DONATION_AMOUNT
FROM TEST.TEST_AREA.CONTACT_ACTIVITY AS CA
WHERE CA."wf_net_worth__c" IS NOT NULL
    AND TOTAL_DONATION_AMOUNT > 5000;




/**********************************

FIXED CODE BELOW



 **************************************/


with RecentGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    GROUP BY 1
),
LastYearGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID2,
        SUM("Amount") AS TOTAL_AMOUNT_LAST_YEAR,
        COUNT("Id") AS TOTAL_OPPORTUNITIES_LAST_YEAR
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    WHERE YEAR("CloseDate") = YEAR(CURRENT_DATE()) - 1
    GROUP BY 1
),
Recent AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        "CloseDate" DATE_OF_MOST_RECENT_DONATION,
        ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
        "Amount" AS MOST_RECENT_DONATION_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
),
MostRecent AS (
    SELECT * FROM Recent WHERE ROW_ORDER = 1
),
RecentMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID1,
        COALESCE(SUM(ORDER_TOTAL), 0) AS TOTAL_AMOUNT,
        COALESCE(COUNT(PARTICIPATION_ID), 0) AS TOTAL_EVENTS
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
),
LastYearMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID2,
        COALESCE(SUM(ORDER_TOTAL), 0) AS TOTAL_AMOUNT_LAST_YEAR,
        COALESCE(COUNT(PARTICIPATION_ID), 0) AS TOTAL_MEETING_LAST_YEAR
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE YEAR(REGISTRATION_DATE) = YEAR(CURRENT_DATE()) - 1
        AND PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
),
Recent1 AS (
    SELECT
        CONTACT_ID AS CONTACT_ID,
        MAX(REGISTRATION_DATE) AS MOST_RECENT_EVENT
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
)
SELECT 
    CON.*,
    COALESCE(MostRecent.MOST_RECENT_DONATION_AMOUNT, 0) AS MOST_RECENT_DONATION_AMOUNT,
    MostRecent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
    COALESCE(RecentGift.TOTAL_AMOUNT, 0) AS TOTAL_DONATION_AMOUNT,
    COALESCE(RecentGift.TOTAL_OPPORTUNITIES, 0) AS TOTAL_OPPORTUNITIES,
    COALESCE(LastYearGift.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_AMOUNT_LAST_YEAR,
    COALESCE(LastYearGift.TOTAL_OPPORTUNITIES_LAST_YEAR, 0) AS TOTAL_OPPORTUNITIES_LAST_YEAR,
    Recent1.MOST_RECENT_EVENT AS DATE_OF_MOST_RECENT_EVENT,
    COALESCE(RecentMeeting.TOTAL_AMOUNT, 0) AS TOTAL_MEETING_PAID_AMOUNT,
    COALESCE(RecentMeeting.TOTAL_EVENTS, 0) AS TOTAL_NUMBER_OF_EVENTS_ATTENDED,
    COALESCE(LastYearMeeting.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR,
    COALESCE(LastYearMeeting.TOTAL_MEETING_LAST_YEAR, 0) AS TOTAL_EVENTS_LAST_YEAR
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON    
    LEFT JOIN MostRecent
        ON CON."Id" = MostRecent.CONTACT_ID
    LEFT JOIN LastYearGift
        ON CON."Id" = LastYearGift.CONTACT_ID2
    LEFT JOIN RecentGift
        ON CON."Id" = RecentGift.CONTACT_ID1
    LEFT JOIN Recent1
        ON CON."Id" = Recent1.CONTACT_ID
    LEFT JOIN RecentMeeting
        ON CON."Id" = RecentMeeting.CONTACT_ID1
    LEFT JOIN LastYearMeeting
        ON CON."Id" = LastYearMeeting.CONTACT_ID2
WHERE CON."Id" = '0031I00000WsO7aQAF';


/*
Comparing columns

Old:
    MEET.DATE_OF_MOST_RECENT_EVENT,
    COALESCE(MEET.TOTAL_MEETING_PAID_AMOUNT, 0) AS TOTAL_MEETING_PAID_AMOUNT,
    COALESCE(MEET.TOTAL_NUMBER_OF_EVENTS_ATTENDED, 0) AS TOTAL_NUMBER_OF_EVENTS_ATTENDED,
    COALESCE(MEET.TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR, 0) AS TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR,
    COALESCE(MEET.TOTAL_EVENTS_LAST_YEAR, 0) AS TOTAL_EVENTS_LAST_YEAR,
    COALESCE(OPP.MOST_RECENT_DONATION_AMOUNT, 0) AS MOST_RECENT_DONATION_AMOUNT,
    OPP.DATE_OF_MOST_RECENT_DONATION,
    COALESCE(OPP.TOTAL_DONATION_AMOUNT, 0) AS TOTAL_DONATION_AMOUNT,
    COALESCE(OPP.TOTAL_OPPORTUNITIES, 0) AS TOTAL_OPPORTUNITIES,
    COALESCE(OPP.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_AMOUNT_LAST_YEAR,
    COALESCE(OPP.TOTAL_OPPORTUNITIES_LAST_YEAR, 0) AS TOTAL_OPPORTUNITIES_LAST_YEAR,

New:
    COALESCE(MostRecent.MOST_RECENT_DONATION_AMOUNT, 0) AS MOST_RECENT_DONATION_AMOUNT,
    MostRecent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
    COALESCE(RecentGift.TOTAL_AMOUNT, 0) AS TOTAL_DONATION_AMOUNT,
    COALESCE(RecentGift.TOTAL_OPPORTUNITIES, 0) AS TOTAL_OPPORTUNITIES,
    COALESCE(LastYearGift.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_AMOUNT_LAST_YEAR,
    COALESCE(LastYearGift.TOTAL_OPPORTUNITIES_LAST_YEAR, 0) AS TOTAL_OPPORTUNITIES_LAST_YEAR
    Recent1.MOST_RECENT_EVENT AS DATE_OF_MOST_RECENT_EVENT,
    COALESCE(RecentMeeting.TOTAL_AMOUNT, 0) AS TOTAL_MEETING_PAID_AMOUNT,
    COALESCE(RecentMeeting.TOTAL_EVENTS, 0) AS TOTAL_NUMBER_OF_EVENTS_ATTENDED,
    COALESCE(LastYearMeeting.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR,
    COALESCE(LastYearMeeting.TOTAL_MEETING_LAST_YEAR, 0) AS TOTAL_EVENTS_LAST_YEAR



 */


/*
FINAL FINAL BELOW
*/

/*Opportunity data */
with RecentGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID1,
        SUM("Amount") AS TOTAL_AMOUNT,
        COUNT("Id") AS TOTAL_OPPORTUNITIES
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    GROUP BY 1
),
LastYearGift AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID2,
        SUM("Amount") AS TOTAL_AMOUNT_LAST_YEAR,
        COUNT("Id") AS TOTAL_OPPORTUNITIES_LAST_YEAR
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
    WHERE YEAR("CloseDate") = YEAR(CURRENT_DATE()) - 1
    GROUP BY 1
),
Recent AS (
    SELECT
        "npe01__Contact_Id_for_Role__c" AS CONTACT_ID,
        "CloseDate" DATE_OF_MOST_RECENT_DONATION,
        ROW_NUMBER() OVER(PARTITION BY "npe01__Contact_Id_for_Role__c" ORDER BY "CloseDate" DESC) AS ROW_ORDER,
        "Amount" AS MOST_RECENT_DONATION_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY
),
MostRecent AS (
    SELECT * FROM Recent WHERE ROW_ORDER = 1
),
RecentMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID1,
        COALESCE(SUM(ORDER_TOTAL), 0) AS TOTAL_AMOUNT,
        COALESCE(COUNT(PARTICIPATION_ID), 0) AS TOTAL_EVENTS
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
),
LastYearMeeting AS (
    SELECT
        CONTACT_ID AS CONTACT_ID2,
        COALESCE(SUM(ORDER_TOTAL), 0) AS TOTAL_AMOUNT_LAST_YEAR,
        COALESCE(COUNT(PARTICIPATION_ID), 0) AS TOTAL_MEETING_LAST_YEAR
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE YEAR(REGISTRATION_DATE) = YEAR(CURRENT_DATE()) - 1
        AND PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
),
Recent1 AS (
    SELECT
        CONTACT_ID AS CONTACT_ID,
        MAX(REGISTRATION_DATE) AS MOST_RECENT_EVENT
    FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP
    WHERE PARTICIPANT_STATUS = 'Yes'
    GROUP BY 1
),
/*Churn/push/title change columns*/
TitleChange AS (
    SELECT
        DISTINCT "Id"
    FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE
    GROUP BY 1
    HAVING COUNT(DISTINCT "Title") > 1 OR COUNT(DISTINCT "AccountId") > 1
),
Pushed1 AS (
    SELECT 
        *,
        LAG(CSM."Paid_thru_date__c") OVER(PARTITION BY CSM."AACR_ID" ORDER BY CSM."Paid_thru_date__c") AS PREVIOUS_PAID_THRU_DATE,
        LAG(CSM."Transaction Id") OVER(PARTITION BY CSM."AACR_ID" ORDER BY CSM."Paid_thru_date__c") AS PREVIOUS_TRANSACTION_ID
    FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE AS CSM
),
Pushed2 AS ( 
    SELECT
        DISTINCT "Id"
    FROM Pushed1 
    WHERE "Paid_thru_date__c" <> PREVIOUS_PAID_THRU_DATE --change in paid thru date
        AND "Transaction Id" IS NULL --current trans id is null
        AND "Id" NOT IN (SELECT "Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Email" LIKE '%yopmail.com%') --filtering out test accounts
        AND "Member_Type__c" <> 'Emeritus Member'
),
OffRoles1 AS (
    SELECT 
        *,
        LAG(CSM."Paid_thru_date__c") OVER(PARTITION BY CSM."AACR_ID" ORDER BY CSM."Paid_thru_date__c") AS PREVIOUS_PAID_THRU_DATE,
        LAG(CSM."Final_Membership_Status") OVER(PARTITION BY CSM."AACR_ID" ORDER BY CSM."Paid_thru_date__c") AS PREVIOUS_OM_STATUS
    FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE AS CSM
),
OffRoles2 AS (
    SELECT
        DISTINCT "Id"
    FROM OffRoles1 
    WHERE "Paid_thru_date__c" <> PREVIOUS_PAID_THRU_DATE
        AND PREVIOUS_OM_STATUS = 'Operating Member' AND "Final_Membership_Status" = 'Non-operating Member'
        AND "Id" NOT IN (SELECT "Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "Email" LIKE '%yopmail.com%') --filtering out test accounts
        AND "Member_Type__c" <> 'Emeritus Member'
)
SELECT
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    CON."Id",
    CON."AccountId",
    CON."Name",
    CON."RecordTypeId",
    CON."MailingStreet",
    CON."MailingCity",
    CON."MailingState",
    CON."MailingPostalCode",
    CON."MailingCountry",
    CON."Title",
    CON."Birthdate",
    CON."HasOptedOutOfEmail",
    CON."DoNotCall",
    CON."CreatedDate",
    CON."LastModifiedDate",
    CON."LastActivityDate",
    CON."IsEmailBounced",
    CON."OrderApi__Home_Do_Not_Call__c",
    CON."OrderApi__Mobile_Do_Not_Call__c",
    CON."OrderApi__Other_Do_Not_Call__c",
    CON."OrderApi__Personal_Email_Opt_Out__c",
    CON."OrderApi__Work_Do_Not_Call__c",
    CON."OrderApi__Work_Email_Opt_Out__c",
    CON."DonorApi__Deceased__c",
    CON."DonorApi__Do_Not_Solicit__c",
    CON."Gender__c",
    CON."Income_Level__c",
    CON."Initial_Join_Date__c",
    CON."Institution_Type__c",
    CON."Member_Type__c",
    CON."Membership_Status__c",
    CON."Paid_thru_date__c",
    CON."Primary_Research_Area_of_Expertise__c",
    CON."Professional_Role__c",
    CON."Race__c",
    CON."Salesforce_ID__c",
    CON."iMIS_ID__c",
    CON."Do_Not_Email__c",
    CON."Pre_Post_Doc__c",
    CON."Highest_Degree__c",
    CON."wf_net_worth__c",
    CON."wf_windfall_id__c",
    CON."wf_small_business_owner__c",
    CON."wf_imported_car_owner__c",
    CON."wf_multi_property_owner__c",
    CON."wf_nonprofit_board_member__c",
    CON."wf_recent_mortgage__c",
    CON."wf_political_donor__c",
    CON."wf_recent_divorce__c",
    CON."wf_recent_mover__c",
    CON."wf_top_political_donor__c",
    CON."wf_luxury_car_owner__c",
    CON."wf_trust__c",
    CON."wf_philanthropic_giver__c",
    CON."wf_plane_owner__c",
    CON."wf_boat_owner__c",
    CON."wf_political_party__c",
    COALESCE(MostRecent.MOST_RECENT_DONATION_AMOUNT, 0) AS MOST_RECENT_DONATION_AMOUNT,
    MostRecent.DATE_OF_MOST_RECENT_DONATION AS DATE_OF_MOST_RECENT_DONATION,
    COALESCE(RecentGift.TOTAL_AMOUNT, 0) AS TOTAL_DONATION_AMOUNT,
    COALESCE(RecentGift.TOTAL_OPPORTUNITIES, 0) AS TOTAL_OPPORTUNITIES,
    COALESCE(LastYearGift.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_AMOUNT_LAST_YEAR,
    COALESCE(LastYearGift.TOTAL_OPPORTUNITIES_LAST_YEAR, 0) AS TOTAL_OPPORTUNITIES_LAST_YEAR,
    Recent1.MOST_RECENT_EVENT AS DATE_OF_MOST_RECENT_EVENT,
    COALESCE(RecentMeeting.TOTAL_AMOUNT, 0) AS TOTAL_MEETING_PAID_AMOUNT,
    COALESCE(RecentMeeting.TOTAL_EVENTS, 0) AS TOTAL_NUMBER_OF_EVENTS_ATTENDED,
    COALESCE(LastYearMeeting.TOTAL_AMOUNT_LAST_YEAR, 0) AS TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR,
    COALESCE(LastYearMeeting.TOTAL_MEETING_LAST_YEAR, 0) AS TOTAL_EVENTS_LAST_YEAR,
    CASE WHEN CON."Id" IN (SELECT "Id" FROM TitleChange) THEN 1 ELSE 0 END AS "TITLE_CHANGE",
    CASE WHEN CON."Id" IN (SELECT DISTINCT "Id" FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE WHERE "Member_Type__c" IS NOT NULL) THEN 1 ELSE 0 END AS ASSOCIATED_WITH_MEMBERSHIP,
    CASE WHEN CON."Id" IN (SELECT "Id" FROM Pushed2) THEN 1 ELSE 0 END AS "PUSHED",
    CASE WHEN CON."Id" IN (SELECT "Id" FROM OffRoles2) THEN 1 ELSE 0 END AS "CHURNED"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON    
    LEFT JOIN MostRecent
        ON CON."Id" = MostRecent.CONTACT_ID
    LEFT JOIN LastYearGift
        ON CON."Id" = LastYearGift.CONTACT_ID2
    LEFT JOIN RecentGift
        ON CON."Id" = RecentGift.CONTACT_ID1
    LEFT JOIN Recent1
        ON CON."Id" = Recent1.CONTACT_ID
    LEFT JOIN RecentMeeting
        ON CON."Id" = RecentMeeting.CONTACT_ID1
    LEFT JOIN LastYearMeeting
        ON CON."Id" = LastYearMeeting.CONTACT_ID2
;



SELECT * FROM TEST.TEST_AREA.CONTACT_ACTIVITY AS CA WHERE CA."LastActivityDate" IS NULL;

SELECT "Id", "LastActivityDate", CON."Initial_Join_Date__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE "Id" = '0031I00000Wsm6rQAB';

SELECT * FROM IMISDATA.DBO.ACTIVITY AS ACT WHERE ACT.ACTIVITY_TYPE = 'GIFT' ORDER BY ACT.TRANSACTION_DATE;

--pledge
--GIFT
--

--4615 date 1988-08-15 00:00:00.000
SELECT 
    ID 
FROM PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS 
WHERE AACR_ID IN (
    '69270',
    '99980',
    '226128',
    '36475',
    '224019',
    '229921',
    '11841',
    '238042',
    '159450',
    '262549',
    '291163',
    '63543',
    '292183',
    '356798',
    '139206',
    '410554',
    '228862',
    '267986',
    '146648',
    '439079',
    '123910',
    '287970',
    '268594',
    '449517',
    '250923',
    '1075330',
    '31439',
    '87318',
    '144882',
    '16747',
    '374696',
    '265807',
    '139323',
    '65765',
    '1058171',
    '154303',
    '100120',
    '470145',
    '1089638',
    '268626',
    '340554',
    '142637',
    '147429',
    '156261',
    '10686',
    '82439',
    '79152',
    '1030643',
    '1297337',
    '70310',
    '317217',
    '260052',
    '262668',
    '61621',
    '127336',
    '240381',
    '147093',
    '236585',
    '1213486',
    '383314',
    '1088493',
    '127940',
    '11355',
    '119391',
    '1258593',
    '1194439',
    '7493',
    '10945',
    '351147'
);


SELECT 
    *
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MH
WHERE MH."SALESFORCE ID" = '0038W00002O13UFQAZ';

;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT_LINE__C; --this links to receipt on OrderApi__Receipt__c = Receipt ID

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C; --OrderApi__Contact__c goes to CONTACT Id

--Contact left join receipt left join receipt line
--receipt line description lists journal title




with LastYearMembership AS (
    SELECT 
        *,
        LAG("TERM YEAR") OVER(PARTITION BY "SALESFORCE ID" ORDER BY "TERM YEAR" DESC) AS PREVIOUS_TERM_YEAR,
        LAG("Transaction Id") OVER(PARTITION BY "SALESFORCE ID" ORDER BY "TERM YEAR" DESC) PREVIOUS_TRANSACTION_ID
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL
)
SELECT 
    *
FROM LASTYEARMEMBERSHIP
WHERE "SALESFORCE ID" = '0038W00002O13UFQAZ'
AND ("Transaction Id" IS NULL OR PREVIOUS_TRANSACTION_ID IS NULL);


SELECT
    *
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL
WHERE "SALESFORCE ID" = '0038W00002O13UFQAZ';


SELECT * FROM TEST.TEST_AREA.CONTACT_ACTIVITY AS CS WHERE CS."Id" = '0038W00001vsbPoQAI';

SELECT * FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE AS CS WHERE CS."Id" = '0038W00001vsbPoQAI';


/*
Testing
 */

with PSH AS (
    SELECT 
        "Id",
        "AACR_ID",
        "Paid_thru_date__c",
        "Transaction Id",
        CONVERTED_SNAPSHOT_DATE,
        LAG(Merged."Paid_thru_date__c") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_PAID_THRU_DATE,
        LAG(Merged."Transaction Id") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_TRANSACTION_ID
    FROM TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE
);


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Id" = 'a4jRm000000fLZxIAM';

SELECT 
    *
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
WHERE MR.EVENT_ID = 'a4jRm000000fLZxIAM' AND PARTICIPANT_STATUS = 'Yes'
ORDER BY REGISTRATION_DATE DESC;


SELECT 
    *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BR
WHERE BR."BR_Event__c" = 'a4jRm000000fLZxIAM' AND BR."Participate__c" = 'Yes'
ORDER BY BR."Registration_Date__c" DESC;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Name" LIKE '%Annual%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT WHERE "LastName" = 'Balog';

SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE EVENT_ID = 'a4jRm000000rDS9IAM' AND CONTACT_ID = '0031I00000Wsn9GQAR';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C WHERE "BR_Event__c" = 'a4jRm000000rDS9IAM' AND ;


SELECT * FROM TEST.TEST_AREA.CONTACT_ACTIVITY_T;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP WHERE OPP."Amount" < 0;

SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP;




SELECT CA. FROM TEST.TEST_AREA.CONTACT_ACTIVITY_T AS CA;


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C AS GL WHERE GL."Name" LIKE '%4020%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BUSINESS_GROUP__C;



SELECT * FROM TEST.TEST_AREA.CONTACT_ACTIVITY_T AS CA WHERE CA.PUSHED = 1;





SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C;


























/*
    '0038W00002O13UFQAZ',
    '0031I00000Wso9KQAR',
    '0038W00001vsbPoQAI',
    '0031I00000WruzTQAR',
    '0031I00001FhOVsQAN',
    '0031I00000Ws7NzQAJ',
    '0031I00000ZECeIQAX',
    '0031I00000Ws8uaQAB',
    '0031I00000bgOFqQAM',
    '0031I00000Wrw89QAB',
    '0031I00000Ws68vQAB',
    '0031I00000WsewZQAR',
    '0031I00000Wrup3QAB',
    '0031I00000Ws4LJQAZ',
    '0031I00001QTZNDQA5',
    '0031I000016luN0QAI',
    '0031I00000Ws4jbQAB',
    '0031I00000WsNuCQAV',
    '0031I00000Wrw2DQAR',
    '0031I00000WshDAQAZ',
    '0031I00000Ws8R8QAJ',
    '0031I00000WrvYHQAZ',
    '003Rm00000W6w5XIAR',
    '0031I00000WsPL2QAN',
    '0031I00000WrtXaQAJ',
    '0031I00000WsnbOQAR',
    '0031I00000mXEVeQAO',
    '0031I00000Ws9JeQAJ',
    '0031I00000WrxCpQAJ',
    '0031I00000WsZU5QAN',
    '0031I00000WrzF5QAJ',
    '0031I00000Ws9JRQAZ',
    '0031I00000Wry6hQAB',
    '0031I00000WsA1YQAV',
    '0031I00000WruIVQAZ',
    '0031I00000Wrxw1QAB',
    '0031I00000WspgiQAB',
    '0031I000016J3LwQAK',
    '0031I00000Ws5pdQAB',
    '0031I00000WrticQAB',
    '0031I000012yOrAQAU',
    '0031I00000WsEcbQAF',
    '0031I00000WsCQLQA3',
    '0031I00000Ws0GkQAJ',
    '0031I00000Ws84mQAB',
    '0031I00001DHg2qQAD',
    '0031I00000Wsol4QAB',
    '0031I00000Ws8PhQAJ',
    '0031I00000Ws4sDQAR',
    '0031I00000Ws02xQAB',
    '0031I00000WsqPbQAJ',
    '0031I00000Ws4OVQAZ',
    '0031I00000Ws5xzQAB',
    '0031I00000Ws6G9QAJ',
    '0031I00000Wrt5UQAR',
    '0031I00000WsFBhQAN',
    '0031I00000Ws9EaQAJ',
    '0031I00000WryBQQAZ',
    '0031I00000WsnB1QAJ',
    '0031I00000WsmnSQAR',
    '0031I00000bgXaGQAU',
    '0031I00000WspFbQAJ',
    '0031I00000WrxlZQAR',
    '0038W00002C7wOWQAZ',
    '0031I00000WsmvxQAB',
    '0031I00000WsN8TQAV',
    '0031I00000WsR9hQAF',
    '0031I00000WsC0nQAF',
    '0031I00000WsTBCQA3'




 */




SELECT * FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MH WHERE MH."Transaction Contact Id" = '0031I00000Wrup3QAB';



/*
a131I0000013Gj1QAE	:	Student Membership
a131I0000013GjGQAU	:	Honorary Membership
a131I0000013GjBQAU	:	Emeritus Membership
a131I0000013GicQAE	:	Associate Membership
a131I0000013GiDQAU	:	Active Membership
a131I0000013GimQAE	:	Affiliate Membership




 */

SELECT COUNT(DISTINCT "TERM ID"), COUNT(*) FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE WHERE "ITEM NAME" <> 'Emeritus Membership';

SELECT COUNT(DISTINCT "Composite Key Demo"), COUNT(*) FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;

SELECT * FROM TEST.TEST_AREA.MEMBERSHIP_PUSH_MERGE;












create or replace view TEST.CONTACT_SNAPSHOTS.CS_MEMBERSHIP_MERGE
as 
WITH AdjustedMembership AS (
    SELECT 
        m.*,
        -- Adjust month forward if transaction is after the snapshot date in the same month
        CASE 
            WHEN m."Transaction Date" > cs.CONVERTED_SNAPSHOT_DATE
            THEN DATEADD(MONTH, 1, m."Transaction Date")
            ELSE m."Transaction Date"
        END AS ADJUSTED_DATE
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL m
        LEFT JOIN TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM cs 
            ON m."SALESFORCE ID" = cs."Id"
            AND DATE_TRUNC('MONTH', m."Transaction Date") = DATE_TRUNC('MONTH', cs.CONVERTED_SNAPSHOT_DATE)
),
Merged AS (
    SELECT 
        cs.*, 
        am.*
    FROM TEST.CONTACT_SNAPSHOTS.FINAL_CONTACT_SNAPSHOT_WITH_OM AS cs
    LEFT JOIN AdjustedMembership am 
        ON cs."Id" = am."SALESFORCE ID" AND DATE_TRUNC('MONTH', cs.CONVERTED_SNAPSHOT_DATE) = DATE_TRUNC('MONTH', am.ADJUSTED_DATE)
), --Merged is the merge table for membership and contact snapshots. The CTEs below will pull from it and add lag logic so we can add pushed, churned, and title change columns at the end.
TC AS (
    SELECT 
        "Id",
        "Title",
        CONVERTED_SNAPSHOT_DATE,
        LAG(Merged.CONVERTED_SNAPSHOT_DATE) OVER(PARTITION BY Merged."Id" ORDER BY Merged.CONVERTED_SNAPSHOT_DATE) AS PREVIOUS_SNAPSHOT_DATE,
        LAG(Merged."Title") OVER(PARTITION BY Merged."Id" ORDER BY Merged.CONVERTED_SNAPSHOT_DATE) AS PREVIOUS_TITLE
    FROM Merged
),
CH AS (
    SELECT 
        "Id",
        "AACR_ID",
        "Paid_thru_date__c",
        "Final_Membership_Status",
        CONVERTED_SNAPSHOT_DATE,
        LAG(Merged."Paid_thru_date__c") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_PAID_THRU_DATE,
        LAG(Merged."Final_Membership_Status") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_OM_STATUS
    FROM Merged
),
PSH AS (
    SELECT 
        "Id",
        "AACR_ID",
        "Paid_thru_date__c",
        "Transaction Id",
        CONVERTED_SNAPSHOT_DATE,
        LAG(Merged."Paid_thru_date__c") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_PAID_THRU_DATE,
        LAG(Merged."Transaction Id") OVER(PARTITION BY Merged."AACR_ID" ORDER BY Merged."Paid_thru_date__c") AS PREVIOUS_TRANSACTION_ID
    FROM Merged
)
SELECT
    Merged.*,
    CASE 
        WHEN TC.PREVIOUS_SNAPSHOT_DATE <> Merged.CONVERTED_SNAPSHOT_DATE AND TC.PREVIOUS_TITLE <> Merged."Title" THEN 'Title Change'
    END AS TITLE_CHANGE,
    CASE 
        WHEN CH.PREVIOUS_PAID_THRU_DATE <> Merged."Paid_thru_date__c"
            AND CH.PREVIOUS_OM_STATUS = 'Operating Member' AND Merged."Final_Membership_Status" = 'Non-operating Member' THEN 'Churned'
    END AS CHURNED,
    CASE
        WHEN PSH.PREVIOUS_PAID_THRU_DATE <> Merged."Paid_thru_date__c" AND Merged."Transaction Id" IS NULL THEN 'Pushed'
    END AS PUSHED
FROM Merged
    LEFT JOIN TC 
        ON Merged."Id" = TC."Id" AND Merged.CONVERTED_SNAPSHOT_DATE = TC.CONVERTED_SNAPSHOT_DATE
    LEFT JOIN CH
        ON Merged."Id" = CH."Id" AND Merged.CONVERTED_SNAPSHOT_DATE = CH.CONVERTED_SNAPSHOT_DATE
    LEFT JOIN PSH
        ON Merged."Id" = PSH."Id" AND Merged.CONVERTED_SNAPSHOT_DATE = PSH.CONVERTED_SNAPSHOT_DATE
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS TERM
        ON Merged.
;




SELECT DISTINCT OPP."AccountId" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP WHERE OPP."Amount" > 0 AND OPP."npe01__Contact_Id_for_Role__c" IS NULL;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY WHERE "AccountId" = '0011I00000cb9vbQAA';



SELECT *
FROM  TABLE(TEST.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'REPL_RINGGOLD."RINGGOLD_IDENTIFY_CSV_ORGALTNAMES"',
        START_TIME => DATEADD('hour',-2,CURRENT_TIMESTAMP())
      ));

SELECT *
FROM  TABLE(TEST.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'REPL_RINGGOLD."RINGGOLD_IDENTIFY_CSV_PLACES"',
        START_TIME => DATEADD('hour',-2,CURRENT_TIMESTAMP())
      ));


GRANT USAGE ON SCHEMA TEST.ABSTRACT_TOPIC_PREDICTIONS TO ROLE INTERN;
GRANT SELECT ON ALL TABLES IN SCHEMA TEST.ABSTRACT_TOPIC_PREDICTIONS TO ROLE INTERN;


/*
Table Column	From Column
Transaction ID	OrderAPI__Transaction__c.ID
Transaction Contact	OrderAPI__Transaction__c.OrderAPI__Contact__c
Transaction Date	OrderAPI__Transaction__c.CreatedDate
Microsoft Dynamics ID	OrderAPI__Transaction__c.Microsoft_Dynamic_ID__c
Original Amount	
Sales Order Status	OrderAPI__Sales_Order__c.Status
Membership ID	
Payment Method	OrderAPI__Sales_Order__c.OrderApi__Payment_Type__c
Sales Order Posted	OrderAPI__Sales_Order__c.OrderApi__Posting_Status__c
	
Sales Order ID	
Receipt ID	




 */

 SELECT SO. FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
;


PRODUCTION.REPL_RINGGOLD
;



AND ACC."Id" NOT IN (
    '001Vq00000UmJpmIAF',
    '001Vq00000UmRuRIAV',
    '001Vq00000UlakhIAB',
    '001Vq00000Ulk3WIAR',
    '001Vq00000U7DEJIA3',
    '001Vq00000UlTO0IAN'
)

;

=MAX(1,MIN(4,
   IF(D3<1000000,E3-$M$3,
      IF(D3<5000000,E3-$L$3,
         E3))))

=MAX(1,MIN(4,
   IF(E2<2000000, J2-$X$8,
      IF(E2<5000000, J2-$Y$8,
         IF(E2<10000000, J2-$Z$8,
            J2)))))


=CHOOSECOLS('2025 Final'!A1:S219, 1, 2, 3, 4, 5, 14, 16)



create or replace view PRODUCTION.MART_SALESFORCE_OBJECTS.CONTACTS
as
SELECT
    CON."AACR_Announcements__c" AS AACR_ANNOUNCEMENTS,
    CON."AACR_Foundation__c" AS AACR_FOUNDATION,
    CASE
        WHEN CON."iMIS_ID__c" IS NULL THEN CON."Salesforce_ID__c"
        ELSE CON."iMIS_ID__c"
    END AS AACR_ID,
    --"Account_Name__c" AS ACCOUNT_NAME,
    CON."AccountId" AS ACCOUNT_ID,
    ACC."Name" AS ACCOUNT_NAME,
    CASE
        WHEN ACC."npe01__SYSTEMIsIndividual__c" = true THEN 'Individual'
        ELSE 'Organization'
    END AS "ACCOUNT_TYPE",
    CON."Additional_Research_Areas__c" AS ADDITIONAL_RESEARCH_AREAS,
    CON."Advocacy_Focus__c" AS ADVOCACY_FOCUS,
    CON."Advocate__c" AS ADVOCATE,
    CON."Advocate_Biography__c" AS ADVOCATE_BIOGRAPHY,
    CON."Annual_Meeting__c" AS ANNUAL_MEETING,
    CON."Application_Status__c" AS APPLICATION_STATUS,
    CON."Assistant_1__c" AS ASSISTANT_1,
    CON."AssistantName" AS ASSISTANT_NAME,
    CON."AssistantPhone" AS ASSISTANT_PHONE,
    CON."Awards_Grants__c" AS AWARDS_GRANTS,
    CON."Bad_Journal_Address__c" AS BAD_JOURNAL_ADDRESS,
    CON."Bad_Mailing_Address__c" AS BAD_MAILING_ADDRESS,
    CON."Bad_Other_Address__c" AS BAD_OTHER_ADDRESS,
    CON."Birthdate" AS BIRTHDATE,
    CON."Blacklisted__c" AS BLACKLISTED,
    --"c4g_Contact_Concatenation__c" AS CONTACT_NAME,
    CASE
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Personal' AND CON."OrderApi__Personal_Email_Opt_Out__c" = false THEN CON."OrderApi__Personal_Email__c"
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Work' AND CON."OrderApi__Work_Email_Opt_Out__c" = false THEN CON."OrderApi__Work_Email__c"
        WHEN CON."OrderApi__Preferred_Email_Type__c" = 'Other' AND CON."OrderApi__Other_Email_Opt_Out__c" = false THEN CON."OrderApi__Other_Email__c"
    END AS "ORDER_PREFERRED_EMAIL",
    CON."c4g_DS_Rating_Level__c" AS DS_RATING_LEVEL,
    CON."c4g_RFM__c" AS RFM,
    CON."c4g_Volunteer_Organization__c" AS VOLUNTEER_ORGANIZATION_2,
    CON."Campaign_Code__c" AS CAMPAIGN_CODE,
    CON."Cancer_Immunology_Group__c" AS CANCER_IMMUNOLOGY_GROUP,
    CON."Cancer_Policy_Advocacy__c" AS CANCER_POLICY_ADVOCACY,
    CON."Cancer_Today_Magazine__c" AS CANCER_TODAY_MAGAZINE,
    CON."CEO_Preference__c" AS CEO_PREFERENCE,
    CON."cfg_Actively_Prospecting_Major_Gift__c" AS ACTIVELY_PROSPECTING_MAJOR_GIFT,
    CON."cfg_Last_Donation_to_Runner_Date__c" AS LAST_DONATION_TO_RUNNER_DATE,
    CON."cfg_Last_Runner_Date__c" AS LAST_RUNNER_DATE,
    CON."cfg_Prospect_Priority__c" AS PROSPECT_PRIORITY,
    CON."Chemistry_in_Cancer_Research__c" AS CHEMISTRY_IN_CANCER_RESEARCH,
    --"Communication_Preferences__c" AS COMMUNICATION_PREFERENCES,
    CON."Complimentary_Journal__c" AS COMPLIMENTARY_JOURNAL,
    CON."Conferences_Workshops__c" AS CONFERENCES_WORKSHOPS,
    --"Contact_ID__c" AS CONTACT_ID,
    CON."Contact_StudentNominatorName__c" AS CONTACT_STUDENT_NOMINATOR_NAME,
    CON."Contact_StudentNominatorTitle__c" AS CONTACT_STUDENT_NOMINATOR_TITLE,
    CON."Contact_StudentNominatorType__c" AS CONTACT_STUDENT_NOMINATOR_TYPE,
    CON."CreatedById" AS CREATED_BY_ID,
    CON."CreatedDate" AS CREATED_DATE,
    CON."Current_Education_Status__c" AS CURRENT_EDUCATION_STATUS,
    CON."Date_of_Death__c" AS DATE_OF_DEATH,
    CON."ddrive__Configuration__c" AS DONORDRIVE_CONFIGURATION,
    CON."ddrive__Created_in_DonorDrive_By__c" AS DONORDRIVE_CREATED_BY,
    CON."ddrive__Created_in_DonorDrive_Relationship__c" AS DONORDRIVE_CREATED_RELATIONSHIP,
    CON."ddrive__Date_Entered__c" AS DONORDRIVE_DATE_ENTERED,
    CON."ddrive__DonorDrive_Company_Name__c" AS DONORDRIVE_COMPANY_NAME,
    CON."ddrive__DonorDrive_ID__c" AS DONORDRIVE_ID,
    CON."ddrive__DonorDrive_Type__c" AS DONORDRIVE_TYPE,
    CON."ddrive__Has_Extranet_Access__c" AS DONORDRIVE_HAS_EXTRANET_ACCESS,
    CON."ddrive__Is_Managed__c" AS DONORDRIVE_IS_MANAGED,
    CON."ddrive__Is_Offline__c" AS DONORDRIVE_IS_OFFLINE,
    CON."ddrive__Languages__c" AS DONORDRIVE_LANGUAGES,
    CON."ddrive__Last_Modified_by_in_DonorDrive__c" AS DONORDRIVE_LAST_MODIFIED_BY,
    CON."ddrive__Last_Modified_in_DonorDrive__c" AS DONORDRIVE_LAST_MODIFIED,
    CON."ddrive__Level__c" AS DONORDRIVE_LEVEL,
    CON."ddrive__Mobile_Carrier__c" AS DONORDRIVE_MOBILE_CARRIER,
    CON."ddrive__Needs_Merge_Review__c" AS DONORDRIVE_NEEDS_MERGE_REVIEW,
    CON."ddrive__Receive_Texts__c" AS DONORDRIVE_RECEIVE_TEXTS,
    CON."Degrees_Held__c" AS DEGREES_HELD,
    CON."Department" AS DEPARTMENT,
    CON."Description" AS DESCRIPTION,
    CON."Dietary_Needs__c" AS DIETARY_NEEDS,
    CON."DirectoryApi__Directory_Opt_Out__c" AS DIRECTORY_OPT_OUT,
    CON."Disability__c" AS DISABILITY,
    --"DNBoptimizer__DnBContactRecord__c" AS DNB_CONTACT_RECORD,
    CON."Do_Not_Display_in_Directory__c" AS DO_NOT_DISPLAY_IN_DIRECTORY,
    CON."Do_Not_Email__c" AS DO_NOT_EMAIL,
    CON."Do_Not_Mail__c" AS DO_NOT_MAIL,
    CON."Do_Not_Trade_Externally__c" AS DO_NOT_TRADE_EXTERNALLY,
    --"Donor_Roll__c" AS DONOR_ROLL,
    CON."Donor_Roll_Legacy__c" AS DONOR_ROLL_LEGACY,
    CON."DonorApi__Active_Recurring_Gift__c" AS DONOR_ACTIVE_RECURRING_GIFT,
    CON."DonorApi__Auto_Calculate_Contact_Greetings__c" AS DONOR_AUTO_CALCULATE_CONTACT_GREETINGS,
    CON."DonorApi__Auto_Calculate_Current_Ask_Amount__c" AS DONOR_AUTO_CALCULATE_CURRENT_ASK_AMOUNT,
    CON."DonorApi__Current_Ask_Amount__c" AS DONOR_CURRENT_ASK_AMOUNT,
    CON."DonorApi__Deceased__c" AS DONOR_DECEASED,
    --"DonorApi__Default_Greeting__c" AS DONOR_DEFAULT_GREETING,
    CON."DonorApi__Default_Greeting_Type__c" AS DONOR_DEFAULT_GREETING_TYPE,
    CON."DonorApi__Direct_Mail_Opt_Out__c" AS DONOR_DIRECT_MAIL_OPT_OUT,
    CON."DonorApi__Do_Not_Solicit__c" AS DONOR_DO_NOT_SOLICIT,
    CON."DonorApi__First_Gift_Date__c" AS DONOR_FIRST_GIFT_DATE,
    CON."DonorApi__Formal_Greeting__c" AS DONOR_FORMAL_GREETING,
    CON."DonorApi__Gift_Opportunities__c" AS DONOR_GIFT_OPPORTUNITIES,
    CON."DonorApi__Gifts_Outstanding__c" AS DONOR_GIFTS_OUTSTANDING,
    CON."DonorApi__Gifts_Received__c" AS DONOR_GIFTS_RECEIVED,
    --"DonorApi__Gifts_Value__c" AS DONOR_GIFTS_VALUE,
    CON."DonorApi__Include_in_Household_Greetings__c" AS DONOR_INCLUDE_IN_HOUSEHOLD_GREETINGS,
    CON."DonorApi__Languages__c" AS DONOR_LANGUAGES,
    CON."DonorApi__Largest_Gift__c" AS DONOR_LARGEST_GIFT,
    CON."DonorApi__Last_Gift_Amount__c" AS DONOR_LAST_GIFT_AMOUNT,
    CON."DonorApi__Last_Gift_Date__c" AS DONOR_LAST_GIFT_DATE,
    CON."DonorApi__Personal_Greeting__c" AS DONOR_PERSONAL_GREETING,
    CON."DonorApi__Personal_Recognition_Name__c" AS DONOR_PERSONAL_RECOGNITION_NAME,
    CON."DonorApi__Primary__c" AS DONOR_PRIMARY,
    CON."DonorApi__Primary_Relationship_Manager__c" AS DONOR_PRIMARY_RELATIONSHIP_MANAGER,
    CON."DonorApi__Spouse__c" AS DONOR_SPOUSE,
    CON."DonorApi__Suffix__c" AS DONOR_SUFFIX,
    CON."DonorDrive_ID_aacr__c" AS DONORDRIVE_ID_AACR,
    CON."DoNotCall" AS DO_NOT_CALL,
    CON."DRCTS__Directory_Preferences__c" AS DIRECTORY_PREFERENCES,
    CON."dtd__company__c" AS COMPANY_MATCHING_GIFTS,
    CON."Email" AS EMAIL,
    CON."EmailBouncedDate" AS EMAIL_BOUNCED_DATE,
    CON."EmailBouncedReason" AS EMAIL_BOUNCED_REASON,
    CON."et4ae5__HasOptedOutOfMobile__c" AS MOBILE_OPT_OUT,
    CON."et4ae5__Mobile_Country_Code__c" AS MOBILE_COUNTRY_CODE,
    CON."Event_Segment__c" AS EVENT_SEGMENT,
    CON."Expected_Completion_Date__c" AS EXPECTED_COMPLETION_DATE,
    CON."Family_Member_with_Cancer__c" AS FAMILY_MEMBER_WITH_CANCER,
    CON."Fax" AS FAX,
    CON."FirstName" AS FIRST_NAME,
    CON."Formerly_Known_As__c" AS FORMERLY_KNOWN_AS,
    CON."Foundation_Do_Not_Solicit__c" AS FOUNDATION_DO_NOT_SOLICIT,
    CON."Gender__c" AS GENDER,
    --"GW_Volunteers__First_Volunteer_Date__c" AS VOLUNTEER_FIRST_DATE,
    --"GW_Volunteers__Last_Volunteer_Date__c" AS VOLUNTEER_LAST_DATE,
    --"GW_Volunteers__Unique_Volunteer_Count__c" AS VOLUNTEER_UNIQUE_COUNT,
    CON."GW_Volunteers__Volunteer_Auto_Reminder_Email_Opt_Out__c" AS VOLUNTEER_AUTO_REMINDER_EMAIL_OPT_OUT,
    CON."GW_Volunteers__Volunteer_Availability__c" AS VOLUNTEER_AVAILABILITY,
    --"GW_Volunteers__Volunteer_Hours__c" AS VOLUNTEER_HOURS,
    CON."GW_Volunteers__Volunteer_Last_Web_Signup_Date__c" AS VOLUNTEER_LAST_WEB_SIGNUP_DATE,
    CON."GW_Volunteers__Volunteer_Manager_Notes__c" AS VOLUNTEER_MANAGER_NOTES,
    CON."GW_Volunteers__Volunteer_Notes__c" AS VOLUNTEER_NOTES,
    CON."GW_Volunteers__Volunteer_Organization__c" AS VOLUNTEER_ORGANIZATION,
    CON."GW_Volunteers__Volunteer_Skills__c" AS VOLUNTEER_SKILLS,
    CON."GW_Volunteers__Volunteer_Status__c" AS VOLUNTEER_STATUS,
    CON."HasOptedOutOfEmail" AS EMAIL_OPT_OUT,
    CON."HasOptedOutOfFax" AS FAX_OPT_OUT,
    CON."Hear_About_AACR__c" AS HEAR_ABOUT_AACR,
    CON."Highest_Degree__c" AS HIGHEST_DEGREE,
    CON."HomePhone" AS HOME_PHONE,
    CON."Id" AS ID,
    CON."iMIS_Created_Date__c" AS IMIS_CREATED_DATE,
    CON."iMIS_ID__c" AS IMIS_ID,
    CON."Income_Level__c" AS INCOME_LEVEL,
    CON."Initial_Join_Date__c" AS INITIAL_JOIN_DATE,
    --"Institution_Company_Name__c" AS INSTITUTION_COMPANY_NAME,
    CON."Institution_Type__c" AS INSTITUTION_TYPE,
    CON."is_affiliated_with_self__c" AS IS_AFFILIATED_WITH_SELF,
    CON."Is_Journal_Addr_Same_As_Preferred_Addr__c" AS IS_JOURNAL_ADDR_SAME_AS_PREFERRED_ADDR,
    CON."IsDeleted" AS IS_DELETED,
    CON."IsEmailBounced" AS IS_EMAIL_BOUNCED,
    CON."Jigsaw" AS JIGSAW_KEY,
    CON."JigsawContactId" AS JIGSAW_CONTACT_ID,
    CON."Journal_City__c" AS JOURNAL_CITY,
    CON."Journal_Country__c" AS JOURNAL_COUNTRY,
    CON."Journal_Email__c" AS JOURNAL_EMAIL,
    CON."Journal_Email_Opt_Out__c" AS JOURNAL_EMAIL_OPT_OUT,
    CON."Journal_State_Province__c" AS JOURNAL_STATE_PROVINCE,
    CON."Journal_Street__c" AS JOURNAL_STREET,
    CON."Journal_Zip_Postal_Code__c" AS JOURNAL_ZIP_POSTAL_CODE,
    CON."LastActivityDate" AS LAST_ACTIVITY_DATE,
    CON."LastCURequestDate" AS LAST_CU_REQUEST_DATE,
    CON."LastCUUpdateDate" AS LAST_CU_UPDATE_DATE,
    CON."LastModifiedById" AS LAST_MODIFIED_BY_ID,
    CON."LastModifiedDate" AS LAST_MODIFIED_DATE,
    CON."LastName" AS LAST_NAME,
    --"LastReferencedDate" AS LAST_REFERENCED_DATE,
    --"LastViewedDate" AS LAST_VIEWED_DATE,
    CON."Lead_List_Reason__c" AS LEAD_LIST_REASON,
    CON."Lead_List_Requestor__c" AS LEAD_LIST_REQUESTOR,
    CON."LeadSource" AS LEAD_SOURCE,
    CON."Maiden_Name__c" AS MAIDEN_NAME,
    CON."MailingCity" AS MAILING_CITY,
    CON."MailingCountry" AS MAILING_COUNTRY,
    CON."MailingCountryCode" AS MAILING_COUNTRY_CODE,
    CON."MailingGeocodeAccuracy" AS MAILING_GEOCODE_ACCURACY,
    CON."MailingLatitude" AS MAILING_LATITUDE,
    CON."MailingLongitude" AS MAILING_LONGITUDE,
    CON."MailingPostalCode" AS MAILING_POSTAL_CODE,
    CON."MailingState" AS MAILING_STATE,
    CON."MailingStateCode" AS MAILING_STATE_CODE,
    CON."MailingStreet" AS MAILING_STREET,
    CON."Major_Focus__c" AS MAJOR_FOCUS,
    CON."Manual_Segment_Override__c" AS MANUAL_SEGMENT_OVERRIDE,
    --"Marketing_Preferences__c" AS MARKETING_PREFERENCES,
    CON."MasterRecordId" AS MASTER_RECORD_ID,
    CON."Member_Type__c" AS MEMBER_TYPE,
    CON."Membership_Id__c" AS MEMBERSHIP_ID,
    CON."Membership_Information__c" AS MEMBERSHIP_INFORMATION,
    CON."Membership_Status__c" AS MEMBERSHIP_STATUS,
    CON."MiddleName" AS MIDDLE_NAME,
    CON."Minorities_in_Cancer_Research__c" AS MINORITIES_IN_CANCER_RESEARCH,
    CON."Minority_Institution__c" AS MINORITY_INSTITUTION,
    CON."MobilePhone" AS MOBILE_PHONE,
    --"Modify_Groups__c" AS MODIFY_GROUPS,
    --"Modify_Institution__c" AS MODIFY_INSTITUTION,
    CON."Molecular_Epidemiology__c" AS MOLECULAR_EPIDEMIOLOGY,
    CON."Name" AS NAME,
    CON."Nickname__c" AS NICKNAME,
    CON."npe01__AlternateEmail__c" AS NPE_ALTERNATE_EMAIL,
    --"npe01__Home_Address__c" AS NPE_HOME_ADDRESS,
    --"npe01__HomeEmail__c" AS NPE_HOME_EMAIL,
    --"npe01__Organization_Type__c" AS NPE_ORGANIZATION_TYPE,
    --"npe01__Other_Address__c" AS NPE_OTHER_ADDRESS,
    --"npe01__Preferred_Email__c" AS NPE_PREFERRED_EMAIL,
    --"npe01__PreferredPhone__c" AS NPE_PREFERRED_PHONE,
    --"npe01__Primary_Address_Type__c" AS NPE_PRIMARY_ADDRESS_TYPE,
    --"npe01__Private__c" AS NPE_PRIVATE,
    --"npe01__Secondary_Address_Type__c" AS NPE_SECONDARY_ADDRESS_TYPE,
    --"npe01__SystemAccountProcessor__c" AS NPE_SYSTEM_ACCOUNT_PROCESSOR,
    --"npe01__Type_of_Account__c" AS NPE_TYPE_OF_ACCOUNT,
    --"npe01__Work_Address__c" AS NPE_WORK_ADDRESS,
    --"npe01__WorkEmail__c" AS NPE_WORK_EMAIL,
    --"npe01__WorkPhone__c" AS NPE_WORK_PHONE,
    CON."npo02__AverageAmount__c" AS NPO_AVERAGE_AMOUNT,
    CON."npo02__Best_Gift_Year__c" AS NPO_BEST_GIFT_YEAR,
    CON."npo02__Best_Gift_Year_Total__c" AS NPO_BEST_GIFT_YEAR_TOTAL,
    CON."npo02__FirstCloseDate__c" AS NPO_FIRST_CLOSE_DATE,
    --"npo02__Formula_HouseholdMailingAddress__c" AS NPE_HOUSEHOLD_MAILING_ADDRESS,
    --"npo02__Formula_HouseholdPhone__c" AS NPE_HOUSEHOLD_PHONE,
    --"npo02__Household__c" AS NPO_HOUSEHOLD,
    --"npo02__Household_Naming_Order__c" AS NPO_HOUSEHOLD_NAMING_ORDER,
    --"npo02__LargestAmount__c" AS NPO_LARGEST_AMOUNT,
    --"npo02__LastCloseDate__c" AS NPO_LAST_CLOSE_DATE,
    --"npo02__LastCloseDateHH__c" AS NPO_LAST_HOUSEHOLD_CLOSE_DATE,
    --"npo02__LastMembershipAmount__c" AS NPO_LAST_MEMBERSHIP_AMOUNT,
    --"npo02__LastMembershipDate__c" AS NPO_LAST_MEMBERSHIP_DATE,
    --"npo02__LastMembershipLevel__c" AS NPO_LAST_MEMBERSHIP_LEVEL,
    --"npo02__LastMembershipOrigin__c" AS NPO_LAST_MEMBERSHIP_ORIGIN,
    --"npo02__LastOppAmount__c" AS NPO_LAST_OPP_AMOUNT,
    --"npo02__MembershipEndDate__c" AS NPO_MEMBERSHIP_END_DATE,
    --"npo02__MembershipJoinDate__c" AS NPO_MEMBERSHIP_JOIN_DATE,
    --"npo02__Naming_Exclusions__c" AS NPO_NAMING_EXCLUSIONS,
    --"npo02__NumberOfClosedOpps__c" AS NPO_NUMBER_OF_CLOSED_OPPS,
    --"npo02__NumberOfMembershipOpps__c" AS NPO_NUMBER_OF_MEMBERSHIP_OPPS,
    --"npo02__OppAmount2YearsAgo__c" AS NPO_OPP_AMOUNT_2_YEARS_AGO,
    --"npo02__OppAmountLastNDays__c" AS NPO_OPP_AMOUNT_LAST_N_DAYS,
    --"npo02__OppAmountLastYear__c" AS NPO_OPP_AMOUNT_LAST_YEAR,
    --"npo02__OppAmountLastYearHH__c" AS NPO_HOUSEHOLD_OPP_AMOUNT_LAST_YEAR,
    --"npo02__OppAmountThisYear__c" AS NPO_OPP_AMOUNT_THIS_YEAR,
    --"npo02__OppAmountThisYearHH__c" AS NPO_HOUSEHOLD_OPP_AMOUNT_THIS_YEAR,
    --"npo02__OppsClosed2YearsAgo__c" AS NPO_OPPS_CLOSED_2_YEARS_AGO,
    --"npo02__OppsClosedLastNDays__c" AS NPO_OPPS_CLOSED_LAST_N_DAYS,
    --"npo02__OppsClosedLastYear__c" AS NPO_OPPS_CLOSED_LAST_YEAR,
    --"npo02__OppsClosedThisYear__c" AS NPO_OPPS_CLOSED_THIS_YEAR,
    --"npo02__SmallestAmount__c" AS NPO_SMALLEST_AMOUNT,
    --"npo02__Soft_Credit_Last_Year__c" AS NPO_SOFT_CREDIT_LAST_YEAR,
    --"npo02__Soft_Credit_This_Year__c" AS NPO_SOFT_CREDIT_THIS_YEAR,
    --"npo02__Soft_Credit_Total__c" AS NPO_SOFT_CREDIT_TOTAL,
    --"npo02__Soft_Credit_Two_Years_Ago__c" AS NPO_SOFT_CREDIT_TWO_YEARS_AGO,
    --"npo02__SystemHouseholdProcessor__c" AS NPO_SYSTEM_HOUSEHOLD_PROCESSOR,
    --"npo02__Total_Household_Gifts__c" AS NPO_TOTAL_HOUSEHOLD_GIFTS,
    --"npo02__TotalMembershipOppAmount__c" AS NPO_TOTAL_MEMBERSHIP_OPP_AMOUNT,
    --"npo02__TotalOppAmount__c" AS NPO_TOTAL_OPP_AMOUNT,
    --"npsp__Address_Verification_Status__c" AS NPSP_ADDRESS_VERIFICATION_STATUS,
    --"npsp__Batch__c" AS NPSP_BATCH,
    --"npsp__Current_Address__c" AS NPSP_CURRENT_ADDRESS,
    --"npsp__CustomizableRollups_UseSkewMode__c" AS NPSP_CUSTOMIZABLE_ROLLUPS_USE_SKEW_MODE,
    --"npsp__Deceased__c" AS NPSP_DECEASED,
    --"npsp__Do_Not_Contact__c" AS NPSP_DO_NOT_CONTACT,
    --"npsp__Exclude_from_Household_Formal_Greeting__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_FORMAL_GREETING,
    --"npsp__Exclude_from_Household_Informal_Greeting__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_INFORMAL_GREETING,
    --"npsp__Exclude_from_Household_Name__c" AS NPSP_EXCLUDE_FROM_HOUSEHOLD_NAME,
    --"npsp__First_Soft_Credit_Amount__c" AS NPSP_FIRST_SOFT_CREDIT_AMOUNT,
    --"npsp__First_Soft_Credit_Date__c" AS NPSP_FIRST_SOFT_CREDIT_DATE,
    --"npsp__HHId__c" AS NPSP_HOUSEHOLD_ID,
    --"npsp__is_Address_Override__c" AS NPSP_IS_ADDRESS_OVERRIDE,
    --"npsp__Largest_Soft_Credit_Amount__c" AS NPSP_LARGEST_SOFT_CREDIT_AMOUNT,
    --"npsp__Largest_Soft_Credit_Date__c" AS NPSP_LARGEST_SOFT_CREDIT_DATE,
    --"npsp__Last_Soft_Credit_Amount__c" AS NPSP_LAST_SOFT_CREDIT_AMOUNT,
    --"npsp__Last_Soft_Credit_Date__c" AS NPSP_LAST_SOFT_CREDIT_DATE,
    --"npsp__Number_of_Soft_Credits__c" AS NPSP_NUMBER_OF_SOFT_CREDITS,
    --"npsp__Number_of_Soft_Credits_Last_N_Days__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_LAST_N_DAYS,
    --"npsp__Number_of_Soft_Credits_Last_Year__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_LAST_YEAR,
    --"npsp__Number_of_Soft_Credits_This_Year__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_THIS_YEAR,
    --"npsp__Number_of_Soft_Credits_Two_Years_Ago__c" AS NPSP_NUMBER_OF_SOFT_CREDITS_TWO_YEARS_AGO,
    --"npsp__Primary_Affiliation__c" AS NPSP_PRIMARY_AFFILIATION,
    --"npsp__Primary_Contact__c" AS NPSP_PRIMARY_CONTACT,
    --"npsp__Soft_Credit_Last_N_Days__c" AS NPSP_SOFT_CREDIT_LAST_N_DAYS,
    --"npsp__Sustainer__c" AS NPSP_SUSTAINER,
    --"npsp__Undeliverable_Address__c" AS NPSP_UNDELIVERABLE_ADDRESS,
    --"Omit_Household_Label__c" AS OMIT_HOUSEHOLD_LABEL,
    CON."OrderApi__Annual_Engagement_Score__c" AS ORDER_ANNUAL_ENGAGEMENT_SCORE,
    CON."OrderApi__Assistant_Do_Not_Call__c" AS ORDER_ASSISTANT_DO_NOT_CALL,
    CON."OrderApi__Assistant_Email__c" AS ORDER_ASSISTANT_EMAIL,
    CON."OrderApi__Assistant_Email_Opt_Out__c" AS ORDER_ASSISTANT_EMAIL_OPT_OUT,
    CON."OrderApi__Badges__c" AS ORDER_BADGES,
    CON."OrderApi__Home_Do_Not_Call__c" AS ORDER_HOME_DO_NOT_CALL,
    --"OrderApi__Is_Primary_Contact__c" AS ORDER_IS_PRIMARY_CONTACT,
    CON."OrderApi__Lifetime_Engagement_Score__c" AS ORDER_LIFETIME_ENGAGEMENT_SCORE,
    CON."OrderApi__Mobile_Do_Not_Call__c" AS ORDER_MOBILE_DO_NOT_CALL,
    CON."OrderApi__Other_Do_Not_Call__c" AS ORDER_OTHER_DO_NOT_CALL,
    CON."OrderApi__Other_Email__c" AS ORDER_OTHER_EMAIL,
    CON."OrderApi__Other_Email_Opt_Out__c" AS ORDER_OTHER_EMAIL_OPT_OUT,
    CON."OrderApi__Outstanding_Credits__c" AS ORDER_OUTSTANDING_CREDITS,
    CON."OrderApi__Personal_Email__c" AS ORDER_PERSONAL_EMAIL,
    CON."OrderApi__Personal_Email_Opt_Out__c" AS ORDER_PERSONAL_EMAIL_OPT_OUT,
    CON."OrderApi__Potential_Duplicate_Contacts__c" AS ORDER_POTENTIAL_DUPLICATE_CONTACTS,
    --"OrderApi__Preferred_Email__c" AS ORDER_PREFERRED_EMAIL,
    --"OrderApi__Preferred_Email_Type__c" AS ORDER_PREFERRED_EMAIL_TYPE,
    --"OrderApi__Preferred_Phone__c" AS ORDER_PREFERRED_PHONE,
    --"OrderApi__Preferred_Phone_Type__c" AS ORDER_PREFERRED_PHONE_TYPE,
    CON."OrderApi__Price_Rules_Usages__c" AS ORDER_PRICE_RULES_USAGES,
    CON."OrderApi__Privacy_Settings__c" AS ORDER_PRIVACY_SETTINGS,
    CON."OrderApi__Sync_Address_Billing__c" AS ORDER_SYNC_ADDRESS_BILLING,
    CON."OrderApi__Sync_Address_Shipping__c" AS ORDER_SYNC_ADDRESS_SHIPPING,
    CON."OrderApi__Sync_Email__c" AS ORDER_SYNC_EMAIL,
    CON."OrderApi__Sync_Phone__c" AS ORDER_SYNC_PHONE,
    CON."OrderApi__Work_Do_Not_Call__c" AS ORDER_WORK_DO_NOT_CALL,
    CON."OrderApi__Work_Email__c" AS ORDER_WORK_EMAIL,
    CON."OrderApi__Work_Email_Opt_Out__c" AS ORDER_WORK_EMAIL_OPT_OUT,
    CON."OrderApi__Work_Phone__c" AS ORDER_WORK_PHONE,
    CON."Organ_Sites__c" AS ORGAN_SITES,
    CON."Other_Dietary_Needs__c" AS OTHER_DIETARY_NEEDS,
    CON."Other_Research_Areas__c" AS OTHER_RESEARCH_AREAS,
    CON."OtherCity" AS OTHER_CITY,
    CON."OtherCountry" AS OTHER_COUNTRY,
    CON."OtherCountryCode" AS OTHER_COUNTRY_CODE,
    CON."OtherGeocodeAccuracy" AS OTHER_GEOCODE_ACCURACY,
    CON."OtherLatitude" AS OTHER_LATITUDE,
    CON."OtherLongitude" AS OTHER_LONGITUDE,
    CON."OtherPhone" AS OTHER_PHONE,
    CON."OtherPostalCode" AS OTHER_POSTAL_CODE,
    CON."OtherState" AS OTHER_STATE,
    CON."OtherStateCode" AS OTHER_STATE_CODE,
    CON."OtherStreet" AS OTHER_STREET,
    CON."Override_By__c" AS OVERRIDE_BY,
    CON."Override_Date__c" AS OVERRIDE_DATE,
    CON."Override_Reason__c" AS OVERRIDE_REASON,
    CON."OwnerId" AS OWNER_ID,
    CON."PagesApi__Cookie_Usage_Accepted__c" AS PAGES_COOKIE_USAGE_ACCEPTED,
    CON."PagesApi__Site__c" AS PAGES_SITE,
    CON."Paid_thru_date__c" AS PAID_THRU_DATE,
    CON."Personal_Email_Bounced__c" AS PERSONAL_EMAIL_BOUNCED,
    CON."Phone" AS PHONE,
    --"PhotoUrl" AS PHOTO_URL,
    CON."Pre_Post_Doc__c" AS PRE_POST_DOC,
    CON."Preferred_Address__c" AS PREFERRED_ADDRESS,
    CON."Primary_Research_Area_of_Expertise__c" AS PRIMARY_RESEARCH_AREA_OF_EXPERTISE,
    CON."Primary_Stakeholder__c" AS PRIMARY_STAKEHOLDER,
    CON."Primary_Stakeholder_Other__c" AS PRIMARY_STAKEHOLDER_OTHER,
    CON."Prior_Member_Status__c" AS PRIOR_MEMBER_STATUS,
    CON."Professional_Role__c" AS PROFESSIONAL_ROLE,
    CON."Race__c" AS RACE,
    CON."RecordTypeId" AS RECORD_TYPE_ID,
    RT."Name" AS RECORD_TYPE_NAME,
    CON."ReportsToId" AS REPORTS_TO_ID,
    --"RT_ByName__c" AS RECORD_TYPE_NAME,
    CON."Salesforce_ID__c" AS SALESFORCE_ID,
    CON."Salutation" AS SALUTATION,
    --"Scientific_Interests__c" AS SCIENTIFIC_INTERESTS,
    CON."Secondary_Stakeholder__c" AS SECONDARY_STAKEHOLDER,
    CON."Secondary_Stakeholder_Other__c" AS SECONDARY_STAKEHOLDER_OTHER,
    CON."smagicinteract__SMSOptOut__c" AS SMS_OPT_OUT,
    CON."Specific_Research_Areas__c" AS SPECIFIC_RESEARCH_AREAS,
    CON."SSP_Title__c" AS SSP_TITLE,
    CON."Stand_Up_2_Cancer__c" AS STAND_UP_2_CANCER,
    --"Suffix_Text__c" AS SUFFIX_TEXT,
    CON."Survivor__c" AS SURVIVOR,
    CON."Survivor_Advocacy__c" AS SURVIVOR_ADVOCACY,
    CON."SystemModstamp" AS SYSTEM_MODSTAMP,
    CON."Title" AS TITLE,
    CON."Tumor_Microenvironment__c" AS TUMOR_MICROENVIRONMENT,
    --"Unique_Contact_Counter__c" AS UNIQUE_CONTACT_COUNTER,
    CON."Unverified_Email_Address__c" AS UNVERIFIED_EMAIL_ADDRESS,
    --"Verify_ORCID__c" AS VERIFY_ORCID,
    CON."Volunteer_For__c" AS VOLUNTEER_FOR,
    CON."WBIL_Override_Reason__c" AS WBIL_OVERRIDE_REASON,
    CON."Women_in_Cancer_Research__c" AS WOMEN_IN_CANCER_RESEARCH,
    CON."Work_Email_Bounced__c" AS WORK_EMAIL_BOUNCED,
    CON."Work_Setting__c" AS WORK_SETTING,
    CON."Working_Groups__c" AS WORKING_GROUPS,
    --"Source_System" AS SRC_SYS_ID,
    --"ID" AS SRC_RECORD_ID,
    CON."CreatedDate" AS SRC_CREATED_DATE,
    CON."LastModifiedDate" AS SRC_LAST_UPDATED_DATE
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.RECORDTYPE AS RT
        ON CON."RecordTypeId" = RT."Id";








SELECT 
    "Transaction Date"::DATE,
    COUNT("Transaction Id"),
    SUM("Transaction Line Credit")
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL 
WHERE "Transaction Date"::DATE BETWEEN '2024-08-01' AND '2024-08-30'
GROUP BY 1
ORDER BY 1;

SELECT 
    COUNT(DISTINCT "Transaction Id"),
    SUM("Transaction Line Credit")
FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL 
WHERE "Transaction Date"::DATE BETWEEN '2024-08-01' AND '2024-08-30'
;

SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.

SELECT A."TERM CREATED DATE Fixed"::DATE FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS A

;
--whole month by day and total $



SELECT B.FULLNAME, A.* 
FROM IMISDATA.DBO.ACTIVITY AS A 
    JOIN IMISDATA.DBO.CONTACTMAIN AS B
        ON A.ID = B.ID
WHERE A.ID IN ('156133', '86698')
ORDER BY 1 DESC;

SELECT * FROM IMISDATA.DBO.CONTACTMAIN AS A WHERE A.ID = '156133';

--B0C83547-3505-410C-9B6D-D60CA0C9E871
SELECT * FROM IMISDATA.DBO.ADDRESSMAIN; AS A WHERE A. = 'B0C83547-3505-410C-9B6D-D60CA0C9E871';


SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT;


SELECT "Name", "Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C WHERE "Id" LIKE '%a4jRm000001CyvtIAC%';

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_PARTICIPATION__C AS BR WHERE BR."BR_Event__c" = 'a4jRm000001CyvtIAC' AND BR."Participate__c" = 'Yes';

SELECT * FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP WHERE EVENT_ID = 'a4jRm000001CyvtIAC' AND PARTICIPANT_STATUS = 'Yes';



SELECT DISTINCT A."Tag__c" FROM PRODUCTION.MART_CONTACT_DIMENSION_TAGS.CONTACT_DIMENSION_TAGS_BACKUP A;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__BADGE_TYPE__C;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C ORDER BY "Start_Date__c" DESC;

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT C WHERE C."ddrive__DonorDrive_ID__c" IS NULL;




/*
grace.zaccarelli@aacr.org;
suzanne.lesher@aacr.org;
vanessa.soondrum@aacr.org;
madhuparna.chakraborty@aacr.org;
leslie.buckingham@aacr.org;
parker.cole@aacr.org;
alfonso.lopez-coral@aacr.org;
taylor.mistretta@aacr.org;
katey.mallon@aacr.org;
lauren.santarone@aacr.org;
cass.hansbury@aacr.org;
lyngine.calizo@aacr.org;
brad.davidson@aacr.org;
coleen.mcmahon@aacr.org;
paul.driscoll@aacr.org;
julianna.latini@aacr.org;
kathleen.parker@aacr.org;
elizabeth.savitz@aacr.org;
imei.siu@aacr.org;
jose.perez@aacr.org;
grace.song@aacr.org;
morgan.robinson@aacr.org;
mike.stewart@aacr.org;
sasha.dutton@aacr.org;
araceli.estrada@aacr.org;
bill.lange@aacr.org;
jeffrey.cheung@aacr.org;
laura.scolaro@aacr.org;
catherine.bozeman@aacr.org;
kaitlin.koffermiller@aacr.org;
raquel.castellanos@aacr.org;
carrie.treadwell@aacr.org;
joshua.britton@aacr.org;
leona.bankoski@aacr.org;
michele.hartsough@aacr.org;
peyton.pflug@aacr.org;
ruby.olexy@aacr.org;
amy.link@aacr.org;
joseph.pontoski@aacr.org;
karen.honey@aacr.org;
nicole.bellanco@aacr.org;
srivani.ravoori@aacr.org;
tahirih.canahuate@aacr.org;
lauren.finacaro@aacr.org;
leeann.barber@aacr.org;
lynnsay.marsan@aacr.org;
maxwell.bicking@aacr.org;
nektaria.leli@aacr.org;
paul.turcotte@aacr.org;
roslin.thoppil@aacr.org;
rukiya.umoja@aacr.org;
christine.juestrich@aacr.org;
frederic.biemar@aacr.org




    CASE 

WHEN "Legacy_Item_Name__c" = 'COMP-O-Cancer Epidemiology, Biomarkers & Prevention Journal' THEN 'compoepidemiologybiomarkers'
WHEN "Legacy_Item_Name__c" = 'COMP-P-Cancer Epidemiology; Biomarkers & Prevention Journal' THEN 'comppepidemiologybiomarkers'
WHEN "Legacy_Item_Name__c" = 'Cancer Epidemiology, Biomarkers & Prevention Journal - Online' THEN 'epidemiologybiomarkersjournalo'
WHEN "Legacy_Item_Name__c" = 'Cancer Epidemiology, Biomarkers & Prevention Journal - Print' THEN 'epidemiologybiomarkersjournalp'
WHEN "Legacy_Item_Name__c" = 'Pathology in Cancer Research Working Group (PICR)' THEN 'pathologyincancerresearchworkinggroup'




 */
;


