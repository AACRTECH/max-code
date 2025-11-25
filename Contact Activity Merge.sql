/*

Member Activity - Anything from CONTACT that might be relevant for models, 
as well as calculated fields from OPPORTUNITY and MEETING_REGISTRATIONS

*/
CREATE OR REPLACE TABLE TEST.TEST_AREA.CONTACT_ACTIVITY_T
--CREATE OR REPLACE VIEW TEST.TEST_AREA.CONTACT_ACTIVITY
AS
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
/*Meeting Reg Data */
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


SELECT COUNT(*) FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON WHERE CON."Member_Type__c" IS NULL;

SELECT DATEDIFF(DAY, "DATE_OF_MOST_RECENT_EVENT", CURRENT_DATE) FROM TEST.TEST_AREA.CONTACT_ACTIVITY LIMIT 10;


--To get actual dataset:
CREATE OR REPLACE VIEW TEST.TEST_AREA.CONTACT_ACTIVITY_FINAL
AS
SELECT
    "Id" AS CONTACT_ID,
    ROW_NUMBER() OVER(ORDER BY "Id") AS NEW_ID,
    "MailingCountry" AS MAILING_COUNTRY,
    LEFT("MailingPostalCode", 5) AS MAILING_ZIP_CODE,
    DATEDIFF(YEAR, "Birthdate", CURRENT_DATE) AS "AGE",
    "HasOptedOutOfEmail" AS HAS_OPTED_OUT_OF_EMAIL,
    "DoNotCall" AS DO_NOT_CALL,
    DATEDIFF(DAY, "CreatedDate", CURRENT_DATE) DAYS_SINCE_CREATED,
    DATEDIFF(DAY, "LastModifiedDate", CURRENT_DATE) AS DAYS_SINCE_MODIFIED,
    DATEDIFF(DAY, "LastActivityDate", CURRENT_DATE) AS DAYS_SINCE_LAST_ACTIVITY,
    "OrderApi__Home_Do_Not_Call__c" AS HOME_DO_NOT_CALL,
    "OrderApi__Mobile_Do_Not_Call__c" AS MOBILE_DO_NOT_CALL,
    "OrderApi__Other_Do_Not_Call__c" AS OTHER_DO_NOT_CALL,
    "OrderApi__Personal_Email_Opt_Out__c" AS PERSONAL_EMAIL_OPT_OUT,
    "OrderApi__Work_Do_Not_Call__c" AS WORK_DO_NOT_CALL,
    "OrderApi__Work_Email_Opt_Out__c" AS WORK_EMAIL_OPT_OUT,
    "DonorApi__Deceased__c" AS DECEASED,
    "Gender__c" AS GENDER,
    "Income_Level__c" AS INCOME_LEVEL,
    DATEDIFF(DAY, "Initial_Join_Date__c", CURRENT_DATE) AS DAYS_SINCE_JOINED,
    "Institution_Type__c" AS INSTITUTION_TYPE,
    "Member_Type__c" AS MEMBER_TYPE,
    "Membership_Status__c" AS MEMBERSHIP_STATUS,
    "Primary_Research_Area_of_Expertise__c" AS PRIMARY_RESEARCH_AREA,
    "Race__c" AS RACE,
    "Do_Not_Email__c" AS DO_NOT_EMAIL,
    "Pre_Post_Doc__c" AS PRE_POST_DOC,
    "Highest_Degree__c" AS HIGHEST_DEGREE,
    "wf_net_worth__c" AS NET_WORTH,
    "wf_small_business_owner__c" AS SMALL_BUSINESS_OWNER,
    "wf_imported_car_owner__c" AS IMPORTED_CAR_OWNER,
    "wf_multi_property_owner__c" AS MULTI_PROPERTY_OWNER,
    "wf_nonprofit_board_member__c" AS NONPROFIT_BOARD_MEMBER,
    "wf_recent_mortgage__c" AS RECENT_MORTGAGE,
    "wf_political_donor__c" AS POLITICAL_DONOR,
    "wf_recent_divorce__c" AS RECENT_DIVORCE,
    "wf_recent_mover__c" AS RECENT_MOVER,
    "wf_top_political_donor__c" AS TOP_POLITICAL_DONOR, 
    "wf_luxury_car_owner__c" AS LUXURY_CAR_OWNER,
    "wf_trust__c" AS TRUST,
    "wf_philanthropic_giver__c" AS PHILANTHROPIC_GIVER,
    "wf_plane_owner__c" AS PLANE_OWNER,
    "wf_boat_owner__c" AS BOAT_OWNER,
    "wf_political_party__c" AS POLITICAL_PARTY,
    DATEDIFF(DAY, "DATE_OF_MOST_RECENT_EVENT", CURRENT_DATE) AS DAYS_SINCE_MOST_RECENT_EVENT,
    "TOTAL_MEETING_PAID_AMOUNT",
    "TOTAL_NUMBER_OF_EVENTS_ATTENDED",
    "TOTAL_MEETING_PAID_AMOUNT_LAST_YEAR",
    "TOTAL_EVENTS_LAST_YEAR",
    "MOST_RECENT_DONATION_AMOUNT",
    DATEDIFF(DAY, "DATE_OF_MOST_RECENT_DONATION", CURRENT_DATE) AS DAYS_SINCE_MOST_RECENT_DONATION,
    "TOTAL_DONATION_AMOUNT",
    "TOTAL_OPPORTUNITIES",
    "TOTAL_AMOUNT_LAST_YEAR",
    "TOTAL_OPPORTUNITIES_LAST_YEAR",
    "ASSOCIATED_WITH_MEMBERSHIP",
    "TITLE_CHANGE",
    "PUSHED",
    "CHURNED"
FROM TEST.TEST_AREA.CONTACT_ACTIVITY_T;



/*
WITH base_dates AS (
    SELECT 
        OPP."npe01__Contact_Id_for_Role__c" AS contact_id,
        OPP."Amount",
        OPP."CloseDate"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    WHERE OPP."CloseDate" IS NOT NULL
),
-- Donations in last 2 years
2y_recent_donations AS (
    SELECT 
        contact_id,
        SUM(Amount) AS total_donated_last_2y
    FROM base_dates
    WHERE CloseDate >= DATEADD(YEAR, -2, CURRENT_DATE)
    GROUP BY contact_id
),
-- Donations before last 2 years
2y_early_donations AS (
    SELECT 
        contact_id,
        SUM(Amount) AS total_donated_first_2y
    FROM base_dates
    WHERE CloseDate < DATEADD(YEAR, -2, CURRENT_DATE)
    GROUP BY contact_id
)
-- Combine into one summary per donor
SELECT 
    COALESCE(r.contact_id, e.contact_id) AS contact_id,
    COALESCE(total_donated_last_2y, 0) AS total_donated_last_2y,
    COALESCE(total_donated_first_2y, 0) AS total_donated_first_2y
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN recent_donations AS r
        ON CON."Id" = r.contact_id
    LEFT JOIN early_donations as e
        ON CON."Id" = e.contact_id
 */

WITH base_dates AS (
    SELECT 
        OPP."npe01__Contact_Id_for_Role__c" AS contact_id,
        OPP."Amount",
        OPP."CloseDate"
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS OPP
    WHERE OPP."CloseDate" IS NOT NULL
),
-- Donations in last 2 years
twoy_recent_donations AS (
    SELECT 
        contact_id,
        SUM("Amount") AS total_donated_last_2y
    FROM base_dates
    WHERE "CloseDate" >= DATEADD(YEAR, -2, CURRENT_DATE)
    GROUP BY contact_id
),
-- Donations before last 2 years
twoy_early_donations AS (
    SELECT 
        contact_id,
        SUM("Amount") AS total_donated_first_2y
    FROM base_dates
    WHERE "CloseDate" < DATEADD(YEAR, -2, CURRENT_DATE)
    GROUP BY contact_id
)
-- Combine into one summary per donor
SELECT 
    CON."Id",
    r.contact_id,
    e.contact_id,
    COALESCE(total_donated_last_2y, 0) AS total_donated_last_2y,
    COALESCE(total_donated_first_2y, 0) AS total_donated_first_2y,
    total_donated_last_2y / NULLIF(total_donated_first_2y, 0) AS donation_growth_rate
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    LEFT JOIN twoy_recent_donations AS r
        ON CON."Id" = r.contact_id
    LEFT JOIN twoy_early_donations as e
        ON CON."Id" = e.contact_id
WHERE CON."wf_net_worth__c" IS NOT NULL;
















