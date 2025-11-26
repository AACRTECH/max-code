--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CAMPAIGN

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CAMPAIGN
AS
SELECT
    CONCAT(C."Id", A."Golden_Record_ID__c") AS "Primary_Key",
    CAST(A."Golden_Record_ID__c" AS VARCHAR) AS "GOLDEN_RECORD_ID",
    A."AACR_ID__c" AS AACR_ID,
    A."Id" AS ACCOUNT_ID,
    a."PersonContactId",
    C."Id" AS CAMPAIGN_ID,
    B."Name",
    C."IsDeleted",
    C."Name" AS CAMPAIGN_NAME,
    C."ParentId",
    C."Type" AS CAMPAIGN_TYPE,
    C."RecordTypeId",
    C."Status",
    C."StartDate",
    C."EndDate",
    C."ExpectedRevenue",
    C."BudgetedCost",
    C."ActualCost",
    C."ExpectedResponse",
    C."NumberSent",
    C."IsActive",
    C."Participant_Type__c",
    C."DD_Campaign_ID__c",
    C."Fiscal_Year_Reporting__c",
    C."Participant_ID__c",
    C."Captain_Participant__c",
    C."City__c",
    C."DD_Record_ID__c",
    C."acem__Available_For_Guest_Users__c",
    C."Default_Designation_Percent_Total__c",
    C."Event_Type__c"
FROM PRODUCTION.REPL_NPC.ACCOUNT AS a
LEFT JOIN PRODUCTION.REPL_NPC.CAMPAIGNMEMBER AS b 
    ON a."PersonContactId"=b."ContactId"
LEFT JOIN PRODUCTION.REPL_NPC.CAMPAIGN AS c 
    ON b."CampaignId" = c."Id"
WHERE C."IsActive" = 'TRUE'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(GOLDEN_RECORD_ID, CAMPAIGN_ID) ORDER BY GOLDEN_RECORD_ID) = 1
;






--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CANCER_POLICY_MONITOR

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CANCER_POLICY_MONITOR
AS
SELECT 
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
FROM LEAD_LISTS_DB.RESULTS_TABLE.RESULTS AS A
WHERE A.LEAD_USE = 'Cancer Policy Monitor' 
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(A.GOLDEN_RECORD_ID, A.TICKET_NUMBER) ORDER BY A.GOLDEN_RECORD_ID) = 1;





--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CANCER_TODAY

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_CANCER_TODAY
AS
SELECT 
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
FROM LEAD_LISTS_DB.RESULTS_TABLE.RESULTS AS A
WHERE A.LEAD_USE = 'Cancer Today' 
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(A.GOLDEN_RECORD_ID, A.TICKET_NUMBER) ORDER BY A.GOLDEN_RECORD_ID) = 1;



--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_EDUCATION

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_EDUCATION
AS
SELECT 
    A."Golden_Record_ID__c" AS "Primary_Key",
    A."Golden_Record_ID__c",
    A."Id",
    A."Name",
    B."Account__c",
    B."Academic_Status__c",
    B."Academic_Degree__c",
    B."Institution_Name__c",
    B."Pursuing_Degree__c" 
FROM PRODUCTION.REPL_NPC.ACCOUNT AS A 
    LEFT JOIN PRODUCTION.REPL_NPC.education__c AS B 
        ON A."Id" = B."Account__c" 
WHERE B."Account__c" IS NOT NULL;



--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_EVENTS

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_EVENTS
AS
SELECT 
    CONCAT(P."Id", A."Golden_Record_ID__c") AS "Primary_Key",
    CAST(A."Golden_Record_ID__c" AS VARCHAR) AS GOLDEN_RECORD_ID,
    A."AACR_ID__c" AS AACR_ID,
    A."Id" AS NPC_ACCOUNT_ID,
    P."Id" as PARTICIPATION_ID,
    E."Name" AS EVENT_NAME,
    E."Id" AS EVENT_ID,
    E."acem__Start__c" AS START_DATE,
    E."acem__End__c" AS END_DATE,
    P."acem__Status__c" AS STATUS
FROM PRODUCTION.REPL_NPC.ACEM__EVENT_PARTICIPATION__C AS P 
    LEFT JOIN PRODUCTION.REPL_NPC.ACEM__EVENT__C AS E
        ON P."acem__Event__c" = E."Id" 
    JOIN PRODUCTION.REPL_NPC.ACCOUNT AS A
        ON A."PersonContactId" = P."acem__Guest_As_Contact__c"
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(GOLDEN_RECORD_ID, PARTICIPATION_ID) ORDER BY GOLDEN_RECORD_ID) = 1;






--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_PREMIUMS

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_PREMIUMS
AS 
WITH latest_status AS (
  SELECT
    "Account__c",
    "Id",
    
    CASE
        WHEN "Legacy_Item_Name__c" = 'COMP-O-Cancer Epidemiology, Biomarkers & Prevention Journal' THEN 'compoepidemiologybiomarkers'
        WHEN "Legacy_Item_Name__c" = 'COMP-P-Cancer Epidemiology; Biomarkers & Prevention Journal' THEN 'comppepidemiologybiomarkers'
        WHEN "Legacy_Item_Name__c" = 'Cancer Epidemiology, Biomarkers & Prevention Journal - Online' THEN 'epidemiologybiomarkersjournalo'
        WHEN "Legacy_Item_Name__c" = 'Cancer Epidemiology, Biomarkers & Prevention Journal - Print' THEN 'epidemiologybiomarkersjournalp'
        WHEN "Legacy_Item_Name__c" = 'Pathology in Cancer Research Working Group (PICR)' THEN 'pathologyincancerresearchworkinggroup'
        WHEN "Legacy_Item_Name__c" = 'Behavioral Science in Cancer Research (BSCR)' THEN 'Behavioral Science in Cancer Research WG'
        WHEN "Legacy_Item_Name__c" = 'Cancer Evolution (CEWG)' THEN 'Cancer Evolution WG'
        WHEN "Legacy_Item_Name__c" = 'Cancer Immunology (CIMM)' THEN 'Cancer Immunology WG'
        WHEN "Legacy_Item_Name__c" = 'Cancer Prevention (CPWG)' THEN 'Cancer Prevention WG'
        WHEN "Legacy_Item_Name__c" = 'Chemistry in Cancer Research (CICR)' THEN 'Chemistry in Cancer Research WG'
        WHEN "Legacy_Item_Name__c" = 'Hematologic Malignancies Working Group (HMWG)' THEN 'Hematologic Malignancies WG'
        WHEN "Legacy_Item_Name__c" = 'Minorities in Cancer Research (MICR)' THEN 'Minorities in Cancer Research WG'
        WHEN "Legacy_Item_Name__c" = 'Pathology in Cancer Research Working Group (PICR)' THEN 'Pathology in Cancer Research WG'
        WHEN "Legacy_Item_Name__c" = 'Pediatric Cancer (PCWG)' THEN 'Pediatric Cancer WG'
        WHEN "Legacy_Item_Name__c" = 'Population Sciences (PSWG)' THEN 'Population Sciences WG'
        WHEN "Legacy_Item_Name__c" = 'Radiation Science and Medicine (RSM)' THEN 'Radiation Science and Medicine WG'
        WHEN "Legacy_Item_Name__c" = 'Tumor Microenvironment (TME)' THEN 'Tumor Microenvironment WG'
        WHEN "Legacy_Item_Name__c" = 'Women in Cancer Research (WICR)' THEN 'Women in Cancer Research WG'
        ELSE LOWER(REGEXP_REPLACE("Legacy_Item_Name__c", '[^a-zA-Z0-9]', ''))
    END AS "Legacy_Item_Name__c",
    "Status__c",
    ROW_NUMBER() OVER (
      PARTITION BY "Account__c",
      "Legacy_Item_Name__c"
      ORDER BY
        "End_Date__c" DESC NULLS LAST
    ) AS rn
  FROM
    PRODUCTION.REPL_NPC.PREMIUMS_ASSIGNMENT__C
)
SELECT
  *
FROM
  (
    SELECT
      a."Name" AS account_name,
      a."Id" AS account_id,
      --p.ITEM_NAME,
      p."Legacy_Item_Name__c",
      p."Status__c",
      a."Golden_Record_ID__c"
    FROM
      latest_status AS p
      JOIN PRODUCTION.REPL_NPC.ACCOUNT AS a ON p."Account__c" = a."Id"
    WHERE
      p.rn = 1
      AND NOT a."Golden_Record_ID__c" IS NULL
  ) PIVOT(
    MAX("Status__c") FOR "Legacy_Item_Name__c" IN (
      SELECT
        DISTINCT LOWER(REGEXP_REPLACE("Legacy_Item_Name__c", '[^a-zA-Z0-9]', ''))
      FROM
        PRODUCTION.REPL_NPC.PREMIUMS_ASSIGNMENT__C
      WHERE
        NOT "Legacy_Item_Name__c" IS NULL
    )
  );


--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_INTERESTS

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_INTERESTS
AS
SELECT 
    CONCAT(A."Id", A."Golden_Record_ID__c") AS "Primary_Key",
    A."Id" AS "ACCOUNT_ID", 
    A."AACR_ID__c", 
    CAST(A."Golden_Record_ID__c" AS VARCHAR) AS "GOLDEN_RECORD_ID", 
    A."Name" AS "ACCOUNT_NAME", 
    B.* 
FROM PRODUCTION.REPL_NPC.ACCOUNT AS A
    LEFT JOIN PRODUCTION.REPL_NPC.CONTACT_INTERESTS__C AS B
        ON A."Id"= B."Account__c";



--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_LEADS

CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_LEADS
AS
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




--PRODUCTION.MART_DATACLOUD.SUBSCRIBER_MASTER
CREATE OR REPLACE VIEW PRODUCTION.MART_DATACLOUD.SUBSCRIBER_MASTER
AS
with Account_Clean_Email AS (
    SELECT
        *,
        CASE
          WHEN "Preferred_Address__c" ILIKE '%Business%' THEN "ShippingCountry"
          WHEN "Preferred_Address__c" ILIKE '%Mailing%'  THEN "PersonMailingCountry"
          WHEN NULLIF(TRIM("ShippingCountry"), '') IS NOT NULL THEN "ShippingCountry"
          WHEN NULLIF(TRIM("PersonMailingCountry"), '') IS NOT NULL THEN "PersonMailingCountry"
          ELSE 'United States'
        END AS PREFERRED_COUNTRY,
        CASE
          WHEN "Preferred_Address__c" ILIKE '%Business%' THEN "ShippingCountryCode"
          WHEN "Preferred_Address__c" ILIKE '%Mailing%'  THEN "PersonMailingCountryCode"
          WHEN NULLIF(TRIM("ShippingCountry"), '') IS NOT NULL THEN "ShippingCountryCode"
          WHEN NULLIF(TRIM("PersonMailingCountry"), '') IS NOT NULL THEN "PersonMailingCountryCode"
          ELSE 'US'
        END AS PREFERRED_COUNTRY_CODE,
        REGEXP_REPLACE("PersonEmail", '\\.invalid$', '') AS "Clean_PersonEmail",
        REGEXP_REPLACE("Work_Email__c", '\\.invalid$', '') AS "Clean_WorkEmail",
        REGEXP_REPLACE("Personal_Email__c", '\\.invalid$', '') AS "Clean_PersonalEmail"
    FROM PRODUCTION.REPL_NPC.ACCOUNT
    WHERE "RecordTypeId" = '012Hp000002JUc1IAG'
),
Donation_History AS (
    SELECT 
        "DonorId",
        MIN("TransactionDate") AS EARLIEST_DONATION_DATE,
        MAX("TransactionDate") MOST_RECENT_DONATION_DATE,
        SUM("OriginalAmount") AS TOTAL_DONATION_AMOUNT
    FROM PRODUCTION.REPL_NPC.GIFTTRANSACTION
    WHERE "RecordTypeId" IN ('012Vq000001Id44IAC', '012Vq000001Id46IAC', '012Vq000001SCVVIA4') --these are matching, standard, & in kind
    GROUP BY "DonorId"
)
SELECT 
      c."Golden_Record_ID__c"::VARCHAR AS "Golden_Record_Id"
    , c."FirstName" as "First_Name"
    , c."LastName" as "Last_Name"
    , c."Salutation"
    , c."Preferred_Email_Type__c"
    , case 
        when c."Preferred_Email_Type__c" = 'Work' and c."Clean_WorkEmail" is not null then c."Clean_WorkEmail"
        when c."Preferred_Email_Type__c" = 'Personal' and c."Clean_PersonalEmail" is not null then c."Clean_PersonalEmail"
        else c."Clean_PersonEmail"
        end as "Preferred_Email_Address"
    , TRUE as "Email_Is_Primary"
    , c."Membership_Type__c" as "Membership_Type"
    , c."Membership_Status__c" as "Membership_Status"
    , c."Initial_Join_Date__c" as "Member_Join_Date"
    , c."Paid_Thru_Date__c" as "Member_Paid_Thru_Date"
    , c."Expected_Completion_Date__c" 
    , COALESCE(NULLIF(c."AACR_ID__c", ''), NULLIF(c."AACRAccountID__c", ''), c."AACR_Id_Autonumber__c") as "Member_Number"
    , TRUE as "Primary_Membership"
    , c."Do_Not_Solicit__c" as "Do_Not_Solicit"
    , FALSE as "Do_Not_Contact"
    , c."PersonMailingCity" as "City"
    , c."PersonMailingState" as "State"
    , CR.LABEL AS "Country"
    , c."Professional_Role__c" as "Professional_Role"
    , c."Work_Setting__c" as "Work_Setting"
    , c."Highest_Degree__c" as "Highest_Degree"
    , c."Current_Education_Status__c" as "Current_Education_Status"
    , c."Pre_Post_Doc__c" as "Pre_Post_Doc"
    , c."Foundation_Only__c" as "Foundation_Only"
    , comp."Name" as "Organization_Name"
    , comp."Institution_Type__c" as "Organization_Industry"
    , c."PersonLeadSource" as "Source"
    , c."Fonteva_Contact_ID__c" as "Fonteva_Id"
    , CR.REGION__C
    , CR.NUMERIC_CLASSIFICATION__C
    , CASE WHEN CR.NUMERIC_CLASSIFICATION__C = 1 THEN 'Yes' ELSE 'No' END AS "HIGH_INCOME"
    , DH.EARLIEST_DONATION_DATE
    , DH.MOST_RECENT_DONATION_DATE
    , DH.TOTAL_DONATION_AMOUNT
FROM Account_Clean_Email C
    LEFT JOIN PRODUCTION.MART_NPC_METADATA.CANCER_RESEARCH_CAPACITY CR --this is for country income category (note: Sudan and South Sudan have same country code and can create dupes)
        ON C.PREFERRED_COUNTRY_CODE = CR.COUNTRY_CODE_CORRECTED
    LEFT JOIN PRODUCTION.REPL_NPC.ACCOUNT COMP --self join for company/org info
        ON C."Company__c" = COMP."Id"
        AND c."PersonEmailBouncedDate" is null and c."Membership_Status__c" <> 'Deceased' and c."Do_Not_Email__c" = false
    LEFT JOIN DONATION_HISTORY DH
        ON C."Id" = DH."DonorId";

        

CALL PRODUCTION.MART_DATACLOUD.grant_select_dc_objects();







//OLD DEFINITION OF MASTER SUBSCRIBER
with Account_Clean_Email AS (
    SELECT
        *,
        CASE
          WHEN "Preferred_Address__c" ILIKE '%Business%' THEN "ShippingCountry"
          WHEN "Preferred_Address__c" ILIKE '%Mailing%'  THEN "PersonMailingCountry"
          WHEN NULLIF(TRIM("ShippingCountry"), '') IS NOT NULL THEN "ShippingCountry"
          WHEN NULLIF(TRIM("PersonMailingCountry"), '') IS NOT NULL THEN "PersonMailingCountry"
          ELSE 'United States'
        END AS PREFERRED_COUNTRY,
        CASE
          WHEN "Preferred_Address__c" ILIKE '%Business%' THEN "ShippingCountryCode"
          WHEN "Preferred_Address__c" ILIKE '%Mailing%'  THEN "PersonMailingCountryCode"
          WHEN NULLIF(TRIM("ShippingCountry"), '') IS NOT NULL THEN "ShippingCountryCode"
          WHEN NULLIF(TRIM("PersonMailingCountry"), '') IS NOT NULL THEN "PersonMailingCountryCode"
          ELSE 'US'
        END AS PREFERRED_COUNTRY_CODE,
        REGEXP_REPLACE("PersonEmail", '\\.invalid$', '') AS "Clean_PersonEmail",
        REGEXP_REPLACE("Work_Email__c", '\\.invalid$', '') AS "Clean_WorkEmail",
        REGEXP_REPLACE("Personal_Email__c", '\\.invalid$', '') AS "Clean_PersonalEmail"
    FROM PRODUCTION.REPL_NPC.ACCOUNT
    WHERE "RecordTypeId" = '012Hp000002JUc1IAG'
)
SELECT 
    CAST(u."PersonId" AS VARCHAR) as "Golden_Record_Id"
    , c."FirstName" as "First_Name"
    , c."LastName" as "Last_Name"
    , c."Salutation"
    , c."Preferred_Email_Type__c"
    , case 
        when c."Preferred_Email_Type__c" = 'Work' and c."Clean_WorkEmail" is not null then c."Clean_WorkEmail"
        when c."Preferred_Email_Type__c" = 'Personal' and c."Clean_PersonalEmail" is not null then c."Clean_PersonalEmail"
        else c."Clean_PersonEmail"
        end as "Preferred_Email_Address"
    , TRUE as "Email_Is_Primary"
    , c."Membership_Type__c" as "Membership_Type"
    , c."Membership_Status__c" as "Membership_Status"
    , c."Initial_Join_Date__c" as "Member_Join_Date"
    , c."Paid_Thru_Date__c" as "Member_Paid_Thru_Date"
    , c."Expected_Completion_Date__c" 
    , COALESCE(NULLIF(c."AACR_ID__c", ''), NULLIF(c."AACRAccountID__c", ''), c."AACR_Id_Autonumber__c") as "Member_Number"
    , TRUE as "Primary_Membership"
    , c."Do_Not_Solicit__c" as "Do_Not_Solicit"
    , FALSE as "Do_Not_Contact"
    , c."PersonMailingCity" as "City"
    , c."PersonMailingState" as "State"
    , CR.LABEL AS "Country"
    , c."Professional_Role__c" as "Professional_Role"
    , c."Work_Setting__c" as "Work_Setting"
    , c."Highest_Degree__c" as "Highest_Degree"
    , c."Current_Education_Status__c" as "Current_Education_Status"
    , c."Pre_Post_Doc__c" as "Pre_Post_Doc"
    , c."Foundation_Only__c" as "Foundation_Only"
    , comp."Name" as "Organization_Name"
    , comp."Institution_Type__c" as "Organization_Industry"
    , c."PersonLeadSource" as "Source"
    , c."Fonteva_Contact_ID__c" as "Fonteva_Id"
    , CR.REGION__C
    , CR.NUMERIC_CLASSIFICATION__C
    , CASE WHEN CR.NUMERIC_CLASSIFICATION__C = 1 THEN 'Yes' ELSE 'No' END AS "HIGH_INCOME"
FROM Account_Clean_Email C
    join IDR.RESULTS.EMAIL u
        on u."Email" = c."Clean_PersonEmail"
        OR u."Email" = c."Clean_WorkEmail"
        OR u."Email" = c."Clean_PersonalEmail"
    LEFT JOIN PRODUCTION.MART_NPC_METADATA.CANCER_RESEARCH_CAPACITY CR
        ON C.PREFERRED_COUNTRY_CODE = CR.COUNTRY_CODE_CORRECTED
    LEFT JOIN PRODUCTION.REPL_NPC.ACCOUNT COMP --self join for company/org info
        ON C."Company__c" = COMP."Id"
        AND c."PersonEmailBouncedDate" is null and c."Membership_Status__c" <> 'Deceased' and c."Do_Not_Email__c" = false;