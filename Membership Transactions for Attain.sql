CREATE OR REPLACE VIEW TEST.MART_MEMBERSHIP_HISTORY.TRANSACTION_LINE_MERGE_BACKUP
AS
SELECT 
    TL."Id" AS "Transaction Line ID"
    , TL."Name" AS "Transaction Line Name"
    , TL."OrderApi__Transaction__c" AS "Transaction Line C"
    , TL."OrderApi__Credit__c" AS ORIGINAL_AMOUNT
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
    , TR."GP_Batch_Number__c"
    , CON."Name" AS "Contact Name"
    , GL."Name" AS "GL Name"
    , IFF(GL."Name" LIKE '2500%' OR GL."Name" LIKE '2517%', YEAR(SO."CreatedDate") + 1, YEAR(SO."CreatedDate")) AS "MEMBERSHIP TRANSACTION YEAR"
    , IFF(GL."Name" LIKE '2500%' OR GL."Name" LIKE '2517%', YEAR(CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate")) + 1, YEAR(CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate"))) AS "MEMBERSHIP TRANSACTION YEAR Fixed"
    , SO."CreatedDate" AS "SALES ORDER CREATED DATE"
    , CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate") AS "SALES ORDER CREATED DATE Fixed"
    , Itemn."Name" AS "Item Name"
    , RC."Id" AS "Receipt ID"
    , SO."Id" AS "SALES ORDER ID"
    , SO."OrderApi__Description__c" AS "SALES_ORDER_DESCRIPTION"
    , SO."OrderApi__Status__c" AS SALES_ORDER_STATUS
    , RC."OrderApi__Payment_Type__c" AS "RECEIPT_PAYMENT_TYPE"
    , SO."OrderApi__Posting_Status__c" AS SALES_ORDER_POSTED
    , SO.
    , SC."Name" AS "SOURCE_CODE"
    , IC."OrderApi__Is_Event__c"
    , TR."OrderApi__Receipt_Type__c"
    , Itemn."Name" AS ITEM_NAME
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    ON TL."OrderApi__Transaction__c" = TR."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C AS GL
    ON TL."OrderApi__GL_Account__c" = GL."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    ON TR."OrderApi__Contact__c" = CON."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
    ON CON."AccountId" = ACC."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
    ON TL."OrderApi__Item_Class__c" = IC."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS RC
    ON TL."OrderApi__Receipt__c" = RC."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
    ON RC."OrderApi__Sales_Order__c" = SO."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
    ON TL."OrderApi__Item__c" = Itemn."Id"
LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SOURCE_CODE__C AS SC
    ON SO."OrderApi__Source_Code__c" = SC."Id"
WHERE IC."Name" IN ('Individual Memberships', 'Internal Staff Use Only') AND ACC."Name" NOT LIKE '%AACR TEST%';


SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C A WHERE "Is_Contribution__c" = true;



create or replace view TEST.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP
as
SELECT a.*, b.*
FROM TEST.MART_MEMBERSHIP_HISTORY.Demos_Backup AS a
LEFT JOIN TEST.MART_MEMBERSHIP_HISTORY.Transaction_Line_Merge_Backup AS b
    ON a."SALES ORDER" = b."SALES ORDER ID"
    AND a."TERM YEAR" = b."MEMBERSHIP TRANSACTION YEAR Fixed"
    AND a."ITEM NAME" = b."Item Name"
ORDER BY "TERM CREATED DATE Fixed" DESC;

/*************************/
/*     Create Schema     */
/*************************/
CREATE SCHEMA TEST.MEMBERSHIP_MIGRATION_TABLES;

/*************************/
/* MEMBERSHIP_PUSH_MERGE */
/*************************/
CREATE OR REPLACE VIEW TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_PUSH_MERGE
AS
with MEM_HISTORY AS (
    SELECT
        MH.*,
        TERM."OrderApi__Term_Start_Date__c" AS TERM_START_DATE,
        TERM."OrderApi__Term_End_Date__c" AS TERM_END_DATE,
        TERM."OrderApi__Renewed_Date__c" AS TERM_RENEWED_DATE,
        TERM."OrderApi__Grace_Period_End_Date__c" AS TERM_GRACE_PERIOD_END_DATE,
        SUB."OrderApi__Status__c" AS SUBSCRIPTION_STATUS,
        SUB."OrderApi__Item__c" AS ITEM_ID,
        SUB."OrderApi__Item_Class__c" AS ITEM_CLASS_ID,
        CASE
            WHEN CONC."iMIS_ID__c" IS NULL THEN CONC."Salesforce_ID__c"
            ELSE CONC."iMIS_ID__c"
        END AS AACR_ID
    FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL AS MH
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CONC
            ON MH."SALESFORCE ID" = CONC."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
            ON MH."SUBSCRIPTION" = SUB."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS TERM
            ON MH."TERM ID" = TERM."Id"
)
SELECT
    MMH.*,
    AP.MEMBER_TYPE AS MEM_TYPE_AT_PUSH,
    AP.PUSH_YEAR,
    CASE WHEN AP.AACR_ID IS NOT NULL THEN 1 ELSE 0 END AS PUSH_FLAG,
    CONCAT(AP.AACR_ID, '-', AP.PUSH_YEAR, '-', AP.MEMBER_TYPE) AS PUSH_KEY
FROM MEM_HISTORY AS MMH
    LEFT JOIN TEST.MEMBERSHIP_PUSHES.ALL_PUSHES AS AP
        ON MMH.AACR_ID = AP.AACR_ID
        AND MMH."TERM YEAR" = AP.PUSH_YEAR;


/*************************/
/*MEMBERSHIP_TRANSACTIONS*/
/*************************/
CREATE OR REPLACE VIEW TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS
AS
SELECT
    "Composite Key Demo"
    , "Transaction Line ID"
    , "Transaction Line Name"
    , "Transaction Line C"
    , ORIGINAL_AMOUNT
    , "Transaction Line Credit"
    , "Transaction Line Debit"
    , "Transaction Line GL Account"
    , "Transaction Line Item Class"
    , "Tranaction Line Memo"
    , "Transaction Line Receipt Line"
    , "Transaction Line Receipt"
    , "Transaction Line SO Line"
    , "Transaction Line Subscription Line"
    , "Transaction Line Subscription Plan"
    , "Transaction Id"
    , "Transaction Name"
    , "Transaction Contact Id"
    , "Transaction Date"
    , "Transaction Memo"
    , "Transaction Receipt"
    , "Transaction Sales Order"
    , "Transaction Subscription"
    , "GP_Batch_Number__c"
    , "Contact Name"
    , "GL Name"
    , "MEMBERSHIP TRANSACTION YEAR"
    , "MEMBERSHIP TRANSACTION YEAR Fixed"
    , "SALES ORDER CREATED DATE"
    , "SALES ORDER CREATED DATE Fixed"
    , "Item Name"
    , "Receipt ID"
    , "SALES ORDER ID"
    , "SALES_ORDER_DESCRIPTION"
    , SALES_ORDER_STATUS
    , "RECEIPT_PAYMENT_TYPE"
    , SALES_ORDER_POSTED
    , "SOURCE_CODE"
    , "OrderApi__Is_Event__c"
    , "OrderApi__Receipt_Type__c"
    , ITEM_NAME
FROM TEST.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP;




SELECT COUNT(*), RECEIPT_PAYMENT_TYPE FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS GROUP BY 2;


SELECT COUNT(*) FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_DATA_TERM_BACKUP;

SELECT COUNT(*) FROM PRODUCTION.MART_MEMBERSHIP_HISTORY.MEMBERSHIP_HISTORY_FINAL;








with Lines AS (
    SELECT 
        TL."OrderApi__Transaction__c",
        SUM(TL."OrderApi__Credit__c") AS TOTAL_ORIGINAL_AMOUNT
    FROM 
        PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
            ON TL."OrderApi__Item_Class__c" = IC."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
            ON TL."OrderApi__Item__c" = Itemn."Id"
    WHERE IC."Name" IN ('Individual Memberships', 'Internal Staff Use Only')
    AND (tl."OrderApi__Credit__c" - tl."OrderApi__Debit__c" > 0) and ic."OrderApi__Is_Event__c" = false
    GROUP BY 1
),
Items AS (
    SELECT A."Transaction Id", A."Item Name" FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A GROUP BY 1, 2
),
finals as (
SELECT
    TR.*,
    Lines.TOTAL_ORIGINAL_AMOUNT,
    Items."Item Name",
    SO."OrderApi__Is_Posted__c",
    SO."OrderApi__Status__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    JOIN Lines
        ON TR."Id" = Lines."OrderApi__Transaction__c"
    LEFT JOIN Items 
        ON TR."Id" = Items."Transaction Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON TR."OrderApi__Sales_Order__c" = SO."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON TR."OrderApi__Contact__c" = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
WHERE ACC."Name" NOT LIKE '%AACR TEST%' AND TR."OrderApi__Receipt_Type__c" <> 'Refund'
ORDER BY TR."Id")
SELECT SUM(TOTAL_ORIGINAL_AMOUNT) FROM finals;




SELECT  
        DISTINCT TR."Name",
        SUM(TL."OrderApi__Credit__c") AS TOTAL_ORIGINAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
        JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
            ON TR."Id" = TL."OrderApi__Transaction__c"
        JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
            ON TL."OrderApi__Item_Class__c" = IC."Id"
        JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
            ON TL."OrderApi__Item__c" = Itemn."Id"
    WHERE Itemn."Name" LIKE '%Member%' OR IC."Name" LIKE '%Journal%'
    AND (tl."OrderApi__Credit__c" - tl."OrderApi__Debit__c" > 0) and ic."OrderApi__Is_Event__c" = false
    GROUP BY 1;



with Lines AS (
    SELECT 
        TR."Id",
        SUM(TL."OrderApi__Credit__c") AS TOTAL_ORIGINAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
        JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
            ON TR."Id" = TL."OrderApi__Transaction__c"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS IC
            ON TL."OrderApi__Item_Class__c" = IC."Id"
        LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS Itemn
            ON TL."OrderApi__Item__c" = Itemn."Id"
    WHERE Itemn."Name" LIKE '%Member%'
    AND (tl."OrderApi__Credit__c" - tl."OrderApi__Debit__c" > 0) and ic."OrderApi__Is_Event__c" = false
    GROUP BY 1
),
Items AS (
    SELECT A."Transaction Id", A."Item Name" FROM TEST.MEMBERSHIP_MIGRATION_TABLES.MEMBERSHIP_TRANSACTIONS AS A GROUP BY 1, 2
),
finals as (
SELECT
    TR.*,
    Lines.TOTAL_ORIGINAL_AMOUNT,
    Items."Item Name",
    SO."OrderApi__Is_Posted__c",
    SO."OrderApi__Status__c"
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    JOIN Lines
        ON TR."Id" = Lines."Id"
    LEFT JOIN Items 
        ON TR."Id" = Items."Transaction Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON TR."OrderApi__Sales_Order__c" = SO."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON TR."OrderApi__Contact__c" = CON."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
        ON CON."AccountId" = ACC."Id"
WHERE ACC."Name" NOT LIKE '%AACR TEST%' AND TR."OrderApi__Receipt_Type__c" <> 'Refund'
ORDER BY TR."Id")
SELECT SUM(TOTAL_ORIGINAL_AMOUNT) FROM finals;


SELECT  
        DISTINCT TR."Name",
        SUM(TL."OrderApi__Credit__c") AS TOTAL_ORIGINAL_AMOUNT
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
        JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
            ON TR."Id" = TL."OrderApi__Transaction__c"
        
GROUP BY 1;

SELECT  
        *
    FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR;








SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C;