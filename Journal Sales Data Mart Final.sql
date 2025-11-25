/*OLD*/

--create or replace view PRODUCTION.MART_JOURNAL_SALES.DEMOS_BACKUP
--as
WITH FINAL_YEAR_TERM_WITH_DETAILS AS (
    SELECT a.*, VALUE::INT as "YEAR"
	FROM REPL_SALESFORCE.Subscription_Terms AS a
    JOIN LATERAL FLATTEN(ARRAY_GENERATE_RANGE(EXTRACT(year FROM a."TERM_START_DATE"), EXTRACT(year FROM a."TERM_END_DATE") + 1))
)
SELECT 
	TERM.Id AS "TERM_ID"
	, TERM."YEAR" AS "TERM_YEAR"
	, TERM.Src_Created_Date AS "TERM_CREATED_DATE"
	, TERM.Subscription_Id AS "SUBSCRIPTION" 
	, TERM.Sales_Order_Id AS "TERM_SALES_ORDER_ID"
	, TERM.Sales_Order_Line_Id AS "SALES_ORDER_LINE_ID"
	, TERM.Subscription_Plan_Id AS "SUBSCRIPTION_PLAN_ID"
	, CON.Name AS "DEMO_CONTACT_NAME"
	, CON.Id AS "CONTACT_ID"
	, IT."Name" AS "DEMO_ITEM_NAME"
	, ITC."Name" AS "ITEM_CLASS_NAME"
	, CONCAT(TERM."ID", TERM."YEAR") AS "Composite_Key_Demo"
	, CONCAT(CON.Id, TERM."YEAR") AS "Composite_Contact"
	, SUB.Primary_Item_Id AS "SUBSCRIPTION_ITEM_ID"
FROM FINAL_YEAR_TERM_WITH_DETAILS AS TERM
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON TERM.Sales_Order_Id = SO."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL
        ON TERM.Sales_Order_Line_Id = SOL."Id"
	LEFT JOIN REPL_SALESFORCE.Subscriptions AS SUB
		ON TERM.Subscription_Id = SUB.Id
	LEFT JOIN REPL_SALESFORCE.Contacts AS CON
		ON SUB.Contact_Id = CON.Id
	LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
		ON SUB.Primary_Item_Id = IT."Id"
	LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
		ON SUB.Item_Class_Id = ITC."Id"
WHERE "ITEM_CLASS_NAME" LIKE '%Journal%';

create or replace view PRODUCTION.MART_JOURNAL_SALES.TRANSACTION_LINE_MERGE_BACKUP
AS
SELECT 
    TL."Id" AS "TRANSACTION_LINE_RECORD_ID"
    , TL."Name" AS "TRANSACTION_LINE_ID"
    , TL."OrderApi__Transaction__c" AS "TRANSACTION_LINE_TRANSACTION_ID"
    , TL."OrderApi__Credit__c" AS "TRANSACTION_LINE_CREDIT"
    , TL."OrderApi__Debit__c" AS "TRANSACTION_LINE_DEBIT"
    , TL."OrderApi__GL_Account__c" AS "TRANSACTION_LINE_GL_ACCOUNT"
    , TL."OrderApi__Item_Class__c" AS "TRANSACTION_LINE_ITEM_CLASS"
    , TL."OrderApi__Memo__c" AS "TRANSACTION_LINE_MEMO"
    , TL."OrderApi__Receipt_Line__c" AS "TRANSACTION_LINE_RECEIPT_LINE"
    , TL."OrderApi__Receipt__c" AS "TRANSACTION_LINE_RECEIPT_ID"
    , TL."OrderApi__Sales_Order_Line__c" AS "TRANSACTION_LINE_SALES_ORDER_LINE"
    , TL."OrderApi__Subscription_Line__c" AS "TRANSACTION_LINE_SUBSCRIPTION_LINE"
    , TL."OrderApi__Subscription_Plan__c" AS "TRANSACTION_LINE_SUBSCRIPTION_PLAN"
    , TR."Id" AS "TRANSACTION_ID"
    , TR."Name" AS "TRANSACTION_NUMBER"
    , TR."OrderApi__Contact__c" AS "TRANSACTION_CONTACT_ID"
    , TR."OrderApi__Date__c" AS "TRANSACTION_DATE"
    , TR."OrderApi__Memo__c" AS "TRANSACTION_MEMO"
    , TR."OrderApi__Receipt__c" AS "TRANSACTION_RECEIPT_ID"
    , TR."OrderApi__Sales_Order__c" AS "TRANSACTION_SALES_ORDER_ID"
    , TR."OrderApi__Subscription__c" AS "TRANSACTION_SUBSCRIPTION_ID"
    , CON.Name AS "CONTACT_NAME"
    , GL.Description AS "GL_DESCRIPTION"
    , IFF(GL.Description LIKE '%Def%', YEAR(SO."CreatedDate") + 1, YEAR(SO."CreatedDate")) AS "MEMBERSHIP_TRANSACTION_YEAR"
    , SO."CreatedDate" AS "SALES_ORDER_CREATED_DATE"
    , Itemn."Name" AS "ITEM_NAME"
    , RC."Id" AS "RECEIPT_ID"
    , SO."Id" AS "SALES_ORDER_ID"
    , RC."OrderApi__Type__c" AS "RECEIPT_TYPE"
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
WHERE ACC."NAME" NOT LIKE '%AACR TEST%' --Might need to expand filter criteria here, include "%Test%"
    AND IC."Name" LIKE '%Journal%';


create or replace view PRODUCTION.MART_JOURNAL_SALES.JOURNAL_SALES_BACKUP
AS
SELECT a.*, b.*
FROM MART_JOURNAL_SALES.Demos_Backup AS a
LEFT JOIN MART_JOURNAL_SALES.Transaction_Line_Merge_Backup AS b
    ON a."TERM_SALES_ORDER_ID" = b."SALES_ORDER_ID"
    AND a."TERM_YEAR" = b."MEMBERSHIP_TRANSACTION_YEAR"
    AND a."DEMO_ITEM_NAME" = b."ITEM_NAME"
WHERE "SALES_ORDER_ID" NOT IN (
    SELECT "SALES_ORDER_ID"
    FROM MART_JOURNAL_SALES.Transaction_Line_Merge_Backup
    WHERE "SALES_ORDER_ID" IS NOT NULL AND "RECEIPT_TYPE" LIKE '%Refund%'
);



/* intermediate */
create or replace view PRODUCTION.MART_JOURNAL_SALES.DEMOS_BACKUP
as
WITH FINAL_YEAR_TERM_WITH_DETAILS AS (
    SELECT a.*, VALUE::INT as "YEAR"
	FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS a
    JOIN LATERAL FLATTEN(ARRAY_GENERATE_RANGE(EXTRACT(year FROM a."OrderApi__Term_Start_Date__c"), EXTRACT(year FROM a."OrderApi__Term_End_Date__c") + 1))
)
SELECT 
	TERM."Id" AS "TERM ID"
	, TERM."YEAR" AS "TERM YEAR"
	, TERM."CreatedDate" AS "TERM CREATED DATE"
    , CONVERT_TIMEZONE('UTC', 'America/New_York', TERM."CreatedDate") AS "TERM CREATED DATE Fixed"
	, TERM."OrderApi__Subscription__c" AS "SUBSCRIPTION" 
	, TERM."OrderApi__Sales_Order__c" AS "SALES ORDER"
	, TERM."OrderApi__Sales_Order_Line__c"  AS "SALES ORDER LINE"
	, TERM."OrderApi__Subscription_Plan__c" AS "SUBSCRIPTION PLAN"
	, CON."Name" AS "CONTACT NAME"
	, CON."Id" AS "SALESFORCE ID"
	, IT."Name" AS "ITEM NAME"
	, ITC."Name" AS "ITEM CLASS NAME"
	, CONCAT(TERM."Id", TERM."YEAR") AS "Composite Key Demo"
	, CONCAT(CON."Id", TERM."YEAR") AS "Composite Contact"
	, SUB."OrderApi__Item__c" AS "Subscription Item ID"
FROM
	FINAL_YEAR_TERM_WITH_DETAILS AS TERM
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON TERM."OrderApi__Sales_Order__c" = SO."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL
        ON TERM."OrderApi__Sales_Order_Line__c" = SOL."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
        ON TERM."OrderApi__Subscription__c" = SUB."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON SUB."OrderApi__Contact__c" = CON."Id" 
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
        ON SUB."OrderApi__Item__c" = IT."Id"
    LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
        ON SUB."OrderApi__Item_Class__c" = ITC."Id"
    WHERE ITC."Name" LIKE '%Journal%';

create or replace view PRODUCTION.MART_JOURNAL_SALES.TRANSACTION_LINE_MERGE_BACKUP
as
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
    , CON."Name" AS "Contact Name"
    , GL."Name" AS "GL Name"
    , IFF(GL."Name" LIKE '2500%' OR GL."Name" LIKE '2517%', YEAR(SO."CreatedDate") + 1, YEAR(SO."CreatedDate")) AS "MEMBERSHIP TRANSACTION YEAR"
    , IFF(GL."Name" LIKE '2500%' OR GL."Name" LIKE '2517%', YEAR(CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate")) + 1, YEAR(CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate"))) AS "MEMBERSHIP TRANSACTION YEAR Fixed"
    , SO."CreatedDate" AS "SALES ORDER CREATED DATE"
    , CONVERT_TIMEZONE('UTC', 'America/New_York', SO."CreatedDate") AS "SALES ORDER CREATED DATE Fixed"
    , Itemn."Name" AS "Item Name"
    , RC."Id" AS "Receipt ID"
    , RC."OrderApi__Type__c" AS "RECEIPT_TYPE"
    , SO."Id" AS "SALES ORDER ID"
    , SO."OrderApi__Description__c" AS "SALES_ORDER_DESCRIPTION"
    , SC."Name" AS "SOURCE_CODE"
FROM REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
    ON TL."OrderApi__Transaction__c" = TR."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C AS GL
    ON TL."OrderApi__GL_Account__c" = GL."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
    ON TR."OrderApi__Contact__c" = CON."Id"
LEFT JOIN REPL_SALESFORCE_OWNBACKUP.ACCOUNT AS ACC
    ON CON."AccountId" = ACC."Id"
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
WHERE IC."Name" LIKE '%Journal%' AND ACC."Name" NOT LIKE '%AACR TEST%';


create or replace view PRODUCTION.MART_JOURNAL_SALES.JOURNAL_SALES_BACKUP
as
SELECT a.*, b.*
FROM MART_JOURNAL_SALES.Demos_Backup AS a
LEFT JOIN MART_JOURNAL_SALES.Transaction_Line_Merge_Backup AS b
    ON a."SALES ORDER" = b."SALES ORDER ID"
    AND a."TERM YEAR" = b."MEMBERSHIP TRANSACTION YEAR Fixed"
    AND a."ITEM NAME" = b."Item Name"
WHERE "SALES ORDER ID" NOT IN (
    SELECT "SALES ORDER ID"
    FROM MART_JOURNAL_SALES.Transaction_Line_Merge_Backup
    WHERE "SALES ORDER ID" IS NOT NULL AND "RECEIPT_TYPE" LIKE '%Refund%'
);




/* FINAL */


create or replace view PRODUCTION.MART_JOURNAL_SALES.DEMOS_BACKUP
as
WITH FINAL_YEAR_TERM_WITH_DETAILS AS (
    SELECT a.*, VALUE::INT as "YEAR"
	FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS a
    JOIN LATERAL FLATTEN(ARRAY_GENERATE_RANGE(EXTRACT(year FROM a."OrderApi__Term_Start_Date__c"), 
        EXTRACT(year FROM a."OrderApi__Term_End_Date__c") + 1))
)
SELECT 
	TERM."Id" AS "TERM_ID"
	, TERM."YEAR" AS "TERM_YEAR"
	, TERM."CreatedDate" AS "TERM_CREATED_DATE"
	, TERM."OrderApi__Subscription__c" AS "SUBSCRIPTION" 
	, TERM."OrderApi__Sales_Order__c" AS "TERM_SALES_ORDER_ID"
	, TERM."OrderApi__Sales_Order_Line__c" AS "SALES_ORDER_LINE_ID"
	, TERM."OrderApi__Subscription_Plan__c" AS "SUBSCRIPTION_PLAN_ID"
	, CON."Name" AS "DEMO_CONTACT_NAME"
	, CON."Id" AS "CONTACT_ID"
	, IT."Name" AS "DEMO_ITEM_NAME"
	, ITC."Name" AS "ITEM_CLASS_NAME"
	, CONCAT(TERM."Id", TERM."YEAR") AS "Composite_Key_Demo"
	, CONCAT(CON."Id", TERM."YEAR") AS "Composite_Contact"
	, SUB."OrderApi__Item__c" AS "SUBSCRIPTION_ITEM_ID"
FROM FINAL_YEAR_TERM_WITH_DETAILS AS TERM
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO
        ON TERM."OrderApi__Sales_Order__c" = SO."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER_LINE__C AS SOL
        ON TERM."OrderApi__Sales_Order_Line__c" = SOL."Id"
	LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SUBSCRIPTION__C AS SUB
		ON TERM."OrderApi__Subscription__c" = SUB."Id"
	LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
		ON SUB."OrderApi__Contact__c" = CON."Id"
	LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM__C AS IT
		ON SUB."OrderApi__Item__c" = IT."Id"
	LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__ITEM_CLASS__C AS ITC
		ON SUB."OrderApi__Item_Class__c" = ITC."Id"
WHERE "ITEM_CLASS_NAME" LIKE '%Journal%';


SELECT A."OrderApi__Subscription_Plan__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RENEWAL__C AS A;


create or replace view PRODUCTION.MART_JOURNAL_SALES.TRANSACTION_LINE_MERGE_BACKUP
AS
SELECT 
    TL."Id" AS "TRANSACTION_LINE_RECORD_ID"
    , TL."Name" AS "TRANSACTION_LINE_ID"
    , TL."OrderApi__Transaction__c" AS "TRANSACTION_LINE_TRANSACTION_ID"
    , TL."OrderApi__Credit__c" AS "TRANSACTION_LINE_CREDIT"
    , TL."OrderApi__Debit__c" AS "TRANSACTION_LINE_DEBIT"
    , TL."OrderApi__GL_Account__c" AS "TRANSACTION_LINE_GL_ACCOUNT"
    , TL."OrderApi__Item_Class__c" AS "TRANSACTION_LINE_ITEM_CLASS"
    , TL."OrderApi__Memo__c" AS "TRANSACTION_LINE_MEMO"
    , TL."OrderApi__Receipt_Line__c" AS "TRANSACTION_LINE_RECEIPT_LINE"
    , TL."OrderApi__Receipt__c" AS "TRANSACTION_LINE_RECEIPT_ID"
    , TL."OrderApi__Sales_Order_Line__c" AS "TRANSACTION_LINE_SALES_ORDER_LINE"
    , TL."OrderApi__Subscription_Line__c" AS "TRANSACTION_LINE_SUBSCRIPTION_LINE"
    , TL."OrderApi__Subscription_Plan__c" AS "TRANSACTION_LINE_SUBSCRIPTION_PLAN"
    , TR."Id" AS "TRANSACTION_ID"
    , TR."Name" AS "TRANSACTION_NUMBER"
    , TR."OrderApi__Contact__c" AS "TRANSACTION_CONTACT_ID"
    , TR."OrderApi__Date__c" AS "TRANSACTION_DATE"
    , TR."OrderApi__Memo__c" AS "TRANSACTION_MEMO"
    , TR."OrderApi__Receipt__c" AS "TRANSACTION_RECEIPT_ID"
    , TR."OrderApi__Sales_Order__c" AS "TRANSACTION_SALES_ORDER_ID"
    , TR."OrderApi__Subscription__c" AS "TRANSACTION_SUBSCRIPTION_ID"
    , CON."Name" AS "CONTACT_NAME"
    , GL."Name" AS "GL_DESCRIPTION"
    , IFF(GL."Name" LIKE '%Def%', YEAR(SO."CreatedDate") + 1, YEAR(SO."CreatedDate")) AS "MEMBERSHIP_TRANSACTION_YEAR"
    , SO."CreatedDate" AS "SALES_ORDER_CREATED_DATE"
    , Itemn."Name" AS "ITEM_NAME"
    , RC."Id" AS "RECEIPT_ID"
    , SO."Id" AS "SALES_ORDER_ID"
    , RC."OrderApi__Type__c" AS "RECEIPT_TYPE"
    , SO."OrderApi__Description__c" AS "SALES_ORDER_DESCRIPTION"
    , SC."Name" AS "SOURCE_CODE"
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
WHERE ACC."Name" NOT LIKE '%AACR TEST%' --Might need to expand filter criteria here, include "%Test%"
    AND IC."Name" LIKE '%Journal%';


create or replace view PRODUCTION.MART_JOURNAL_SALES.JOURNAL_SALES_BACKUP
AS
SELECT a.*, b.*
FROM PRODUCTION.MART_JOURNAL_SALES.DEMOS_BACKUP AS a
LEFT JOIN PRODUCTION.MART_JOURNAL_SALES.TRANSACTION_LINE_MERGE_BACKUP AS b
    ON a."TERM_SALES_ORDER_ID" = b."SALES_ORDER_ID"
    AND a."TERM_YEAR" = b."MEMBERSHIP_TRANSACTION_YEAR"
    AND a."DEMO_ITEM_NAME" = b."ITEM_NAME"
WHERE "SALES_ORDER_ID" NOT IN (
    SELECT "SALES_ORDER_ID"
    FROM PRODUCTION.MART_JOURNAL_SALES.TRANSACTION_LINE_MERGE_BACKUP
    WHERE "SALES_ORDER_ID" IS NOT NULL AND "RECEIPT_TYPE" LIKE '%Refund%'
);
