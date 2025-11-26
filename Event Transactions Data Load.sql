SELECT 
    CONCAT(CON."FirstName", ' ', CON."LastName", ' ', EV."Name", ' ', MR.ORDER_TOTAL) AS "Name",
    MR.PARTICIPATION_ID,
    ACC."Id" AS "DonorId",
    'Events' AS "Revenue_Line__c",
    '012Vq000001uvXiIAI' AS "RecordTypeId",
    EV."Name" AS "Description",
    MR.ORDER_TOTAL AS "OriginalAmount",
    REC."OrderApi__Payment_Type__c" AS "PaymentMethod",
    'Individual' AS "GiftType",
    --Event Booking,
    SO."OrderApi__Closed_Date__c" AS "TransactionDate",
    'Paid' AS "Status",
    'Don\'t Send' AS "TaxReceiptStatus",
    'Don\'t Send' AS "AcknowledgementStatus",
    GL."Name" AS "GL_NAME",
    GL."Id" AS GL_ID,
    TL."OrderApi__Credit__c" AS "TL_CREDIT",
    TL."OrderApi__Debit__c" AS "TL_DEBIT",
    TL."Id" AS "TL_ID",
    EP.*
FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.CONTACT AS CON
        ON CON."Id" = MR.CONTACT_ID
    LEFT JOIN PRODUCTION.REPL_NPC.ACCOUNT AS ACC
        ON CON."Id" = ACC."Legacy_ID__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.BR_EVENT__C AS EV
        ON MR.EVENT_ID = EV."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO 
        ON MR.SALES_ORDER_ID = SO."Id"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS REC
        ON SO."Id" = REC."OrderApi__Sales_Order__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C AS TR
        ON REC."Id" = TR."OrderApi__Receipt__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION_LINE__C AS TL
        ON TR."Id" = TL."OrderApi__Transaction__c"
    LEFT JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__GL_ACCOUNT__C AS GL
        ON TL."OrderApi__GL_Account__c" = GL."Id"
    LEFT JOIN PRODUCTION.REPL_NPC.ACEM__EVENT_PARTICIPATION__C EP
        ON MR.PARTICIPATION_ID = EP."Legacy_Participation__c"
WHERE TR."OrderApi__Date__c" >= '2020-01-01'
ORDER BY SO."Id", GL."Name";




//Event booking

SELECT * FROM PRODUCTION.REPL_NPC.ACEM__EVENT_TRANSACTION__C

/*
Name, 
acem__Transaction_Amount__c, 
acem__Payer__c (participation name), 
acem__Payment_Status__c (check list of values in snowflake), 
acem__Payment_Method__c, 
acem__Event_Name__c, 
acem__Registrant_First_Name__c, 
acem__Registrant_Last_Name__c, 
acem__Registrant_Email__c, 
CreatedById, 
OwnerId, 
LastModifiedById

Transaction ID will require some transformation ... 
 
select ID, PAYMENT_INTENT from stripe.stripe.charges WHERE PAYMENT_INTENT = 'pi_3MgUDyFI5AsNuGeu0SZjmC4y'
 
Fonteva used Stripe Payment Intent ID, NPC-AC appears to use transaction ID which can be found based on the query above 
 
Sample above from this record
https://aacr.lightning.force.com/lightning/r/OrderApi__Receipt__c/a1C8W000009i8rfUAA/view


-Name 
(Migrated)
-Legacy id
-Invoice number


*/


//Gift transaction designation
--need everything besides uniqueid

--gl from fonteva, map to 

SELECT * FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY




//journal entries







SELECT * FROM PRODUCTION.REPL_NPC.ACEM__EVENT_PARTICIPATION__C



SELECT * FROM PRODUCTION.REPL_NPC.ACEM__EVENT_PARTICIPATION__C

SELECT * FROM PRODUCTION.REPL_NPC.GIFTDESIGNATION


SELECT * FROM PRODUCTION.REPL_NPC.GIFTTRANSACTION

SELECT DISTINCT MR.POSTING_STATUS FROM PRODUCTION.MART_MEETING_REGISTRATION.MEETING_REGISTRATIONS_BACKUP AS MR


SELECT REC."Id", TR."Id" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__RECEIPT__C AS REC
    JOIN PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__TRANSACTION__C TR
        ON REC."Id" = TR."OrderApi__Receipt__c"