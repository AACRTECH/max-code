select distinct t."Id" TransactionId, t."OrderApi__Contact__c" Contact_Id__c, t."GP_Batch_Number__c", t."OrderApi__Date__c" TransactionDate, t."OrderApi__Sales_Order__c", r."OrderApi__Payment_Type__c", so."OrderApi__Status__c", so."OrderApi__Is_Posted__c", (select sum("OrderApi__Credit__c") from orderapi__transaction_line__c tl where tl."OrderApi__Transaction__c" = t."Id") OriginalAmount, SO."OrderApi__Status__c" AS SALES_ORDER_STATUS, SO."OrderApi__Posting_Status__c" AS SALES_ORDER_POSTED, i."Name" from orderapi__transaction__c t
left outer join orderapi__sales_order__c so on t."OrderApi__Sales_Order__c" = so."Id"
left outer join orderapi__receipt__c r on t."OrderApi__Receipt__c" = r."Id"
join orderapi__transaction_line__c tl on tl."OrderApi__Transaction__c" = t."Id"
join orderapi__item__c i on tl."OrderApi__Item__c" = i."Id"
join orderapi__item_class__c ic on ic."Id" = i."OrderApi__Item_Class__c"
left outer join Contact c on t."OrderApi__Contact__c" = c."Id"
left outer join Account a on c."AccountId" = a."Id"
where tl."OrderApi__Credit__c" > 0  and ic."OrderApi__Is_Event__c" = false and  t."OrderApi__Receipt_Type__c" <> 'Refund' and a."Name" not like '%AACR Test%' And (ic."Name" LIKE '%Member%' OR ic."Name" = 'Internal Staff Use Only');




select distinct t."Id" TransactionId, t."OrderApi__Contact__c" Contact_Id__c, t."GP_Batch_Number__c", t."OrderApi__Date__c" TransactionDate, t."OrderApi__Sales_Order__c", r."OrderApi__Payment_Type__c", so."OrderApi__Status__c", so."OrderApi__Is_Posted__c", 
(select sum("OrderApi__Credit__c") from orderapi__transaction_line__c tl where tl."OrderApi__Transaction__c" = t."Id") 
OriginalAmount, SO."OrderApi__Status__c" AS SALES_ORDER_STATUS, SO."OrderApi__Posting_Status__c" AS SALES_ORDER_POSTED,
o."npsp__Acknowledgment_Date__c" AcknowledgementDate, o."npsp__Acknowledgment_Status__c" AcknowledgementStatus, o."CampaignId" CampaignId, o."ddrive__DonorDrive_Recurring_Profile_ID__c" DD_Recurring_Profile_ID__c, o."Description" Description, o."GL_Account__c" Distribution_Code__c, o."npsp__Notification_Message__c" Donation_Message__c, o."npsp__Tribute_Type__c" DonorDive_Tribute_Notification__c, o."npsp__Honoree_Name__c" DonorDrive_Honor_Name__c, o."npsp__Tribute_Type__c" DonorDrive_Honor_or_Memory__c, o."ddrive__DonorDrive_Matching_Employer__c" DonorDrive_Matching_Employer__c, o."npsp__Honoree_Name__c" DonorDrive_Memory_Name__c, o."ddrive__Ticket_Type__c" DonorDrive_Ticket_Type__c, o."npsp__Notification_Recipient_Name__c" DonorDrive_Tribute_Recipient_Name__c, o."npsp__Notification_Recipient_Information__c" DonorDrive_Tribute_Recipient_Street__c, o."FiscalYear" Fiscal_Year__c, o."ddrive__DD_Original_Processing_Fee_Amount__c" Gift_Fees__c, o."npsp__In_Kind_Description__c" In_Kind_Description__c, o."npsp__Matching_Gift__c" MatchingEmployerTransactionId, o."npsp__Fair_Market_Value__c" NonTaxDeductibleAmount, o."ddrive__DD_Original_Processing_Fee_Amount__c" ProcessorTransactionFee, o."ddrive__Sum_Tax_Deduction__c" TaxDeductionAmount, o."ddrive__Donation_Receipt_Date__c" TaxReceiptStatus, o."ddrive__Donation_Receipt_Date__c" Tax_Receipt_Date__c, o."c4g_Appeal_Code__c" Legacy_Appeal_Code__c, o."ddrive__DonorDrive_Gift_Description__c" Legacy_DD_Description__c, o."ddrive__Donation_Receipt_ID__c" Legacy_DD_Receipt_ID__C, o."ddrive__Donation_Transaction_ID__c" Legacy_DonorDrive_Transaction_ID__c, o."AccountId" Legacy_Opportunity_Account__c, o."ContactId" Legacy_Opportunity_Contact__c, r."OrderApi__Gateway_Transaction_Id__c" GatewayReference, r."Id" Legacy_Receipt_ID__c, r."OrderApi__Reference_Number__c" PaymentIdentifier, r."OrderApi__Payment_Type__c" PaymentMethod, r."OrderApi__Total__c" OriginalAmount, so."Id" Legacy_Sales_Order_ID__c, so."OrderApi__Status__c" Status, t."Microsoft_Dynamic_ID__c" MS_Dynamics_ID__c, t."Id" Transaction_Legacy_ID__c, t."OrderApi__Date__c" TransactionDate, inv."OrderApi__Due_Date__c" TransactionDueDate, r."OrderApi__Is_Posted__c"
from orderapi__transaction__c t
left outer join orderapi__sales_order__c so on t."OrderApi__Sales_Order__c" = so."Id"
left outer join OPPORTUNITY o on so."Opportunity__c" = o."Id"
left outer join orderapi__receipt__c r on t."OrderApi__Receipt__c" = r."Id"
left outer join orderapi__invoice__c inv on t."OrderApi__Invoice__c" = inv."Id"
join orderapi__transaction_line__c tl on tl."OrderApi__Transaction__c" = t."Id"
join orderapi__item__c i on tl."OrderApi__Item__c" = i."Id"
join orderapi__item_class__c ic on ic."Id" = i."OrderApi__Item_Class__c"
join Contact c on t."OrderApi__Contact__c" = c."Id"
join Account a on c."AccountId" = a."Id"
where (tl."OrderApi__Credit__c" - tl."OrderApi__Debit__c" > 0) and ic."OrderApi__Is_Event__c" = false and  t."OrderApi__Receipt_Type__c" <> 'Refund' and a."Name" not like '%AACR Test%'
and t."Id" NOT IN (SELECT "Legacy_Transaction_ID__c" from ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_GIFT_TRANSACTION)
;




SELECT SO."Opportunity__c" FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.ORDERAPI__SALES_ORDER__C AS SO;


SELECT *
FROM PRODUCTION.REPL_SALESFORCE_OWNBACKUP.OPPORTUNITY AS O