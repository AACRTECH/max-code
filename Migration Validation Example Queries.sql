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




SELECT 
    A."Initial_Join_Date__c",
    B."Initial_Join_Date__c",
    A."Id",
    B."Fonteva_Contact_ID__c"
FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.FONTEVA_CONTACT AS A
    JOIN ATTAIN_MIGRATION.MIGRATION_TO_NPC.STG_ACCOUNT AS B
        ON A."Id" = B."Fonteva_Contact_ID__c"
WHERE A."Initial_Join_Date__c"::VARCHAR <> B."Initial_Join_Date__c"::VARCHAR;








SELECT * FROM ATTAIN_MIGRATION.MIGRATION_TO_NPC.IDCROSSWALK;