
-- EXEC bronze.load_data;

-----------------------------------------------------------
-- STORED PROCEDURE: bronze.load_data
-- PURPOSE:
--   Load all raw CSV files into the Bronze Layer.
--   This layer stores raw, uncleaned, untyped data exactly
--   as received — following the Medallion Architecture.
--
-- WHY BRONZE?
--   - No datatype casting → full fidelity
--   - Capture raw errors for quality checks
--   - Ensures reproducibility of pipeline
-----------------------------------------------------------

create or alter procedure bronze.load_data as

BEGIN
    -------------------------------------------------------
    -- APPLICATION DATA (train + test)
    -------------------------------------------------------
    -- Truncate: Keeps table structure but removes all data.
    -- Used instead of DROP to avoid losing permissions & metadata.
    -------------------------------------------------------

    truncate table bronze.Application; 

    -------------------------------------------------------
    -- Load application_train.csv
    -- BULK INSERT is used for fast loading of large CSV files.
    -- KEY POINTS:
    --   FIRSTROW = 2 → skip header
    --   FIELDQUOTE = '"' → handle quoted fields
    --   CODEPAGE = 65001 → UTF-8 (avoids Arabic/Unicode issues)
    --   ROWTERMINATOR = 0x0a → Linux newline for Kaggle dataset
    -------------------------------------------------------
    BULK INSERT bronze.Application
    FROM 'C:\bbbbbulk\application_train.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -- Load application_test.csv (same schema)
    BULK INSERT bronze.Application
    FROM 'C:\bbbbbulk\application_test.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -------------------------------------------------------
    -- BUREAU TABLE
    -------------------------------------------------------

    truncate table bronze.bureau; 
    BULK INSERT bronze.bureau
    FROM 'C:\bbbbbulk\bureau.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );
    -------------------------------------------------------
    -- BUREAU BALANCE TABLE
    -------------------------------------------------------

    truncate table bronze.bureau_balance; 
    BULK INSERT bronze.bureau_balance
    FROM 'C:\bbbbbulk\bureau_balance.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -------------------------------------------------------
    -- CREDIT CARD BALANCE TABLE
    -------------------------------------------------------

    truncate table bronze.credit_card_balance; 
    BULK INSERT bronze.credit_card_balance
    FROM 'C:\bbbbbulk\credit_card_balance.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -------------------------------------------------------
    -- INSTALLMENTS PAYMENTS TABLE
    -------------------------------------------------------

    truncate table bronze.installments_payments; 
    BULK INSERT bronze.installments_payments
    FROM 'C:\bbbbbulk\installments_payments.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -------------------------------------------------------
    -- POS_CASH BALANCE TABLE
    -------------------------------------------------------

    truncate table bronze.POS_CASH_balance; 
    BULK INSERT bronze.POS_CASH_balance
    FROM 'C:\bbbbbulk\POS_CASH_balance.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

    -------------------------------------------------------
    -- PREVIOUS APPLICATION TABLE
    -------------------------------------------------------

    truncate table bronze.previous_application; 
    BULK INSERT bronze.previous_application
    FROM 'C:\bbbbbulk\previous_application.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDQUOTE='"',
        ROWTERMINATOR='0x0a',
        CODEPAGE='65001',
        TABLOCK
    );

END


