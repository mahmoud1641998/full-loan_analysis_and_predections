
-----------------------------------------------------------
-- SILVER LAYER CREATION SCRIPT
-- PURPOSE:
--   Transform and clean raw Bronze tables into Silver layer.
--   Silver layer contains:
--     - Corrected datatypes
--     - Standardized categorical values
--     - Replaced missing/abnormal values
--     - Derived features (flags, scores, etc.)
--   This is step 2 in Medallion Architecture:
--     Bronze → Raw
--     Silver → Cleaned / Trusted
-----------------------------------------------------------

-----------------------------------------------------------
-- APPLICATION TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Convert numeric columns to correct types
--   - Replace abnormal values (e.g., 365243 → 0)
--   - Standardize categorical columns
--   - Create binary flags
--   - Prepare features for ML modeling
-----------------------------------------------------------
IF OBJECT_ID('silver.application_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.application_cleaned;

    SELECT
        ---------------------------------------
        -- Identifiers       2 &&&  2
        ---------------------------------------
        CAST(trim(SK_ID_CURR) AS INT)                       AS Human_C,
        TRY_CAST(trim(TARGET) AS INT)                           AS Target,
        CAST(
            CASE 
                WHEN TARGET is null THEN 1 
                ELSE 0 
            END
        AS bit) AS Is_Test ,

        ---------------------------------------
        -- Numeric columns (clean abnormal missing values)   4 &&& 6
        ---------------------------------------
        TRY_CAST(trim(AMT_INCOME_TOTAL) AS Float)           AS Total_Income,
        TRY_CAST(trim(AMT_CREDIT) AS Float)                 AS loan_amount,
        TRY_CAST(trim(AMT_ANNUITY) AS Float)                AS Loan_Installment_Amount,
        TRY_CAST(trim(AMT_GOODS_PRICE) AS Float)            AS Item_Price ,
        ---------------------------------------
        -- DAYS variables (replace 365243 with ZERO)   5 &&&  11
        ---------------------------------------
     
        CAST(
            CASE 
                WHEN TRIM(DAYS_EMPLOYED) = '365243' THEN 0 
                ELSE TRY_CAST(TRIM(DAYS_EMPLOYED) AS FLOAT)
            END
        AS FLOAT) AS Employment_Duration_Days,
        
       TRY_CAST(trim(DAYS_REGISTRATION) AS Float)  AS Days_Since_Registration_Update,
       TRY_CAST(trim(DAYS_ID_PUBLISH) AS Float)  AS Days_Since_ID_Update,
       TRY_CAST(trim(DAYS_BIRTH) AS Float)   AS Age_Days ,

       TRY_CAST(trim(DAYS_LAST_PHONE_CHANGE) AS Float)   AS Days_Since_Last_Phone_Change,

    --------------------------------------- 
    -- Categorical columns (Standardized)     10 &&&  21   
    ---------------------------------------

    UPPER(TRIM(NAME_CONTRACT_TYPE))               AS Loan_Type ,
    NULLIF(UPPER(TRIM(CODE_GENDER))   ,'XNA')     AS GENDER  ,
    CASE 
        WHEN trim(FLAG_OWN_CAR) = 'Y' THEN 1
        ELSE 0
    END AS Has_Car  ,
    
    CASE 
        WHEN trim(FLAG_OWN_REALTY) = 'Y' THEN 1
        ELSE 0
    END AS Has_House,
    UPPER(TRIM(NAME_EDUCATION_TYPE))              AS Education_Type,
    
    NULLIF(UPPER(TRIM(NAME_FAMILY_STATUS)),'UNKNOWN')               AS Marital_Status,
    
    UPPER(TRIM(NAME_INCOME_TYPE))                 AS Income_Type,
    UPPER(TRIM(OCCUPATION_TYPE))                  AS Job_Type,
    UPPER(TRIM(NAME_HOUSING_TYPE))                AS Housing_Type,
    UPPER(TRIM(WEEKDAY_APPR_PROCESS_START))       AS Weekday_Application ,

    ---------------------------------------
    -- Binary / Flags columns        6    &&&   27
    ---------------------------------------

    TRY_CAST(FLAG_MOBIL AS INT)                       AS Has_Mobile,
    TRY_CAST(FLAG_WORK_PHONE AS INT)                  AS Has_Work_Phone,
    TRY_CAST(FLAG_PHONE AS INT)                       AS Has_Phone,
    TRY_CAST(FLAG_EMAIL AS INT)                       AS Has_Email,
    TRY_CAST(FLAG_EMP_PHONE AS INT)                   AS Has_Emp_Phone,
    TRY_CAST(FLAG_CONT_MOBILE AS INT)                 AS Can_Contacting,
    ---------------------------------------
    -- External sources            3  &&&  30
    ---------------------------------------
    round(TRY_cast(trim(EXT_SOURCE_1) as float),2) as ESource_Scores_1,
    round(TRY_cast(trim(EXT_SOURCE_2) as float),2) as ESource_Scores_2,
    round(TRY_cast(trim(EXT_SOURCE_3) as float),2) as ESource_Scores_3,
    ---------------------------------------
    -- Children / Family / Region     5 &&&  35
    ---------------------------------------
   
    TRY_CAST(CNT_CHILDREN AS float) AS Num_Children,
    TRY_CAST(CNT_FAM_MEMBERS AS float) AS Family_Size,
    round(TRY_CAST(REGION_POPULATION_RELATIVE AS FLOAT),4) AS Region_Population_Ratio,
    TRY_CAST(REGION_RATING_CLIENT AS float) AS Region_Rating_Client,
    TRY_CAST(REGION_RATING_CLIENT_W_CITY AS float) AS Region_Rating_Client_WCity,
    ---------------------------------------
    -- Application Timing            8  &&&  43
    ---------------------------------------
   
    TRY_CAST(HOUR_APPR_PROCESS_START AS INT) AS App_Process_Hour,
    TRY_CAST(REG_REGION_NOT_LIVE_REGION AS INT) AS Reg_Not_Live_Region,
    TRY_CAST(REG_REGION_NOT_WORK_REGION AS INT) AS Reg_Not_Work_Region,
    TRY_CAST(LIVE_REGION_NOT_WORK_REGION AS INT) AS Live_Not_Work_Region,
    TRY_CAST(REG_CITY_NOT_LIVE_CITY AS INT) AS Reg_City_Not_Live,
    TRY_CAST(REG_CITY_NOT_WORK_CITY AS INT) AS Reg_City_Not_Work,
    TRY_CAST(LIVE_CITY_NOT_WORK_CITY AS INT) AS Live_City_Not_Work,
    nullif (UPPER(TRIM(ORGANIZATION_TYPE)),'XNA') AS Organization_Type,
    
    ---------------------------------------
    -- Apartment / House Scores       14 &&&  57
    ---------------------------------------
    
    round(TRY_CAST(APARTMENTS_AVG AS FLOAT),3) AS Apartment_Area_Score,
    round(TRY_CAST(BASEMENTAREA_AVG AS FLOAT),3) AS Basement_Area_Score,
    round(TRY_CAST(YEARS_BEGINEXPLUATATION_AVG AS FLOAT),3) AS Building_Year_Built_Score,
    round(TRY_CAST(YEARS_BUILD_AVG AS FLOAT),3) AS Building_Age_Score,
    round(TRY_CAST(COMMONAREA_AVG AS FLOAT),3) AS Common_Area_Score,
    round(TRY_CAST(ELEVATORS_AVG AS FLOAT),3) AS Elevator_Count_Score,
    round(TRY_CAST(ENTRANCES_AVG AS FLOAT),3) AS Building_Entrances_Score,
    round(TRY_CAST(FLOORSMAX_AVG AS FLOAT),3) AS MaxFloors_Score,
    round(TRY_CAST(FLOORSMIN_AVG AS FLOAT),3) AS MinFloors_Score,
    round(TRY_CAST(LANDAREA_AVG AS FLOAT),3) AS LandArea_Score,
    round(TRY_CAST(LIVINGAPARTMENTS_AVG AS FLOAT),3) AS LivingApts_Score,
    round(TRY_CAST(LIVINGAREA_AVG AS FLOAT),3) AS LivingArea_Score,
    round(TRY_CAST(NONLIVINGAPARTMENTS_AVG AS FLOAT),3) AS NonLivingApts_Score,
    round(TRY_CAST(NONLIVINGAREA_AVG AS FLOAT),3) AS NonLivingArea_Score  ,
    ---------------------------------------
    -- Apartment / House Median Scores       14 &&&  71
    ---------------------------------------

    round(TRY_CAST(APARTMENTS_MEDI AS FLOAT),3) AS Apartments_Median_Score,
    round(TRY_CAST(BASEMENTAREA_MEDI AS FLOAT),3) AS BasementArea_Median_Score,
    round(TRY_CAST(YEARS_BEGINEXPLUATATION_MEDI AS FLOAT),3) AS Building_Year_Built_Median_Scor,
    round(TRY_CAST(YEARS_BUILD_MEDI AS FLOAT),3) AS Build_Age_Median_Score,
    round(TRY_CAST(COMMONAREA_MEDI AS FLOAT),3) AS CommonArea_Median_Score,
    round(TRY_CAST(ELEVATORS_MEDI AS FLOAT),3) AS Elevators_Median_Score,
    round(TRY_CAST(ENTRANCES_MEDI AS FLOAT),3) AS Entrances_Median_Score,
    round(TRY_CAST(FLOORSMAX_MEDI AS FLOAT),3) AS MaxFloors_Median_Score,
    round(TRY_CAST(FLOORSMIN_MEDI AS FLOAT),3) AS MinFloors_Median_Score,
    round(TRY_CAST(LANDAREA_MEDI AS FLOAT),3) AS LandArea_Median_Score,
    round(TRY_CAST(LIVINGAPARTMENTS_MEDI AS FLOAT),3) AS LivingApts_Median_Score,
    round(TRY_CAST(LIVINGAREA_MEDI AS FLOAT),3) AS LivingArea_Median_Score,
    round(TRY_CAST(NONLIVINGAPARTMENTS_MEDI AS FLOAT),3) AS NonLivingApts_Median_Score,
    round(TRY_CAST(NONLIVINGAREA_MEDI AS FLOAT),3) AS NonLivingArea_Median_Score,

        ---------------------------------------
    -- Apartment / House Mode Scores       14 &&&  85
    ---------------------------------------
    
    round(TRY_CAST(APARTMENTS_MODE AS FLOAT),3) AS Apartments_Mode_Score,
    round(TRY_CAST(BASEMENTAREA_MODE AS FLOAT),3) AS BasementArea_Mode_Score,
    round(TRY_CAST(YEARS_BEGINEXPLUATATION_MODE AS FLOAT),3) AS Building_Year_Built_Mode_Scor,
    round(TRY_CAST(YEARS_BUILD_MODE AS FLOAT),3) AS Build_Age_Mode_Score,
    round(TRY_CAST(COMMONAREA_MODE AS FLOAT),3) AS CommonArea_Mode_Score,
    round(TRY_CAST(ELEVATORS_MODE AS FLOAT),3) AS Elevators_Mode_Score,
    round(TRY_CAST(ENTRANCES_MODE AS FLOAT),3) AS Entrances_Mode_Score,
    round(TRY_CAST(FLOORSMAX_MODE AS FLOAT),3) AS MaxFloors_Mode_Score,
    round(TRY_CAST(FLOORSMIN_MODE AS FLOAT),3) AS MinFloors_Mode_Score,
    round(TRY_CAST(LANDAREA_MODE AS FLOAT),3) AS LandArea_Mode_Score,
    round(TRY_CAST(LIVINGAPARTMENTS_MODE AS FLOAT),3) AS LivingApts_Mode_Score,
    round(TRY_CAST(LIVINGAREA_MODE AS FLOAT),3) AS LivingArea_Mode_Score,
    round(TRY_CAST(NONLIVINGAPARTMENTS_MODE AS FLOAT),3) AS NonLivingApts_Mode_Score,
    round(TRY_CAST(NONLIVINGAREA_MODE AS FLOAT),3) AS NonLivingArea_Mode_Score ,


    ---------------------------------------
    -- Apartment / House info       5 &&&  90
    ---------------------------------------
    
    CASE 
        WHEN FONDKAPREMONT_MODE IS NULL or FONDKAPREMONT_MODE = 'not specified'THEN 'NoInfo'
        WHEN FONDKAPREMONT_MODE = 'org spec account' THEN 'OrgSpecialAccount'
        WHEN FONDKAPREMONT_MODE = 'reg oper account' THEN 'RegionalOperAccount'
        WHEN FONDKAPREMONT_MODE = 'reg oper spec account' THEN 'RegionalSpecialAccount'
        ELSE 'Other'
    END AS RepairFundType,
    UPPER(TRIM(HOUSETYPE_MODE)) AS HouseType_Mode,
    round(TRY_CAST(TOTALAREA_MODE AS FLOAT),3) AS TotalArea_Mode_Score,
    UPPER(TRIM(WALLSMATERIAL_MODE)) AS WallsMaterial_Mode,
    UPPER(TRIM(EMERGENCYSTATE_MODE)) AS Has_Emergency,
    ---------------------------------------
    -- Social Circle / Documents     24 &&&  114  yes
    ---------------------------------------

    TRY_CAST(OBS_30_CNT_SOCIAL_CIRCLE AS float) AS Obs_30_Cnt_Social,
    TRY_CAST(DEF_30_CNT_SOCIAL_CIRCLE AS float) AS Def_30_Cnt_Social,
    TRY_CAST(OBS_60_CNT_SOCIAL_CIRCLE AS float) AS Obs_60_Cnt_Social,
    TRY_CAST(DEF_60_CNT_SOCIAL_CIRCLE AS float) AS Def_60_Cnt_Social,

    TRY_CAST(FLAG_DOCUMENT_2 AS INT) AS Has_Document_2,
    TRY_CAST(FLAG_DOCUMENT_3 AS INT) AS Has_Document_3,
    TRY_CAST(FLAG_DOCUMENT_4 AS INT) AS Has_Document_4,
    TRY_CAST(FLAG_DOCUMENT_5 AS INT) AS Has_Document_5,
    TRY_CAST(FLAG_DOCUMENT_6 AS INT) AS Has_Document_6,
    CAST(FLAG_DOCUMENT_7 AS INT) AS Has_Document_7,
    CAST(FLAG_DOCUMENT_8 AS INT) AS Has_Document_8,
    CAST(FLAG_DOCUMENT_9 AS INT) AS Has_Document_9,
    CAST(FLAG_DOCUMENT_10 AS INT) AS Has_Document_10,
    CAST(FLAG_DOCUMENT_11 AS INT) AS Has_Document_11,
    CAST(FLAG_DOCUMENT_12 AS INT) AS Has_Document_12,
    CAST(FLAG_DOCUMENT_13 AS INT) AS Has_Document_13,
    CAST(FLAG_DOCUMENT_14 AS INT) AS Has_Document_14,
    CAST(FLAG_DOCUMENT_15 AS INT) AS Has_Document_15,
    CAST(FLAG_DOCUMENT_16 AS INT) AS Has_Document_16,
    CAST(FLAG_DOCUMENT_17 AS INT) AS Has_Document_17,
    CAST(FLAG_DOCUMENT_18 AS INT) AS Has_Document_18,
    CAST(FLAG_DOCUMENT_19 AS INT) AS Has_Document_19,
    CAST(FLAG_DOCUMENT_20 AS INT) AS Has_Document_20,    
    CAST(FLAG_DOCUMENT_21 AS INT) AS Has_Document_21,

    ---------------------------------------
    -- Credit Last Requests     6 &&&  120  yes
    ---------------------------------------
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_HOUR AS FLOAT) AS countReq_Last_Hour,
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_DAY AS FLOAT) AS countReq_Last_Day,
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_WEEK AS FLOAT) AS countReq_Last_Week,
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_MON AS FLOAT) AS countReq_Last_Month,
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_QRT AS FLOAT) AS countReq_Last_Quarter,
    TRY_CAST(AMT_REQ_CREDIT_BUREAU_YEAR AS FLOAT) AS countReq_Last_Year ,

    ----
    UPPER(TRIM(NAME_TYPE_SUITE)) AS TypeSuite ,
    TRY_CAST(OWN_CAR_AGE AS FLOAT) AS Car_Age_Years   


INTO silver.application_cleaned
FROM bronze.application;

-------------------------
-- Add Primary Key
-------------------------

ALTER TABLE silver.application_cleaned
ALTER COLUMN Human_C INT NOT NULL;

ALTER TABLE silver.application_cleaned
ADD CONSTRAINT PK_application_cleaned PRIMARY KEY (Human_C);

--------------------------------
--------------------------------


-----------------------------------------------------------
-- BUREAU TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Correct numeric types
--   - Replace abnormal DAYS values
--   - Uppercase categorical values
--   - Add PK on Bureau_ID
-----------------------------------------------------------
IF OBJECT_ID('silver.bureau_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.bureau_cleaned;

SELECT
    ---------------------------------------
    -- Identifiers
    ---------------------------------------
    CAST(SK_ID_BUREAU AS int) AS Bureau_ID,
    CAST(SK_ID_CURR AS int) AS Human_C,
 
    ---------------------------------------
    -- Categorical columns
    ---------------------------------------
    UPPER(TRIM(CREDIT_ACTIVE)) AS Credit_Active_Status,
    UPPER(TRIM(CREDIT_CURRENCY)) AS Credit_Currency,
    UPPER(TRIM(CREDIT_TYPE)) AS Credit_Type ,

    ---------------------------------------
    -- DAYS variables (replace 365243 with 0)
    ---------------------------------------
   
    CAST(
        CASE 
            WHEN DAYS_CREDIT = '365243' THEN 0
            ELSE DAYS_CREDIT
        END AS FLOAT) * -1 AS Days_Credit_Start ,
    
    CAST(
        CASE
            WHEN CREDIT_DAY_OVERDUE IS NULL THEN 0
            ELSE CREDIT_DAY_OVERDUE
        END AS FLOAT) AS Days_Credit_Overdue ,

    CAST(DAYS_CREDIT_ENDDATE AS FLOAT) AS Days_Credit_End ,
   
    CAST(DAYS_ENDDATE_FACT AS FLOAT) AS Days_Credit_End_Fact,

    CAST(DAYS_CREDIT_UPDATE AS FLOAT) * -1 AS Days_Credit_Update ,

    ---------------------------------------
    -- Numeric / Amounts
    ---------------------------------------
    CAST(AMT_CREDIT_MAX_OVERDUE AS FLOAT) AS Credit_Max_Overdue,
    CAST(CNT_CREDIT_PROLONG AS FLOAT) AS Num_Prolongations,
    CAST(AMT_CREDIT_SUM AS FLOAT) AS Credit_Sum,
    CAST(AMT_CREDIT_SUM_DEBT AS FLOAT) AS Credit_Sum_Debt,
    CAST(AMT_CREDIT_SUM_LIMIT AS FLOAT) AS Credit_Sum_Limit,
    CAST(AMT_CREDIT_SUM_OVERDUE AS FLOAT) AS Credit_Sum_Overdue,
    CAST(AMT_ANNUITY AS FLOAT) AS Annuity_Amount

INTO silver.bureau_cleaned
FROM bronze.bureau;

---------------------
---------------------


-- Primary Key
ALTER TABLE silver.bureau_cleaned
ALTER COLUMN Bureau_ID INT NOT NULL;

ALTER TABLE silver.bureau_cleaned
ADD CONSTRAINT PK_Bureau_ID PRIMARY KEY (Bureau_ID);




---------------
---------------


-----------------------------------------------------------
-- BUREAU_BALANCE TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Correct types
--   - Uppercase categorical columns
--   - Used for bureau time-series analysis
-----------------------------------------------------------
IF OBJECT_ID('silver.bureau_balance_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.bureau_balance_cleaned;

SELECT
    ---------------------------------------
    -- Identifiers
    ---------------------------------------
    CAST(SK_ID_BUREAU AS INT) AS Bureau_ID,
    CAST(MONTHS_BALANCE AS INT) AS Month_Index,

    ---------------------------------------
    -- Categorical columns
    ---------------------------------------
    UPPER(TRIM(STATUS)) AS Status

    
INTO silver.bureau_balance_cleaned
FROM bronze.bureau_balance;


-------------------------------
-------------------------------

-----------------------------------------------------------
-- PREVIOUS APPLICATION TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Convert numeric & DAYS columns
--   - Uppercase categorical values
--   - Create PK on Prev_Application_ID
-----------------------------------------------------------

IF OBJECT_ID('silver.previous_application_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.previous_application_cleaned;

SELECT
    -------------------------------------------------------
    -- Identifiers
    -------------------------------------------------------
    CAST(TRIM(SK_ID_PREV) AS INT)  AS Prev_Application_ID,
    CAST(TRIM(SK_ID_CURR) AS INT)  AS Human_C,

    -------------------------------------------------------
    -- Loan Details
    -------------------------------------------------------
    UPPER(TRIM(NAME_CONTRACT_TYPE))         AS Loan_Type,
    CAST(TRIM(AMT_ANNUITY) AS FLOAT)        AS Annuity_Amount,
    CAST(TRIM(AMT_APPLICATION) AS FLOAT)    AS Application_Amount,
    CAST(TRIM(AMT_CREDIT) AS FLOAT)         AS Credit_Amount,
    CAST(TRIM(AMT_DOWN_PAYMENT) AS FLOAT)   AS Down_Payment,
    CAST(TRIM(AMT_GOODS_PRICE) AS FLOAT)    AS Goods_Price,

    UPPER(TRIM(WEEKDAY_APPR_PROCESS_START)) AS Application_Weekday,
    CAST(TRIM(HOUR_APPR_PROCESS_START) AS INT) AS Application_Hour,

    TRIM(FLAG_LAST_APPL_PER_CONTRACT)   AS Last_Application_Per_Contract,
    CAST(TRIM(NFLAG_LAST_APPL_IN_DAY) AS INT)       AS Last_Application_In_Day_Flag,

    CAST(TRIM(RATE_DOWN_PAYMENT) AS FLOAT)          AS Rate_Down_Payment,
    CAST(TRIM(RATE_INTEREST_PRIMARY) AS FLOAT)      AS Interest_Rate_Primary,
    CAST(TRIM(RATE_INTEREST_PRIVILEGED) AS FLOAT)   AS Interest_Rate_Privileged ,

    -------------------------------------------------------
    -- Purpose / Status / Types
    -------------------------------------------------------
    UPPER(TRIM(NAME_CASH_LOAN_PURPOSE))   AS Cash_Loan_Purpose,
    UPPER(TRIM(NAME_CONTRACT_STATUS))     AS Contract_Status,
    UPPER(TRIM(NAME_PAYMENT_TYPE))        AS Payment_Type,
    UPPER(TRIM(CODE_REJECT_REASON))       AS Reject_Reason,
    UPPER(TRIM(NAME_TYPE_SUITE))          AS Type_Suite,
    UPPER(TRIM(NAME_CLIENT_TYPE))         AS Client_Type,
    UPPER(TRIM(NAME_GOODS_CATEGORY))      AS Goods_Category,
    UPPER(TRIM(NAME_PORTFOLIO))           AS Portfolio,
    UPPER(TRIM(NAME_PRODUCT_TYPE))        AS Product_Type,
    UPPER(TRIM(CHANNEL_TYPE))             AS Channel_Type,
    UPPER(TRIM(SELLERPLACE_AREA))         AS Seller_Place_Area,
    UPPER(TRIM(NAME_SELLER_INDUSTRY))     AS Seller_Industry,
    UPPER(TRIM(NAME_YIELD_GROUP))         AS Yield_Group,
    UPPER(TRIM(PRODUCT_COMBINATION))      AS Product_Combination,

    -------------------------------------------------------
    -- CNT columns
    -------------------------------------------------------
    CAST(TRIM(CNT_PAYMENT) AS float) AS Total_Payment_Count,

    -------------------------------------------------------
    -- DAYS columns (apply 365243 → 0 then * -1)
    -------------------------------------------------------
    CAST(
        CASE WHEN TRIM(DAYS_DECISION) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_DECISION) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_Since_Decision,

    CAST(
        CASE WHEN TRIM(DAYS_FIRST_DRAWING) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_FIRST_DRAWING) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_First_Drawing,

    CAST(
        CASE WHEN TRIM(DAYS_FIRST_DUE) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_FIRST_DUE) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_First_Due,

    CAST(
        CASE WHEN TRIM(DAYS_LAST_DUE_1ST_VERSION) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_LAST_DUE_1ST_VERSION) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_Last_Due_First_Version,

    CAST(
        CASE WHEN TRIM(DAYS_LAST_DUE) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_LAST_DUE) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_Last_Due,

    CAST(
        CASE WHEN TRIM(DAYS_TERMINATION) = '365243' 
             THEN 0 ELSE TRY_CAST(TRIM(DAYS_TERMINATION) AS FLOAT) 
        END * -1 AS FLOAT
    ) AS Days_Termination,

    -------------------------------------------------------
    -- Flags
    -------------------------------------------------------
    CAST(TRIM(NFLAG_INSURED_ON_APPROVAL) AS float) AS Insured_On_Approval_Flag

INTO silver.previous_application_cleaned
FROM bronze.previous_application;

-------------------------
-- Add Primary Key
-------------------------

ALTER TABLE silver.previous_application_cleaned
ALTER COLUMN Prev_Application_ID INT NOT NULL;

ALTER TABLE silver.previous_application_cleaned
ADD CONSTRAINT PK_previous_application_cleaned PRIMARY KEY (Prev_Application_ID);



-----------
-----------
-----------
-----------------------------------------------------------
-- CREDIT CARD BALANCE TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Convert numeric columns
--   - Uppercase categorical columns
--   - Create composite PK: Prev_Application_ID + Months_Balance
-----------------------------------------------------------

IF OBJECT_ID('silver.credit_card_balance_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.credit_card_balance_cleaned;

SELECT
    -------------------------------------------------------
    -- Identifiers
    -------------------------------------------------------
    CAST(TRIM(SK_ID_PREV) AS INT)  AS Prev_Application_ID,
    CAST(TRIM(SK_ID_CURR) AS INT)  AS Human_C,
    CAST(TRIM(MONTHS_BALANCE) AS INT) AS Months_Balance,

    -------------------------------------------------------
    -- Balance / Credit Info
    -------------------------------------------------------
    CAST(TRIM(AMT_BALANCE) AS FLOAT)                   AS Balance_Amount,
    CAST(TRIM(AMT_CREDIT_LIMIT_ACTUAL) AS FLOAT)       AS Credit_Limit_Actual,
    CAST(TRIM(AMT_DRAWINGS_ATM_CURRENT) AS FLOAT)      AS Drawings_ATM_Current,
    CAST(TRIM(AMT_DRAWINGS_CURRENT) AS FLOAT)          AS Drawings_Current,
    CAST(TRIM(AMT_DRAWINGS_OTHER_CURRENT) AS FLOAT)    AS Drawings_Other_Current,
    CAST(TRIM(AMT_DRAWINGS_POS_CURRENT) AS FLOAT)      AS Drawings_POS_Current,
    CAST(TRIM(AMT_INST_MIN_REGULARITY) AS FLOAT)       AS Installment_Min_Regularity,
    CAST(TRIM(AMT_PAYMENT_CURRENT) AS FLOAT)           AS Payment_Current,
    CAST(TRIM(AMT_PAYMENT_TOTAL_CURRENT) AS FLOAT)     AS Payment_Total_Current,
    CAST(TRIM(AMT_RECEIVABLE_PRINCIPAL) AS FLOAT)      AS Receivable_Principal,
    CAST(TRIM(AMT_RECIVABLE) AS FLOAT)                 AS Receivable_Amount,
    CAST(TRIM(AMT_TOTAL_RECEIVABLE) AS FLOAT)          AS Total_Receivable,

    -------------------------------------------------------
    -- Counts
    -------------------------------------------------------
    CAST(TRIM(CNT_DRAWINGS_ATM_CURRENT) AS FLOAT)        AS Count_Drawings_ATM_Current,
    CAST(TRIM(CNT_DRAWINGS_CURRENT) AS FLOAT)            AS Count_Drawings_Current,
    CAST(TRIM(CNT_DRAWINGS_OTHER_CURRENT) AS FLOAT)      AS Count_Drawings_Other_Current,
    CAST(TRIM(CNT_DRAWINGS_POS_CURRENT) AS FLOAT)        AS Count_Drawings_POS_Current,
    CAST(TRIM(CNT_INSTALMENT_MATURE_CUM) AS FLOAT)       AS Count_Installment_Mature_Cum,

    -------------------------------------------------------
    -- Contract Status
    -------------------------------------------------------
    UPPER(TRIM(NAME_CONTRACT_STATUS)) AS Contract_Status,

    -------------------------------------------------------
    -- Overdue Stats
    -------------------------------------------------------
    CAST(TRIM(SK_DPD) AS FLOAT)       AS Days_Past_Due,
    CAST(TRIM(SK_DPD_DEF) AS FLOAT)   AS Days_Past_Due_Default

INTO silver.credit_card_balance_cleaned
FROM bronze.credit_card_balance;

-------------------------
-- Add the Composit Primary Key
-------------------------
ALTER TABLE silver.credit_card_balance_cleaned
Alter column Prev_Application_ID INT NOT NULL;

ALTER TABLE silver.credit_card_balance_cleaned
Alter column MONTHS_BALANCE INT NOT NULL;


ALTER TABLE silver.credit_card_balance_cleaned
ADD CONSTRAINT PK_creditcard_prev_month
PRIMARY KEY (Prev_Application_ID, MONTHS_BALANCE);



---------------
---------------
---------------

-----------------------------------------------------------
-- POS_CASH_BALANCE TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Convert numeric columns
--   - Uppercase categorical columns
--   - Create composite PK: Prev_Application_ID + Months_Balance
-----------------------------------------------------------
IF OBJECT_ID('silver.pos_cash_balance_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.pos_cash_balance_cleaned;

SELECT
    -----------------------------------------------------
    -- Identifiers
    -----------------------------------------------------
    CAST(TRIM(SK_ID_PREV) AS INT) AS Prev_Application_ID,
    CAST(TRIM(SK_ID_CURR) AS INT) AS Customer_ID,

    -----------------------------------------------------
    -- Time axis
    -----------------------------------------------------
    CAST(TRIM(MONTHS_BALANCE) AS INT) AS Months_Balance,

    -----------------------------------------------------
    -- Installment counters
    -----------------------------------------------------
    CAST(TRIM(CNT_INSTALMENT) AS FLOAT) AS Instalment_Count,
    CAST(TRIM(CNT_INSTALMENT_FUTURE) AS FLOAT) AS Instalment_Future_Count,

    -----------------------------------------------------
    -- Contract status
    -----------------------------------------------------
    UPPER(TRIM(NAME_CONTRACT_STATUS)) AS Contract_Status,

    -----------------------------------------------------
    -- Delinquency metrics
    -----------------------------------------------------
    CAST(TRIM(SK_DPD) AS INT) AS Days_Past_Due,
    CAST(TRIM(SK_DPD_DEF) AS INT) AS Days_Past_Due_Def

INTO silver.pos_cash_balance_cleaned
FROM bronze.POS_CASH_balance;

-------------------------
-- Add the Composit Primary Key
-------------------------
ALTER TABLE silver.pos_cash_balance_cleaned
Alter column Prev_Application_ID INT NOT NULL;

ALTER TABLE silver.pos_cash_balance_cleaned
Alter column MONTHS_BALANCE INT NOT NULL;

ALTER TABLE silver.pos_cash_balance_cleaned
ADD CONSTRAINT PK_Prev_Application_ID_Months_Balance
PRIMARY KEY (Prev_Application_ID, Months_Balance);


---------------
---------------
---------------

-----------------------------------------------------------
-- INSTALLMENTS PAYMENTS TABLE CLEANING
-- GOAL:
--   - Standardize identifiers
--   - Convert numeric & DAYS columns
--   - Handle installment amounts
--   - Create composite PK: Prev_Application_ID + Instalment_Number
-----------------------------------------------------------
IF OBJECT_ID('silver.installments_payments_cleaned', 'U') IS NOT NULL
    DROP TABLE silver.installments_payments_cleaned;

SELECT
    -----------------------------------------------------
    -- Identifiers
    -----------------------------------------------------
    CAST(TRIM(SK_ID_PREV) AS INT) AS Prev_Application_ID,
    CAST(TRIM(SK_ID_CURR) AS INT) AS Human_C,

    -----------------------------------------------------
    -- Installment Version / Number
    -----------------------------------------------------
    CAST(TRIM(NUM_INSTALMENT_VERSION) AS float) AS Instalment_Version,
    CAST(TRIM(NUM_INSTALMENT_NUMBER) AS INT) AS Instalment_Number,

    -----------------------------------------------------
    -- DAYS (convert 365243 → 0 then * -1)
    -----------------------------------------------------
   CAST(TRIM(DAYS_INSTALMENT) AS float) AS Days_Scheduled,

   CAST(TRIM(DAYS_ENTRY_PAYMENT) AS float) AS Days_Paid,

    -----------------------------------------------------
    -- Amounts
    -----------------------------------------------------
    CAST(TRIM(AMT_INSTALMENT) AS FLOAT) AS Instalment_Amount,
    CAST(TRIM(AMT_PAYMENT) AS FLOAT)    AS Payment_Amount

INTO silver.installments_payments_cleaned
FROM bronze.installments_payments;




-------------------------
-- Add the Composit Primary Key
-------------------------
ALTER TABLE silver.installments_payments_cleaned
Alter column Prev_Application_ID INT NOT NULL;

ALTER TABLE silver.installments_payments_cleaned
Alter column Instalment_Number INT NOT NULL;




-----------
