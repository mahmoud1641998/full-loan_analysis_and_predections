/********************************************************************************************
 PROJECT: Home Credit Default Risk – Data Engineering Pipeline
 SCRIPT: 02 - Bronze Layer (Raw Tables)
 PURPOSE:
    - Create raw landing-zone tables for all CSV sources.
    - Mirror CSV structure exactly (no type casting, no constraints).
    - Allow re-run of script safely during development.
 NOTES:
    ✔ Bronze = "Raw Zone" → Data is ingested AS-IS.
    ✔ All columns use NVARCHAR(MAX) to avoid type errors during ingestion.
    ✔ No primary keys, no foreign keys, no constraints → handled later in Silver.
********************************************************************************************/
use Bank_loan;
/********************************************************************************************
TABLE: bronze.Application
    • Main customer application file (train).
    • Contains TARGET column used for ML.
    • Contains 120+ demographic + financial + house-related features.
********************************************************************************************/


if OBJECT_ID ('bronze.Application','U') is not null
    drop table bronze.Application;
CREATE TABLE  bronze.Application (
    SK_ID_CURR                 NVARCHAR(MAX) ,
    TARGET                     NVARCHAR(MAX),
    -- Application basic info
    NAME_CONTRACT_TYPE         NVARCHAR(MAX),
    CODE_GENDER                NVARCHAR(MAX),
    FLAG_OWN_CAR               NVARCHAR(MAX),
    FLAG_OWN_REALTY            NVARCHAR(MAX),

    -- Financials
    CNT_CHILDREN               NVARCHAR(MAX),
    AMT_INCOME_TOTAL           NVARCHAR(MAX),
    AMT_CREDIT                 NVARCHAR(MAX),
    AMT_ANNUITY                NVARCHAR(MAX),
    AMT_GOODS_PRICE            NVARCHAR(MAX),

    -- Categorical socioeconomic details
    NAME_TYPE_SUITE            NVARCHAR(MAX),
    NAME_INCOME_TYPE           NVARCHAR(MAX),
    NAME_EDUCATION_TYPE        NVARCHAR(MAX),
    NAME_FAMILY_STATUS         NVARCHAR(MAX),
    NAME_HOUSING_TYPE          NVARCHAR(MAX),

    REGION_POPULATION_RELATIVE NVARCHAR(MAX),

    -- Time-based (negative = days before application)
    DAYS_BIRTH                 NVARCHAR(MAX),
    DAYS_EMPLOYED              NVARCHAR(MAX),
    DAYS_REGISTRATION          NVARCHAR(MAX),
    DAYS_ID_PUBLISH            NVARCHAR(MAX),
    OWN_CAR_AGE                NVARCHAR(MAX),
    
    -- Contact & phone flags
    FLAG_MOBIL                 NVARCHAR(MAX),
    FLAG_EMP_PHONE             NVARCHAR(MAX),
    FLAG_WORK_PHONE            NVARCHAR(MAX),
    FLAG_CONT_MOBILE           NVARCHAR(MAX),
    FLAG_PHONE                 NVARCHAR(MAX),
    FLAG_EMAIL                 NVARCHAR(MAX),

    -- Occupation & rating
    OCCUPATION_TYPE            NVARCHAR(MAX),
    CNT_FAM_MEMBERS            NVARCHAR(MAX),
    REGION_RATING_CLIENT       NVARCHAR(MAX),
    REGION_RATING_CLIENT_W_CITY NVARCHAR(MAX),
    WEEKDAY_APPR_PROCESS_START NVARCHAR(MAX),
    HOUR_APPR_PROCESS_START    NVARCHAR(MAX),

    -- Region mismatch flags
    REG_REGION_NOT_LIVE_REGION NVARCHAR(MAX),
    REG_REGION_NOT_WORK_REGION NVARCHAR(MAX),
    LIVE_REGION_NOT_WORK_REGION NVARCHAR(MAX),

    REG_CITY_NOT_LIVE_CITY     NVARCHAR(MAX),
    REG_CITY_NOT_WORK_CITY     NVARCHAR(MAX),
    LIVE_CITY_NOT_WORK_CITY    NVARCHAR(MAX),

    ORGANIZATION_TYPE          NVARCHAR(MAX),

    -- EXT sources (external score features)
    EXT_SOURCE_1               NVARCHAR(MAX),
    EXT_SOURCE_2               NVARCHAR(MAX),
    EXT_SOURCE_3               NVARCHAR(MAX),

    -- House attributes (avg/mode/medi)
    APARTMENTS_AVG             NVARCHAR(MAX),
    BASEMENTAREA_AVG           NVARCHAR(MAX),
    YEARS_BEGINEXPLUATATION_AVG NVARCHAR(MAX),
    YEARS_BUILD_AVG            NVARCHAR(MAX),
    COMMONAREA_AVG             NVARCHAR(MAX),
    ELEVATORS_AVG              NVARCHAR(MAX),
    ENTRANCES_AVG              NVARCHAR(MAX),
    FLOORSMAX_AVG              NVARCHAR(MAX),
    FLOORSMIN_AVG              NVARCHAR(MAX),
    LANDAREA_AVG               NVARCHAR(MAX),
    LIVINGAPARTMENTS_AVG       NVARCHAR(MAX),
    LIVINGAREA_AVG             NVARCHAR(MAX),
    NONLIVINGAPARTMENTS_AVG    NVARCHAR(MAX),
    NONLIVINGAREA_AVG          NVARCHAR(MAX),

    APARTMENTS_MODE            NVARCHAR(MAX),
    BASEMENTAREA_MODE          NVARCHAR(MAX),
    YEARS_BEGINEXPLUATATION_MODE NVARCHAR(MAX),
    YEARS_BUILD_MODE           NVARCHAR(MAX),
    COMMONAREA_MODE            NVARCHAR(MAX),
    ELEVATORS_MODE             NVARCHAR(MAX),
    ENTRANCES_MODE             NVARCHAR(MAX),
    FLOORSMAX_MODE             NVARCHAR(MAX),
    FLOORSMIN_MODE             NVARCHAR(MAX),
    LANDAREA_MODE              NVARCHAR(MAX),
    LIVINGAPARTMENTS_MODE      NVARCHAR(MAX),
    LIVINGAREA_MODE            NVARCHAR(MAX),
    NONLIVINGAPARTMENTS_MODE   NVARCHAR(MAX),
    NONLIVINGAREA_MODE         NVARCHAR(MAX),

    APARTMENTS_MEDI            NVARCHAR(MAX),
    BASEMENTAREA_MEDI          NVARCHAR(MAX),
    YEARS_BEGINEXPLUATATION_MEDI NVARCHAR(MAX),
    YEARS_BUILD_MEDI           NVARCHAR(MAX),
    COMMONAREA_MEDI            NVARCHAR(MAX),
    ELEVATORS_MEDI             NVARCHAR(MAX),
    ENTRANCES_MEDI             NVARCHAR(MAX),
    FLOORSMAX_MEDI             NVARCHAR(MAX),
    FLOORSMIN_MEDI             NVARCHAR(MAX),
    LANDAREA_MEDI              NVARCHAR(MAX),
    LIVINGAPARTMENTS_MEDI      NVARCHAR(MAX),
    LIVINGAREA_MEDI            NVARCHAR(MAX),
    NONLIVINGAPARTMENTS_MEDI   NVARCHAR(MAX),
    NONLIVINGAREA_MEDI         NVARCHAR(MAX),
    
    -- Other house metadata
    FONDKAPREMONT_MODE         NVARCHAR(MAX),
    HOUSETYPE_MODE             NVARCHAR(MAX),
    TOTALAREA_MODE             NVARCHAR(MAX),
    WALLSMATERIAL_MODE         NVARCHAR(MAX),
    EMERGENCYSTATE_MODE        NVARCHAR(MAX),

    -- Social circle risk features
    OBS_30_CNT_SOCIAL_CIRCLE   NVARCHAR(MAX),
    DEF_30_CNT_SOCIAL_CIRCLE   NVARCHAR(MAX),
    OBS_60_CNT_SOCIAL_CIRCLE   NVARCHAR(MAX),
    DEF_60_CNT_SOCIAL_CIRCLE   NVARCHAR(MAX),

    DAYS_LAST_PHONE_CHANGE     NVARCHAR(MAX),

    -- Document binary flags
    FLAG_DOCUMENT_2            NVARCHAR(MAX),
    FLAG_DOCUMENT_3            NVARCHAR(MAX),
    FLAG_DOCUMENT_4            NVARCHAR(MAX),
    FLAG_DOCUMENT_5            NVARCHAR(MAX),
    FLAG_DOCUMENT_6            NVARCHAR(MAX),
    FLAG_DOCUMENT_7            NVARCHAR(MAX),
    FLAG_DOCUMENT_8            NVARCHAR(MAX),
    FLAG_DOCUMENT_9            NVARCHAR(MAX),
    FLAG_DOCUMENT_10           NVARCHAR(MAX),
    FLAG_DOCUMENT_11           NVARCHAR(MAX),
    FLAG_DOCUMENT_12           NVARCHAR(MAX),
    FLAG_DOCUMENT_13           NVARCHAR(MAX),
    FLAG_DOCUMENT_14           NVARCHAR(MAX),
    FLAG_DOCUMENT_15           NVARCHAR(MAX),
    FLAG_DOCUMENT_16           NVARCHAR(MAX),
    FLAG_DOCUMENT_17           NVARCHAR(MAX),
    FLAG_DOCUMENT_18           NVARCHAR(MAX),
    FLAG_DOCUMENT_19           NVARCHAR(MAX),
    FLAG_DOCUMENT_20           NVARCHAR(MAX),
    FLAG_DOCUMENT_21           NVARCHAR(MAX),

    -- Bureau inquiries
    AMT_REQ_CREDIT_BUREAU_HOUR NVARCHAR(MAX),
    AMT_REQ_CREDIT_BUREAU_DAY  NVARCHAR(MAX),
    AMT_REQ_CREDIT_BUREAU_WEEK NVARCHAR(MAX),
    AMT_REQ_CREDIT_BUREAU_MON  NVARCHAR(MAX),
    AMT_REQ_CREDIT_BUREAU_QRT  NVARCHAR(MAX),
    AMT_REQ_CREDIT_BUREAU_YEAR NVARCHAR(MAX)
);

/********************************************************************************************
 TABLE: bronze.bureau
    • Historical credit reports from external bureau.
    • One customer → Many bureau entries.
********************************************************************************************/

if OBJECT_ID ('bronze.bureau','U') is not null
    drop table  bronze.bureau;

CREATE TABLE bronze.bureau (
    SK_ID_CURR                 NVARCHAR(MAX)             ,
    SK_ID_BUREAU              NVARCHAR(MAX),
    CREDIT_ACTIVE             NVARCHAR(MAX),
    CREDIT_CURRENCY           NVARCHAR(MAX),
    DAYS_CREDIT               NVARCHAR(MAX),
    CREDIT_DAY_OVERDUE        NVARCHAR(MAX),
    DAYS_CREDIT_ENDDATE       NVARCHAR(MAX),
    DAYS_ENDDATE_FACT         NVARCHAR(MAX),
    AMT_CREDIT_MAX_OVERDUE    NVARCHAR(MAX),
    CNT_CREDIT_PROLONG        NVARCHAR(MAX),
    AMT_CREDIT_SUM            NVARCHAR(MAX),
    AMT_CREDIT_SUM_DEBT       NVARCHAR(MAX),
    AMT_CREDIT_SUM_LIMIT      NVARCHAR(MAX),
    AMT_CREDIT_SUM_OVERDUE    NVARCHAR(MAX),
    CREDIT_TYPE               NVARCHAR(MAX),
    DAYS_CREDIT_UPDATE        NVARCHAR(MAX),
    AMT_ANNUITY               NVARCHAR(MAX)
);


/********************************************************************************************
TABLE: bronze.bureau_balance
    • Monthly snapshots for each bureau loan.
    • One SK_ID_BUREAU → Many monthly rows.
********************************************************************************************/
if OBJECT_ID ('bronze.bureau_balance','U') is not null
    drop table bronze.bureau_balance;
CREATE TABLE bronze.bureau_balance (
    SK_ID_BUREAU     NVARCHAR(MAX)          ,     -- FK to bureau
    MONTHS_BALANCE   NVARCHAR(MAX)            ,     -- monthly snapshot
    STATUS           VARCHAR(10)
);

/********************************************************************************************
TABLE: bronze.previous_application
    • All previous loan applications per customer.
    • One customer → Many previous applications.
********************************************************************************************/
if OBJECT_ID ('bronze.previous_application','U') is not null
    drop table bronze.previous_application;

CREATE TABLE bronze.previous_application (
    SK_ID_PREV                     NVARCHAR(MAX),
    SK_ID_CURR                     NVARCHAR(MAX),
    NAME_CONTRACT_TYPE             NVARCHAR(MAX),
    AMT_ANNUITY                    NVARCHAR(MAX),
    AMT_APPLICATION                NVARCHAR(MAX),
    AMT_CREDIT                     NVARCHAR(MAX),
    AMT_DOWN_PAYMENT               NVARCHAR(MAX),
    AMT_GOODS_PRICE                NVARCHAR(MAX),
    WEEKDAY_APPR_PROCESS_START     NVARCHAR(MAX),
    HOUR_APPR_PROCESS_START        NVARCHAR(MAX),
    FLAG_LAST_APPL_PER_CONTRACT    NVARCHAR(MAX),
    NFLAG_LAST_APPL_IN_DAY         NVARCHAR(MAX),
    RATE_DOWN_PAYMENT              NVARCHAR(MAX),
    RATE_INTEREST_PRIMARY          NVARCHAR(MAX),
    RATE_INTEREST_PRIVILEGED       NVARCHAR(MAX),
    NAME_CASH_LOAN_PURPOSE         NVARCHAR(MAX),
    NAME_CONTRACT_STATUS           NVARCHAR(MAX),
    DAYS_DECISION                  NVARCHAR(MAX),
    NAME_PAYMENT_TYPE              NVARCHAR(MAX),
    CODE_REJECT_REASON             NVARCHAR(MAX),
    NAME_TYPE_SUITE                NVARCHAR(MAX),
    NAME_CLIENT_TYPE               NVARCHAR(MAX),
    NAME_GOODS_CATEGORY            NVARCHAR(MAX),
    NAME_PORTFOLIO                 NVARCHAR(MAX),
    NAME_PRODUCT_TYPE              NVARCHAR(MAX),
    CHANNEL_TYPE                   NVARCHAR(MAX),
    SELLERPLACE_AREA               NVARCHAR(MAX),
    NAME_SELLER_INDUSTRY           NVARCHAR(MAX),
    CNT_PAYMENT                    NVARCHAR(MAX),
    NAME_YIELD_GROUP               NVARCHAR(MAX),
    PRODUCT_COMBINATION            NVARCHAR(MAX),
    DAYS_FIRST_DRAWING             NVARCHAR(MAX),
    DAYS_FIRST_DUE                 NVARCHAR(MAX),
    DAYS_LAST_DUE_1ST_VERSION      NVARCHAR(MAX),
    DAYS_LAST_DUE                  NVARCHAR(MAX),
    DAYS_TERMINATION               NVARCHAR(MAX),
    NFLAG_INSURED_ON_APPROVAL      NVARCHAR(MAX)
);


/********************************************************************************************
TABLE: bronze.credit_card_balance
    • Monthly credit card performance per previous loan.
    • One SK_ID_PREV → Many monthly credit card rows.
********************************************************************************************/
if OBJECT_ID ('bronze.credit_card_balance','U') is not null
    drop table bronze.credit_card_balance;

CREATE TABLE bronze.credit_card_balance (
    SK_ID_PREV                     NVARCHAR(MAX),
    SK_ID_CURR                     NVARCHAR(MAX),
    MONTHS_BALANCE                 NVARCHAR(MAX),
    AMT_BALANCE                    NVARCHAR(MAX),
    AMT_CREDIT_LIMIT_ACTUAL        NVARCHAR(MAX),
    AMT_DRAWINGS_ATM_CURRENT       NVARCHAR(MAX),
    AMT_DRAWINGS_CURRENT           NVARCHAR(MAX),
    AMT_DRAWINGS_OTHER_CURRENT     NVARCHAR(MAX),
    AMT_DRAWINGS_POS_CURRENT       NVARCHAR(MAX),
    AMT_INST_MIN_REGULARITY        NVARCHAR(MAX),
    AMT_PAYMENT_CURRENT            NVARCHAR(MAX),
    AMT_PAYMENT_TOTAL_CURRENT      NVARCHAR(MAX),
    AMT_RECEIVABLE_PRINCIPAL       NVARCHAR(MAX),
    AMT_RECIVABLE                  NVARCHAR(MAX),
    AMT_TOTAL_RECEIVABLE           NVARCHAR(MAX),
    CNT_DRAWINGS_ATM_CURRENT       NVARCHAR(MAX),
    CNT_DRAWINGS_CURRENT           NVARCHAR(MAX),
    CNT_DRAWINGS_OTHER_CURRENT     NVARCHAR(MAX),
    CNT_DRAWINGS_POS_CURRENT       NVARCHAR(MAX),
    CNT_INSTALMENT_MATURE_CUM      NVARCHAR(MAX),
    NAME_CONTRACT_STATUS           NVARCHAR(MAX),
    SK_DPD                         NVARCHAR(MAX),
    SK_DPD_DEF                     NVARCHAR(MAX),
    
);

/********************************************************************************************
TABLE: bronze.POS_CASH_balance
    • Monthly POS / cash loan performance.
********************************************************************************************/

if OBJECT_ID ('bronze.POS_CASH_balance','U') is not null
    drop table bronze.POS_CASH_balance;
CREATE TABLE bronze.POS_CASH_balance (
    SK_ID_PREV              NVARCHAR(MAX),
    SK_ID_CURR              NVARCHAR(MAX),
    MONTHS_BALANCE          NVARCHAR(MAX),
    CNT_INSTALMENT          NVARCHAR(MAX),
    CNT_INSTALMENT_FUTURE   NVARCHAR(MAX),
    NAME_CONTRACT_STATUS    NVARCHAR(MAX),
    SK_DPD                  NVARCHAR(MAX),
    SK_DPD_DEF              NVARCHAR(MAX)
);

/********************************************************************************************
TABLE: bronze.installments_payments
    • Ground-truth repayment behavior of customers.
    • Base table for payment delay, underpayment, and behavior scoring.
********************************************************************************************/

if OBJECT_ID ('bronze.installments_payments','U') is not null
    drop table bronze.installments_payments;
CREATE TABLE bronze.installments_payments (
    SK_ID_PREV               NVARCHAR(MAX),
    SK_ID_CURR               NVARCHAR(MAX),
    NUM_INSTALMENT_VERSION   NVARCHAR(MAX),
    NUM_INSTALMENT_NUMBER    NVARCHAR(MAX),
    DAYS_INSTALMENT          NVARCHAR(MAX),
    DAYS_ENTRY_PAYMENT       NVARCHAR(MAX),
    AMT_INSTALMENT           NVARCHAR(MAX),
    AMT_PAYMENT              NVARCHAR(MAX)
);



/********************************************************************************************
END OF BRONZE DEFINITIONS
    • All raw ingestion tables ready
    • No types enforced yet
    • Next step → Download Dataset from CSV's to our Bronze layer
********************************************************************************************/



