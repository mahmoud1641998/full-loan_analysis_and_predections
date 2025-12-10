/*
====================================================================================
Gold Layer View: Dim_Account_Balances
------------------------------------------------------------------------------------
Purpose: 
This view aggregates the customer's POS cash balances and credit card balances 
to create a single dimensional table for analytics and modeling. 
It summarizes historical repayment behavior, delinquency, utilization, and contract activity.
====================================================================================
*/

CREATE VIEW Gold.Dim_Account_Balances AS

-- ==================================================================================
-- CTE 1: Aggregate POS_CASH_BALANCE data
-- ==================================================================================

WITH POS_Cash_Agg AS (
    SELECT
        P.Customer_ID AS Customer_id,  -- Customer identifier

        -- Count of months the customer has POS_CASH balances
        COUNT(P.Prev_Application_ID) AS POS_Month_Count,

        -- Difference between future expected installments and actual installments paid
        -- Used as a proxy for delinquency
        AVG(CAST(P.Instalment_Future_Count AS FLOAT) - P.Instalment_Count) AS POS_Avg_Future_Inst_Diff,

        -- Average days past due across all months
        AVG(P.Days_Past_Due) AS POS_Avg_DPD,

        -- Maximum days past due recorded
        MAX(P.Days_Past_Due) AS POS_Max_DPD,

        -- Count of active POS loans for the customer
        SUM(CASE WHEN P.Contract_Status = 'ACTIVE' THEN 1 ELSE 0 END) AS POS_Active_Count

    FROM 
        Silver.pos_cash_balance_cleaned AS P
    GROUP BY 
        P.Customer_ID
),


-- ==================================================================================
-- CTE 2: Aggregate CREDIT_CARD_BALANCE data
-- ==================================================================================
Credit_Card_Agg AS (
    SELECT
        CC.Human_C AS Customer_id,  -- Customer identifier

        -- Count of months with credit card balance history
        COUNT(CC.Prev_Application_ID) AS CC_Balance_Months_Count,

        -- Average utilization of credit limit (balance / limit)
        AVG(CC.Balance_Amount / NULLIF(CC.Credit_Limit_Actual, 0)) AS CC_Avg_Utilization,

        -- Average days past due on credit cards
        AVG(CC.Days_Past_Due) AS CC_Avg_DPD_Card,

        -- Average ratio of total payment to current installment
        -- High values indicate good repayment
        AVG(CC.Payment_Total_Current / NULLIF(CC.Payment_Current, 0)) AS CC_Avg_Payment_Ratio,

        -- Maximum balance reported in the history
        MAX(CC.Balance_Amount) AS CC_Max_Balance

    FROM 
        Silver.credit_card_balance_cleaned AS CC
    GROUP BY 
        CC.Human_C
)


-- ==================================================================================
-- Final SELECT: Join aggregated balances with application table
-- ==================================================================================
SELECT
    A.Human_C AS Customer_id,  -- Customer identifier

    -- ======================
    -- POS Cash Features
    -- ======================
    ISNULL(PCA.POS_Month_Count, 0) AS POS_Month_Count,  -- Number of months with POS data
    ISNULL(PCA.POS_Avg_DPD, 0) AS POS_Avg_DPD,  -- Avg days past due
    ISNULL(PCA.POS_Max_DPD, 0) AS POS_Max_DPD,  -- Max days past due
    ISNULL(PCA.POS_Active_Count, 0) AS POS_Active_Count,  -- Number of active POS loans

    -- ======================
    -- Credit Card Features
    -- ======================
    ISNULL(CCA.CC_Balance_Months_Count, 0) AS CC_Balance_Months_Count,  -- Number of months with credit card balance
    ISNULL(CCA.CC_Avg_Utilization, 0) AS CC_Avg_Utilization,  -- Avg utilization of credit card limit
    ISNULL(CCA.CC_Avg_DPD_Card, 0) AS CC_Avg_DPD_Card,  -- Avg days past due for credit card
    ISNULL(CCA.CC_Avg_Payment_Ratio, 0) AS CC_Avg_Payment_Ratio,  -- Avg ratio of payments made
    ISNULL(CCA.CC_Max_Balance, 0) AS CC_Max_Balance  -- Max balance ever reported

FROM 
    Silver.application_cleaned AS A
LEFT JOIN 
    POS_Cash_Agg AS PCA ON A.Human_C = PCA.Customer_id
LEFT JOIN 
    Credit_Card_Agg AS CCA ON A.Human_C = CCA.Customer_id;
