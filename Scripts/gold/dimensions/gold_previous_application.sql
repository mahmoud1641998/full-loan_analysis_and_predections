
/*
====================================================================================
Gold Layer View: Dim_Previous_Applications
------------------------------------------------------------------------------------
Purpose: 
Aggregates historical loan applications per customer to capture previous credit 
behavior, including approval/refusal counts, financial stats, timing, and loan types.
====================================================================================
*/
CREATE VIEW gold.Dim_Previous_Applications AS
-- ==================================================================================
-- Step 1: Aggregate previous applications per customer
-- ----------------------------------------------------------------------------------
-- CTE calculates counts, sums, averages, and categorical loan type counts for each customer
-- ==================================================================================
WITH Previous_App_Agg AS (
    SELECT
        P.Human_C AS Customer_id,

        -- 1. Count and Ratio Features
        COUNT(P.Prev_Application_ID) AS Total_Prev_Applications,
        
        -- Count of refused applications
        SUM(CASE WHEN P.Contract_Status = 'REFUSED' THEN 1 ELSE 0 END) AS Prev_Refused_Count,
        
        -- Count of approved applications
        SUM(CASE WHEN P.Contract_Status = 'APPROVED' THEN 1 ELSE 0 END) AS Prev_Approved_Count,

        
        -- 2. Financial Aggregations
        AVG(P.Annuity_Amount) AS Avg_Prev_Annuity,  -- Average previous annuity amount
        MAX(P.Credit_Amount) AS Max_Prev_Credit_Amt, -- Maximum credit amount requested
        AVG(P.Credit_Amount / NULLIF(P.Application_Amount,0)) AS Avg_Credit_Requested_Ratio, -- Avg ratio of credit requested vs applied

        -- 3. Timing
        MAX(P.Days_Since_Decision) AS Max_Days_Decision,
        MIN(P.Days_Since_Decision) AS Min_Days_Decision,
        
        -- 4. Loan Type Counts (Pivoted Features)
        SUM(CASE WHEN P.Loan_Type = 'CASH LOANS' THEN 1 ELSE 0 END) AS Prev_Cash_Loan_Count,
        SUM(CASE WHEN P.Loan_Type = 'CONSUMER LOANS' THEN 1 ELSE 0 END) AS Prev_Consumer_Loan_Count,
        SUM(CASE WHEN P.Loan_Type = 'REVOLVING LOANS' THEN 1 ELSE 0 END) AS Revolving_Loan_Count
        
    FROM 
        Silver.previous_application_cleaned AS P
    GROUP BY 
        P.Human_C
)

-- ==================================================================================
-- Step 2: Final SELECT
-- ----------------------------------------------------------------------------------
-- Join aggregated previous applications with application table
-- Impute NULLs with 0 for customers with no previous application history
-- ==================================================================================
SELECT
    A.Human_C AS Customer_id,
    
    -- Feature Engineering: Check if there is any history
    CASE WHEN PAA.Customer_id IS NOT NULL THEN 1 ELSE 0 END AS Has_Prev_App_History_Flag,
    
    -- Total applications and approved count (imputed 0 if no history)
    ISNULL(PAA.Total_Prev_Applications, 0) AS Total_Prev_Applications,
    ISNULL(PAA.Prev_Approved_Count, 0) AS Prev_Approved_Count,
    
    -- Rejection rate calculation
    ISNULL(CAST(PAA.Prev_Refused_Count AS FLOAT) / NULLIF(PAA.Total_Prev_Applications, 0), 0) AS Prev_Rejection_Rate,

    -- Financial metrics imputed with 0
    ISNULL(PAA.Avg_Prev_Annuity, 0) AS Avg_Prev_Annuity,
    ISNULL(PAA.Max_Prev_Credit_Amt, 0) AS Max_Prev_Credit_Amt,
    ISNULL(PAA.Avg_Credit_Requested_Ratio, 0) AS Avg_Credit_Requested_Ratio,

    -- Timing metrics imputed with 0
    ISNULL(PAA.Max_Days_Decision, 0) AS Max_Days_Decision,
    ISNULL(PAA.Min_Days_Decision, 0) AS Min_Days_Decision,

    -- Loan type counts imputed with 0
    ISNULL(PAA.Prev_Cash_Loan_Count, 0) AS Prev_Cash_Loan_Count,
    ISNULL(PAA.Prev_Consumer_Loan_Count, 0) AS Prev_Consumer_Loan_Count,
    ISNULL(PAA.Revolving_Loan_Count, 0) AS Revolving_Loan_Count

FROM 
    Silver.application_cleaned AS A
LEFT JOIN 
    Previous_App_Agg AS PAA ON A.Human_C = PAA.Customer_id;