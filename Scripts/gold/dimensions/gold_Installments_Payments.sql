
/*
====================================================================================
Gold Layer View: Dim_Installments_Payments
------------------------------------------------------------------------------------
Purpose: 
This view aggregates installments and payments data per customer to capture 
payment behavior patterns, including timeliness, overpayments, and underpayments.
====================================================================================
*/
CREATE VIEW Gold.Dim_Installments_Payments AS


-- ==================================================================================
-- Step 1: Pre-calculate differences per installment record
-- ----------------------------------------------------------------------------------
-- CTE calculates the timing and amount differences for each installment:
--  - Days_Difference: how early or late the installment was paid
--  - Amount_Difference: how much more or less was paid compared to the scheduled amount
-- ==================================================================================
WITH Installments_Agg AS (
   SELECT
        IP.Human_C AS Customer_id,
        IP.Prev_Application_ID, 
        
        -- Difference in days (Scheduled Day - Actual Payment Day)
        (IP.Days_Scheduled - IP.Days_Paid) AS Days_Difference,
        
        -- Difference in amount (Scheduled Amount - Actual Payment Amount)
        (IP.Instalment_Amount - IP.Payment_Amount) AS Amount_Difference
    FROM 
        Silver.installments_payments_cleaned AS IP
),
-- ==================================================================================
-- Step 2: Final Aggregation per customer
-- ----------------------------------------------------------------------------------
-- Aggregates all installments per customer to derive behavior metrics
-- ==================================================================================
Final_Installments_Agg AS (
    SELECT
        IA.Customer_id,
        
        -- 1. Counts and Frequencies (COUNT is now applied here)
        COUNT(IA.Prev_Application_ID) AS Total_Installments, 
        SUM(CASE WHEN IA.Days_Difference < 0 THEN 1 ELSE 0 END) AS Inst_Paid_Late_Count,
        SUM(CASE WHEN IA.Days_Difference > 0 THEN 1 ELSE 0 END) AS Inst_Paid_Early_Count,
        
        -- 2. Timing Aggregations
        AVG(CASE WHEN IA.Days_Difference < 0 THEN ABS(IA.Days_Difference) ELSE 0 END) AS Avg_Late_Days_Paid,
        
        -- 3. Financial Aggregations
        AVG(IA.Amount_Difference) AS Avg_Amount_Difference,
        MAX(CASE WHEN IA.Amount_Difference < 0 THEN ABS(IA.Amount_Difference) ELSE 0 END) AS Max_Overpayment,
        MAX(CASE WHEN IA.Amount_Difference > 0 THEN IA.Amount_Difference ELSE 0 END) AS Max_Underpayment

    FROM 
        Installments_Agg AS IA
    GROUP BY 
        IA.Customer_id
)
-- ==================================================================================
-- Final SELECT: Join aggregated installment data with application table
-- ----------------------------------------------------------------------------------
-- Impute NULLs with 0 for customers with no installment history
-- ==================================================================================
SELECT
    A.Human_C AS Customer_id,
    CASE WHEN FIA.Customer_id IS NOT NULL THEN 1 ELSE 0 END AS Has_Installment_History_Flag,
    
    ISNULL(FIA.Total_Installments, 0) AS Total_Installments,
    ISNULL(FIA.Inst_Paid_Late_Count, 0) AS Inst_Paid_Late_Count,
    ISNULL(FIA.Inst_Paid_Early_Count, 0) AS Inst_Paid_Early_Count,
    
    ISNULL(FIA.Avg_Late_Days_Paid, 0) AS Avg_Late_Days_Paid,
    ISNULL(FIA.Avg_Amount_Difference, 0) AS Avg_Amount_Difference,
    ISNULL(FIA.Max_Overpayment, 0) AS Max_Overpayment,
    ISNULL(FIA.Max_Underpayment, 0) AS Max_Underpayment

FROM 
    Silver.application_cleaned AS A
LEFT JOIN 
    Final_Installments_Agg AS FIA ON A.Human_C = FIA.Customer_id;