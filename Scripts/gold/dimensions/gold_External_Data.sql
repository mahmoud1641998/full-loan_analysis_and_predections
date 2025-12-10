

/*
====================================================================================
Gold Layer View: Dim_External_Data
------------------------------------------------------------------------------------
Purpose: 
This view aggregates external credit bureau and bureau balance data to provide 
historical credit behavior features per customer. 
It includes delinquency metrics, credit exposure, and loan history summaries.
====================================================================================
*/
CREATE VIEW Gold.Dim_External_Data AS

-- ==================================================================================
-- Step 1: Aggregate Bureau_Balance data
-- ----------------------------------------------------------------------------------
-- Calculate monthly status metrics per prior loan (Bureau_ID) to capture historical
-- delinquency patterns and exposure.
-- ==================================================================================
WITH Bureau_Balance_Agg AS (
    SELECT
        BB.Bureau_ID,

        -- Max DPD per loan (Higher number = Worse status)
        -- Mapping status codes: C, X -> 0 (good), 0 -> 1, 1 -> 2 ... 5 -> 6        
        MAX(CASE BB.Status
            WHEN 'C' THEN 0
            WHEN 'X' THEN 0
            WHEN '0' THEN 1
            WHEN '1' THEN 2
            WHEN '2' THEN 3
            WHEN '3' THEN 4
            WHEN '4' THEN 5
            WHEN '5' THEN 6
            ELSE 0 END) AS Buro_Loan_Max_DPD,
        
        -- Count of records with severe delinquency (STATUS 4, 5, or DPD > 90)
        SUM(CASE 
            WHEN BB.Status IN ('4', '5') THEN 1 
            ELSE 0 
        END) AS Buro_Loan_Count_120_Plus,
        
        -- Number of months recorded in bureau balance        
        COUNT(BB.Month_Index) AS Buro_Balance_Months_Count
        
    FROM 
        Silver.bureau_balance_cleaned AS BB
    GROUP BY 
        BB.Bureau_ID
),

-- ==================================================================================
-- Step 2: Aggregate Bureau data and merge with Bureau_Balance aggregation
-- ----------------------------------------------------------------------------------
-- Summarize prior credit lines for each customer.
-- ==================================================================================
Bureau_Final_Agg AS (
    SELECT
        B.Human_C AS Customer_id,
               
        -- Total number of prior bureau loans
        COUNT(B.Bureau_ID) AS Total_Bureau_Loans,
        
        -- Sum of active credit lines
        SUM(CASE WHEN B.Credit_Active_Status = 'Active' THEN B.Credit_Sum ELSE 0 END) AS Total_Active_Credit,
        
        -- Max overdue amount recorded across all prior loans
        MAX(B.Credit_Sum_Overdue) AS Max_Overdue,
        
        -- Average age of prior credit lines (in days)
        AVG(CAST(B.Days_Credit_Start AS FLOAT)) AS Mean_Days_Credit,
        
        -- Aggregating the features from the monthly balance CTE
        AVG(BBA.Buro_Loan_Max_DPD) AS Mean_Max_DPD,   -- Avg max DPD across all loans
        MAX(BBA.Buro_Loan_Count_120_Plus) AS Max_120_Plus_Count,  -- Max count of severe delinquency
        SUM(BBA.Buro_Balance_Months_Count) AS Total_Balance_Months -- Total months of bureau history
        
    FROM 
        Silver.bureau_cleaned AS B
    LEFT JOIN 
        Bureau_Balance_Agg AS BBA ON B.Bureau_ID = BBA.Bureau_ID
    GROUP BY 
        B.Human_C
)
-- ==================================================================================
-- Final SELECT: Join aggregated bureau data with application table
-- ==================================================================================
SELECT
    A.Human_C AS Customer_id,
    
    -- Feature Engineering: Has Bureau History Flag
    CASE WHEN BFA.Customer_id IS NOT NULL THEN 1 ELSE 0 END AS Has_Bureau_History_Flag,
    
    -- Impute NULL values (for clients with no bureau history) with 0
    ISNULL(BFA.Total_Bureau_Loans, 0) AS Total_Bureau_Loans,
    ISNULL(BFA.Total_Active_Credit, 0) AS Total_Active_Credit,
    ISNULL(BFA.Max_Overdue, 0) AS Max_Overdue,
    
    -- Mean Days Credit: Imputing missing averages with 0
    ISNULL(BFA.Mean_Days_Credit, 0) AS Mean_Days_Credit, 
    
    -- Average maximum DPD across prior loans (impute 0 if no history)
    ISNULL(BFA.Mean_Max_DPD, 0) AS Mean_Max_DPD,

    -- Maximum count of 120+ DPD events (impute 0 if no history)
    ISNULL(BFA.Max_120_Plus_Count, 0) AS Max_120_Plus_Count,

    -- Total months of bureau history per customer (impute 0 if no history)
    ISNULL(BFA.Total_Balance_Months, 0) AS Total_Balance_Months
    
FROM 
    Silver.application_cleaned AS A 
LEFT JOIN 
    Bureau_Final_Agg AS BFA ON A.Human_C = BFA.Customer_id;