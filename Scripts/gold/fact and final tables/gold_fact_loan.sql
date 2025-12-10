/*
====================================================================================
Gold Layer View: Fact_Loan
------------------------------------------------------------------------------------
Purpose:
This is the fact table for loans capturing the main loan-level metrics for each 
customer, including target label, loan type, amounts, installment, term, and application details.
====================================================================================
*/

CREATE VIEW Gold.Fact_Loan AS

SELECT
    AC.Human_C AS Customer_id,  -- Customer identifier

    AC.Target,                  -- Target variable (e.g., defaulted or not)
    AC.Is_Test,                 -- Flag indicating if the record is part of the test set

    AC.Loan_Type,               -- Type of the loan (e.g., CASH LOAN, CONSUMER LOAN)
    AC.Loan_Amount,             -- Total loan amount granted
    AC.Loan_Installment_Amount, -- Monthly installment amount for the loan

    -- Calculate loan term in months (Loan Amount ÷ Monthly Installment)
    ROUND(AC.Loan_Amount / NULLIF(AC.Loan_Installment_Amount, 0), 0) AS Loan_Term_Months,

    AC.Item_Price,               -- Price of the item being financed
    AC.Weekday_Application       -- Day of the week when application was submitted

FROM 
    Silver.application_cleaned AS AC;
