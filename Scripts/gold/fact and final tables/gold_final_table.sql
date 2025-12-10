
/*
====================================================================================
Gold Layer View: final_table
------------------------------------------------------------------------------------
Purpose:
This is the consolidated Gold Layer table combining all fact and dimension views. 
It includes:
1. Loan fact metrics
2. Customer demographic, income, and housing info
3. External bureau and credit history
4. Installments/payment behavior
5. POS cash & credit card balances
6. Previous applications history

This table is ready for ML modeling or advanced analytics.
====================================================================================
*/
CREATE VIEW gold.final_table AS

select 
    --------------------------------------------------
    -- 1. IDENTIFIERS AND TARGET (Output/Keys)
    --------------------------------------------------
    F.customer_id ,
    F.target,
    F.is_test,
    --------------------------------------------------
    -- 2. LOAN FACTS (Core Financials and Terms)
    --------------------------------------------------
    F.Loan_Type,
    F.loan_amount,
    F.Loan_Installment_Amount ,
    F.Loan_Term_Months,
    F.Item_Price,
    F.Weekday_Application,
    --------------------------------------------------
    -- 3. DIMENSION: CUSTOMER INFO (Demographics, Income, Geo) (c)
    --------------------------------------------------
    -- Demographics
    C.Gender,
    C.Age_years,
    C.Martial_status,
    C.Num_children,
    C.Family_size,
    C.Education_type,
    C.Type_suite,

    -- Employment / Income
    C.Total_income,
    C.Employed_years,
    C.Income_type,
    C.Job_type,
    C.Organization,
    
    -- Assets
    C.Has_car,
    C.Car_age,
    C.Has_house,
    C.House_type,

    -- External Scores
    C.Escores_1 AS EXT_SOURCE_1,
    C.Escores_2 AS EXT_SOURCE_2,
    C.Escores_3 AS EXT_SOURCE_3,

    -- Geo / Contact Info
    C.Region_population_ratio,
    C.Region_Rating_Client,
    C.Total_Geo_Mismatches,
    C.Has_emp_phone,
    C.Can_contacting,
    C.Total_contact_channels,
    C.Days_Since_Registration_Update,
    C.Days_Since_ID_Update,

    -- Housing Scores (AVG, MEDI, MODE scores should be included here if needed, 
    -- but for simplicity, using the AVG scores listed)
    C.Apartment_area_score,
    C.Basement_area_score,
    C.Building_year_built_score,
    C.Building_age_score,
    C.Common_area_score,
    C.Elevator_count_score,
    C.Building_entrances_score,
    C.Max_floors_score,
    C.Min_floors_score,
    C.Land_area_score,
    C.Living_apts_score,
    C.Living_area_score,
    C.Non_living_apts_score,
    C.Non_living_area_score,
    C.Repair_fund_type,
    C.House_type_mode,
    C.Total_area_mode_score,
    C.Walls_material,
    C.Has_emergency,

    -- Social / Documents
    C.Obs_30_cnt_social,
    C.Def_30_cnt_social,
    C.Obs_60_cnt_social,
    C.Def_60_cnt_social,
    C.Num_Of_Docs,

    --------------------------------------------------
    -- 4. DIMENSION: EXTERNAL DATA (e)
    --------------------------------------------------
    E.Has_Bureau_History_Flag AS Has_Bureau_hist ,
    E.Total_Bureau_Loans,
    E.Total_Active_Credit,
    E.Max_Overdue,
    round(E.Mean_Days_Credit,2) AS Mean_Days_Credit ,
    E.Mean_Max_DPD,
    E.Max_120_Plus_Count,
    E.Total_Balance_Months  ,

    --------------------------------------------------
    -- 5. DIMENSION: INSTALLMENTS (i)
    --------------------------------------------------
    I.Has_Installment_History_Flag,
    I.Total_Installments,
    I.Inst_Paid_Late_Count,
    I.Inst_Paid_Early_Count,
    I.Avg_Late_Days_Paid,
    I.Avg_Amount_Difference,
    I.Max_Overpayment,
    I.Max_Underpayment ,

    --------------------------------------------------
    -- 6. DIMENSION: ACCOUNT BALANCES (a)
    --------------------------------------------------
    A.POS_Month_Count,
    A.POS_Avg_DPD,
    A.POS_Max_DPD,
    A.POS_Active_Count,
    A.CC_Balance_Months_Count,
    A.CC_Avg_Utilization,
    A.CC_Avg_DPD_Card,
    A.CC_Avg_Payment_Ratio,
    A.CC_Max_Balance,
    p.Has_Prev_App_History_Flag,
    p.Total_Prev_Applications,
    p.Prev_Approved_Count,
    p.Prev_Cash_Loan_Count,
    p.Prev_Consumer_Loan_Count,
    p.Revolving_Loan_Count,
    p.Prev_Rejection_Rate,
    p.Max_Prev_Credit_Amt,
    p.Avg_Credit_Requested_Ratio,
    p.Avg_Prev_Annuity,
    p.Max_Days_Decision,
    p.Min_Days_Decision


FROM 
    gold.fact_loan  AS F

LEFT JOIN gold.Dim_Customer AS C
    ON F.Customer_id = C.Cust_id
LEFT JOIN gold.Dim_External_Data AS E
    ON F.Customer_id = E.Customer_id
LEFT JOIN gold.Dim_Installments_Payments AS I
    ON F.Customer_id = I.Customer_id
LEFT JOIN gold.Dim_Account_Balances AS A
    ON F.Customer_id = A.Customer_id
LEFT JOIN gold.Dim_Previous_Applications AS P
    ON F.Customer_id = P.Customer_id;