
-- ===============================================================
-- View: gold.dim_customer
-- Purpose: Transform silver.application_cleaned into a customer-level
--          dimension for analytics and feature engineering.
-- ===============================================================
CREATE OR ALTER VIEW gold.dim_customer AS
SELECT
    -- ----------------------
    -- Identifiers
    -- ----------------------
    Human_C AS Cust_id,                     -- Unique Customer ID

    -- ----------------------
    -- Demographics
    -- ----------------------
    GENDER AS Gender,                       -- Customer Gender
    Nullif(ROUND(ABS(Age_Days)/365.25,1),0) AS Age_years,   -- Age in years
    Marital_Status AS Martial_status,       -- Marital Status
    Num_Children AS Num_children,           -- Number of children
    Family_Size AS Family_size,             -- Total family members
    Nullif(ROUND(ABS(Employment_Duration_Days)/365.25,1),0) AS Employed_years, -- Years employed
    Education_Type AS Education_type,       -- Education level
    Has_Car AS Has_car,                     -- Owns a car (1/0)
    Car_Age_Years AS Car_age,               -- Car age in years
    Total_Income AS Total_income,           -- Annual income
    Income_Type AS Income_type,             -- Type of income
    Job_Type AS Job_type,                   -- Occupation
    Organization_Type AS Organization,      -- Employer type
    Has_House AS Has_house,                 -- Owns house (1/0)
    Housing_Type AS House_type,             -- Type of housing
    TypeSuite AS Type_suite,                -- Living arrangement type
    Days_Since_Registration_Update,         -- Days since registration update
    Days_Since_ID_Update,                   -- Days since ID publication

    -- ----------------------
    -- Contact info
    -- ----------------------
    Has_Mobile + Has_Phone + Has_Work_Phone + Has_Email AS Total_contact_channels,  -- Total contact methods
    Has_Emp_Phone AS Has_emp_phone,        -- Work phone available
    Can_Contacting AS Can_contacting,      -- Can contact via mobile

    -- ----------------------
    -- Region info
    -- ----------------------
    Region_Population_Ratio AS Region_population_ratio,
    Region_Rating_Client_WCity AS Region_Rating_Client,
    Reg_Not_Live_Region + Reg_Not_Work_Region + Live_Not_Work_Region + 
    Reg_City_Not_Live + Reg_City_Not_Work + Live_City_Not_Work AS Total_Geo_Mismatches,  -- Total geo mismatches

       

    -- ----------------------
    -- Apartment / Housing Scores
    -- ----------------------
    Apartment_Area_Score AS Apartment_area_score,
    Basement_Area_Score AS Basement_area_score,
    Building_Year_Built_Score AS Building_year_built_score,
    Building_Age_Score AS Building_age_score,
    Common_Area_Score AS Common_area_score,
    Elevator_Count_Score AS Elevator_count_score,
    Building_Entrances_Score AS Building_entrances_score,
    MaxFloors_Score AS Max_floors_score,
    MinFloors_Score AS Min_floors_score,
    LandArea_Score AS Land_area_score,
    LivingApts_Score AS Living_apts_score,
    LivingArea_Score AS Living_area_score,
    NonLivingApts_Score AS Non_living_apts_score,
    NonLivingArea_Score AS Non_living_area_score,

    
    -- ----------------------
    -- External scores
    -- ----------------------
    ESource_Scores_1 AS Escores_1,
    ESource_Scores_2 AS Escores_2,
    ESource_Scores_3 AS Escores_3,

    -- ----------------------
    -- Housing & repair info
    -- ----------------------
    RepairFundType AS Repair_fund_type,
    HouseType_Mode AS House_type_mode,
    TotalArea_Mode_Score AS Total_area_mode_score,
    WallsMaterial_Mode AS Walls_material,
    CASE WHEN Has_Emergency = 'Yes' THEN 1 ELSE 0 END AS Has_emergency,

    -- ----------------------
    -- Social circle
    -- ----------------------
    Obs_30_Cnt_Social AS Obs_30_cnt_social,
    Def_30_Cnt_Social AS Def_30_cnt_social,
    Obs_60_Cnt_Social AS Obs_60_cnt_social,
    Def_60_Cnt_Social AS Def_60_cnt_social,

    -- ----------------------
    -- Documents
    -- ----------------------
    Has_Document_2 + Has_Document_3 + Has_Document_4 +
    Has_Document_5 + Has_Document_6 + Has_Document_7 + 
    Has_Document_8 + Has_Document_9 + Has_Document_10 + 
    Has_Document_11 + Has_Document_12 + Has_Document_13 + 
    Has_Document_14 + Has_Document_15 + Has_Document_16 +
    Has_Document_17 + Has_Document_18 + Has_Document_19 + 
    Has_Document_20 + Has_Document_21 AS Num_Of_Docs      -- Total number of documents

    FROM Bank_loan.silver.application_cleaned


