## 🏦 Home Credit Default Risk Prediction
#### An End-to-End Data Engineering & Machine Learning Pipeline

This project delivers a production-grade Data Warehouse (DW) and ML-ready data pipeline for the complex Home Credit Default Risk dataset.
The objective was to seamlessly transform raw, fragmented data from multiple transactional sources into a single, clean, feature-rich analytical dataset optimized for machine learning.
------------------------------------------
#### 🎯 Project Overview

The solution follows a full end-to-end data engineering lifecycle:

🔹 Designing a scalable ETL pipeline with T-SQL

🔹 Building a Star Schema to support analytics and ML

🔹 Performing extensive data cleaning & standardization

🔹 Engineering high-value aggregated features

🔹 Producing a governed ML-Ready Wide Table

🔹 Ensuring consistent logic across both Train & Test sets (no leakage)

This guarantees high data quality, strong analytical performance, and complete traceability from raw to gold.

-----------------------------------------------------
### 🌊 Data Warehouse Architecture (ETL Pipeline)

The project adopts the classical Bronze → Silver → Gold layered DWH pattern:

#### 1. 🟫 Bronze Layer

Example: bronze.application

Raw ingestion of source CSV files

Combined Train + Test into a unified structure

Stored with minimal transformation

#### 2. ⚪ Silver Layer

Example: silver.application_cleaned
Data cleaning and standardization:

Applied TRY_CAST to safely convert >50 numeric columns without breaking (prevents Error 8114)

Normalized categorical fields using UPPER(), TRIM()

Corrected domain anomalies (e.g., mapping 365243 days → 0)

Enforced Primary Keys and Foreign Keys to preserve integrity

#### 3. 🟡 Gold Layer

Examples:
gold.Dim_Customer · gold.Dim_External_Data · gold.Fact_Loan

Implemented a performant Star Schema

Created aggregated customer-level metrics

Built analytical tables optimized for BI & ML

Generated the final ML-Ready Wide Table

---------------------------------------------

### 🛠️ Key Technical Achievements
#### 1. Advanced T-SQL & Dimensional Modeling

Designed a clean and efficient Star Schema where all Dimension tables are fully aggregated at the Customer level, resulting in one row per customer in every dimension.

This ensures that all relationships between the Fact table and Dimensions in the Gold Layer are One-to-One (1:1), simplifying joins and guaranteeing stable ML feature consumption.

Built a central Fact table (gold.Fact_Loan) containing application-level attributes, while Dimensions store aggregated historical patterns (credit, bureau, POS, etc.).

Applied safe, defensive casting (TRY_CAST) and strict data-quality rules to eliminate noisy or malformed raw data.

Enforced robust Primary Key & Foreign Key constraints to guarantee referential integrity across Gold Layer tables.

#### 2. High-Value Feature Engineering

Each Dimension table in the Gold layer represents customer-level aggregated behavior, computed from multi-row transactional sources:

📌 Payment Behavior (installments_payments → Dim_Installments)
Metric	Description
Avg_Late_Days_Paid	Average lateness across all installment payments
Max_Underpayment	Maximum recorded underpayment amount
📌 Credit Card Behavior (credit_card_balance,POS_cash_balance → Dim_Credit_Card)
Metric	Description
CC_Avg_Utilization	Average percentage of credit limit used
CC_Max_Balance	Maximum historical outstanding balance
📌 Bureau Credit History (bureau → Dim_Bureau)
Metric	Description
Total_Active_Credit	Total active credit amount across all bureau loans
Mean_Max_DPD	Mean of maximum days past due
Total_Bureau_Loans	Number of credit bureau loans
📌 Derived Application-Level Features (application → Fact_Loan)
Metric	Logic
Loan_Term_Months	Calculated as Loan Amount ÷ Annuity Amount
🚀 Final ML-Ready Wide Table

----------------------------------------------
Because all Dimension tables are aggregated per customer, the final output view
gold.Final_ML_Training_Data_View is built entirely from 1:1 joins, producing a stable, leakage-free, and ML-friendly wide table where:

Each customer = 1 row

All historical behavior is pre-aggregated

No exploding rows, no duplicates, no many-to-many issues

Zero leakage (same logic applied to Train & Test)
The curated dataset is exposed through gold.Final_ML_Training_Data_View, built via LEFT JOINs across all Dimensions and Fact tables.
-----------------------------------------------
Structured Sections
🧾 Identifiers & Target

Customer_ID

Target_Flag (default risk)

Is_Test_Flag

💰 Loan Attributes

Loan_Amount

Annuity_Amount

Loan_Term_Months

Item_Price

👤 Customer Demographics

Gender

Age_years

Total_income

Employed_years

Education_type

📊 Risk Indicators

Ext_Source_1/2/3

Region_Rating_Client

Total_Geo_Mismatches

🧮 Credit History & Behavior

Total_Active_Credit

Max_Overdue

Mean_Days_Credit

Avg_Late_Days_Paid

POS_Avg_DPD

CC_Avg_Utilization

----------------------------------------------
This structure ensures direct, zero-prep consumption for ML models in Pandas, Scikit-learn, or XGBoost.

📚 Technology Stack
🗄️ Data Engineering

SQL Server

T-SQL

Stored Procedures, Views, Constraints

Dimensional Modeling (Star Schema)

ETL Pipelines

-------------------------------
🤖 data analysis Machine Learning to getting predictions  (Next Phase)

Python

Pandas

Scikit-learn

XGBoost

🧩 Project Highlights

✔ Fully automated ETL pipeline
✔ Clean, validated, leakage-free training dataset
✔ Star Schema design ensures performance & maintainability
✔ High-value aggregated features boost model predictive power
✔ Production-ready data governance principles applied
