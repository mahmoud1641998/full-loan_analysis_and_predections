/********************************************************************************************
 PROJECT: Home Credit Default Risk – Data Engineering Pipeline
 SCRIPT: 01 - Environment & Schema Initialization
 PURPOSE:
    - Reset the database environment during development.
    - Create logical schemas for a clean Bronze ? Silver ? Gold Data Lakehouse architecture.
    - Ensure idempotency (script can run multiple times safely).
********************************************************************************************/

-- Switch to master before any DB-level operations
USE master;
GO

/********************************************************************************************
1. DROP EXISTING DATABASE (Development-Only Step)
     Forces SINGLE_USER mode to terminate active connections.
     Ensures clean re-creation of the pipeline during development / testing.
********************************************************************************************/
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Bank_loan')
BEGIN
    ALTER DATABASE Bank_loan SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Bank_loan;
END;
GO

/********************************************************************************************
2. CREATE PROJECT DATABASE
     This database hosts all layers of the ETL pipeline.
     Separates project data from other server databases.
********************************************************************************************/
CREATE DATABASE Bank_loan;
GO

-- Switch to new database
USE Bank_loan;
GO

/********************************************************************************************
 3. CREATE SCHEMAS FOR THE MEDALLION ARCHITECTURE
     bronze ? raw ingested CSV data (as-is)
     silver ? cleaned & standardized tables (TRY_CAST, normalization, value mappings)
     gold   ? dimensional model + aggregated features for ML
********************************************************************************************/

-- Bronze schema (raw landing zone)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- Silver schema (clean, typed, validated data)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- Gold schema (dimensional + aggregated + ML-ready)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

/********************************************************************************************
 END OF SCRIPT
     Database created
     Schemas initialized
     Environment ready for Bronze ingestion scripts
********************************************************************************************/

