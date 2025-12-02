USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Bank_loan')
BEGIN
    ALTER DATABASE Bank_loan SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Bank_loan;
END;
GO

CREATE DATABASE Bank_loan;
GO


USE Bank_loan;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO
