-- Certificates are used to provide the security context of OQS
-- This allows us to create a special user with minimal permissions
-- and also to ensure that the credentials can only be used for the
-- OQS stored procedure

-- Step 1:	Connect to the database where you previously installed OQS using the
--			official installer

USE {DatabaseWhereOQSIsRunning}


-- We first create a certificate. Feel free to use a different password, but
-- the password is used only until we have signed the ActivateOQSScheduler
-- The START_DATE and EXPIRY_DATE as necessary to define the "lifetime" of
-- the certificate.

CREATE CERTIFICATE [OpenQueryStore]
ENCRYPTION BY PASSWORD = 'OQS1234!'
WITH SUBJECT = 'OpenQueryStore Broker Certificate',
	 START_DATE = '2017-06-01',
	 EXPIRY_DATE = '2030-06-01' ;
GO

ADD SIGNATURE TO OBJECT::[oqs].[ActivateOQSScheduler]
BY CERTIFICATE [OpenQueryStore]
WITH PASSWORD = 'OQS1234!';
GO

ALTER CERTIFICATE [OpenQueryStore]
REMOVE PRIVATE KEY;
GO

-- Change the file location to a valid value
-- This needs to be a full folder and filename
-- e.g. C:\Temp\OpenQueryStore.CER

BACKUP CERTIFICATE [OpenQueryStore]
TO FILE = 'Enter A File Location accessible by the SQL Server Service Account';
GO


-- We now move to the master database to import the certificate and associate it with
-- an otherwise unused login: OpenQueryStore

USE MASTER
GO

-- Enter the file location previously used to create the certificate
CREATE CERTIFICATE [OpenQueryStore]
FROM FILE = 'Enter A File Location accessible by the SQL Server Service Account';
GO

-- Create the login using the certificate
CREATE LOGIN [OpenQueryStore]
FROM CERTIFICATE [OpenQueryStore];
GO

-- Assign the permissions to OpenQueryStore
GRANT AUTHENTICATE SERVER TO [OpenQueryStore];
GRANT VIEW SERVER STATE TO [OpenQueryStore];
GO

-- We are now ready to use OQS inside a secure context
