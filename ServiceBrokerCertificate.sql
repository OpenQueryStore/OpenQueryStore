-- Replace the file locations to a folder where you can read/write!

CREATE CERTIFICATE [OpenQueryStore]
ENCRYPTION BY PASSWORD = 'OQS1234!'
WITH SUBJECT = 'OpenQueryStore Broker Certificate';
GO

ADD SIGNATURE TO OBJECT::[oqs].[ActivateOQSScheduler]
BY CERTIFICATE [OpenQueryStore]
WITH PASSWORD = 'OQS1234!';
GO

ALTER CERTIFICATE [OpenQueryStore]
REMOVE PRIVATE KEY;
GO

BACKUP CERTIFICATE [OpenQueryStore]
TO FILE = 'D:\temp\OpenQueryStore.CER';
GO

USE MASTER
GO

CREATE CERTIFICATE [OpenQueryStore]
FROM FILE = 'D:\temp\OpenQueryStore.CER';
GO

CREATE LOGIN [OpenQueryStore]
FROM CERTIFICATE [OpenQueryStore];
GO

GRANT AUTHENTICATE SERVER TO [OpenQueryStore];
GRANT VIEW SERVER STATE TO [OpenQueryStore];
GO