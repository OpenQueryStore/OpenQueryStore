/*********************************************************************************************
Open Query Store
Install Service Broker certificate for Open Query Store
v0.4 - August 2017

Copyright:
William Durkin (@sql_williamd) / Enrico van de Laar (@evdlaar)

https://github.com/OpenQueryStore/OpenQueryStore

License:
	This script is free to download and use for personal, educational, and internal
	corporate purposes, provided that this header is preserved. Redistribution or sale
	of this script, in whole or in part, is prohibited without the author's express
	written consent.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
	INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
	IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
	OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

**********************************************************************************************/

-- Certificates are used to provide the security context of OQS
-- This allows us to create a special user with minimal permissions
-- and also to ensure that the credentials can only be used for the
-- OQS stored procedure

-- Step 1:	Connect to the database where you previously installed OQS using the
--			official installer

USE {DatabaseWhereOQSIsRunning}
GO

-- We first create a certificate. Feel free to use a different password, but
-- the password is used only until we have signed the ActivateOQSScheduler
-- The START_DATE and EXPIRY_DATE as necessary to define the "lifetime" of
-- the certificate.

CREATE CERTIFICATE [open_query_store]
    ENCRYPTION BY PASSWORD = 'OQS1234!'
    WITH SUBJECT = 'OpenQueryStore Broker Certificate',
         START_DATE = '2017-06-01',
         EXPIRY_DATE = '2030-06-01';
GO

ADD SIGNATURE TO OBJECT::[oqs].[activate_oqs_scheduler] BY CERTIFICATE [open_query_store] WITH PASSWORD = 'OQS1234!';
GO

ALTER CERTIFICATE [open_query_store] REMOVE PRIVATE KEY;
GO

-- Change the file location to a valid value
-- This needs to be a full folder and filename
-- e.g. C:\Temp\OpenQueryStore.CER
BACKUP CERTIFICATE [open_query_store] TO FILE = '{Enter A File Location accessible by the SQL Server Service Account}';
GO


-- We now move to the master database to import the certificate and associate it with
-- an otherwise unused login: OpenQueryStore

USE [master]
GO

-- Enter the file location previously used to create the certificate
CREATE CERTIFICATE [open_query_store] FROM FILE = '{Enter A File Location accessible by the SQL Server Service Account}';
GO

-- Create the login using the certificate
CREATE LOGIN [open_query_store] FROM CERTIFICATE [open_query_store];
GO

-- Assign the permissions to OpenQueryStore
GRANT AUTHENTICATE SERVER TO [open_query_store];
GRANT VIEW SERVER STATE TO [open_query_store];
GO

-- We are now ready to use OQS inside a secure context
