/*********************************************************************************************
Open Query Store
Install Service Broker infrastructure for Open Query Store
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

USE [{DatabaseWhereOQSIsRunning}]
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

SET NOCOUNT ON;
GO

-- Enable the Service Broker for the user database
DECLARE @db sysname;

SET @db = DB_NAME();

IF  (   SELECT [is_broker_enabled]
        FROM   [sys].[databases]
        WHERE  [database_id] = DB_ID( @db )
    ) = 0
    BEGIN
        EXEC ( 'ALTER DATABASE ' + @db + ' SET ENABLE_BROKER' );
    END;
GO

-- Create the Service Broker structure
CREATE QUEUE [oqs].[oqs_scheduler];
CREATE SERVICE [oqs_service] ON QUEUE [oqs].[oqs_scheduler] ( [DEFAULT] );
GO

-- This is the stored procedure run by Service Broker to perform the looped execution of data collection
CREATE PROCEDURE [oqs].[activate_oqs_scheduler]
WITH EXECUTE AS OWNER
AS
    BEGIN
        DECLARE @Handle uniqueidentifier,
                @Type   sysname,
                @msg    nvarchar (MAX);
        WAITFOR (   RECEIVE TOP ( 1 ) @Handle = [conversation_handle],
                                      @Type = [message_type_name]
                        FROM [oqs].[oqs_scheduler]
                ),
        TIMEOUT 5000;
        IF @Handle IS NULL RETURN;
        IF @Type = 'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer' -- This is the timer loop for "normal" operation
            BEGIN
                -- Grab the OQS configuration
                DECLARE @Command            nvarchar (2000),
                        @CollectionInterval bigint,
                        @OQSMode            nvarchar (20),
                        @OQSClassicDB       nvarchar (128);

                SELECT @Command            = [CM].[command],
                       @CollectionInterval = [CM].[collection_interval],
                       @OQSMode            = [CM].[oqs_mode],
                       @OQSClassicDB       = [CM].[oqs_classic_db]
                FROM   [oqs].[collection_metadata] AS [CM];

                -- Classic mode only monitors one database (stored in the collection_metadata table)
                -- We need to remove all other entries in monitored_databases to keep the dashboard clean
                -- This "trick" keeps the codebase for both classic and centralized mode cleaner
                IF @OQSMode = N'classic'
                    BEGIN
                        TRUNCATE TABLE [oqs].[monitored_databases];
                        INSERT INTO [oqs].[monitored_databases] ( [database_name] )
                        VALUES ( @OQSClassicDB );
                    END;

                -- Place the OQS collection command into a delayed execution queue in service broker
                BEGIN CONVERSATION TIMER ( @Handle ) TIMEOUT = @CollectionInterval;
                EXEC ( @Command );

            END;
        ELSE END CONVERSATION @Handle;
    END;
GO

-- Add the stored procedure to the queue so it gets activated
ALTER QUEUE [oqs].[oqs_scheduler]
    WITH STATUS = ON,
         RETENTION = OFF,
         ACTIVATION (   STATUS = ON,
                        PROCEDURE_NAME = [oqs].[activate_oqs_scheduler],
                        MAX_QUEUE_READERS = 1,
                        EXECUTE AS OWNER
                    );
GO

-- This is a stored procedure to initiate the Service Broker loop, it can be called manually, or added as a
-- startup procedure to ensure data collection when SQL Server starts up
CREATE PROCEDURE [oqs].[start_scheduler]
AS
    BEGIN
        DECLARE @handle uniqueidentifier;
        SELECT @handle = [conversation_handle]
        FROM   [sys].[conversation_endpoints]
        WHERE  [is_initiator] = 1
               AND [far_service] = 'oqs_service'
               AND [state] <> 'CD';
        IF @@RowCount = 0
            BEGIN
                BEGIN DIALOG CONVERSATION @handle
                    FROM SERVICE [oqs_service]
                    TO SERVICE 'oqs_service'
                    ON CONTRACT [DEFAULT]
                    WITH ENCRYPTION = OFF;

                BEGIN CONVERSATION TIMER ( @handle ) TIMEOUT = 1;
            END;
    END;
GO

-- This is a stored procedure to (temporarilly) stop OQS data collection
CREATE PROCEDURE [oqs].[stop_scheduler]
AS
    BEGIN
        DECLARE @handle uniqueidentifier;
        SELECT @handle = [conversation_handle]
        FROM   [sys].[conversation_endpoints]
        WHERE  [is_initiator] = 1
               AND [far_service] = 'oqs_service'
               AND [state] <> 'CD';
        IF @@RowCount <> 0 END CONVERSATION @handle;
    END;
GO

USE [master];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [dbo].[open_query_store_startup]
AS
    -- This stored procedure is used to activate Open Query Store data collection using Service Broker
    -- The procedure is called at every SQL Server startup
    BEGIN
        SET NOCOUNT ON;
        EXEC {DatabaseWhereOQSIsRunning}.[oqs].[start_scheduler];
    END;
GO

EXEC [sys].[sp_procoption] @ProcName = 'dbo.open_query_store_startup',
                           @OptionName = 'startup',
                           @OptionValue = 'on'
GO