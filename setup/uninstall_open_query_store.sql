/*********************************************************************************************
Open Query Store
Uninstall Open Query Store
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
-- Now perform the nuke process

IF EXISTS ( SELECT * FROM [sys].[services] AS [S] WHERE [S].[name] = 'oqs_service' )
    BEGIN
        DROP SERVICE [oqs_service];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[service_queues] AS [SQ]
              WHERE  [SQ].[name] = 'oqs_scheduler'
          )
    BEGIN
        DROP QUEUE [oqs].[oqs_scheduler];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[oqs].[stop_scheduler]' )
          )
    BEGIN
        DROP PROC [oqs].[stop_scheduler];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[oqs].[start_scheduler]' )
          )
    BEGIN
        DROP PROC [oqs].[start_scheduler];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[oqs].[gather_statistics]' )
          )
    BEGIN
        DROP PROC [oqs].[gather_statistics];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[oqs].[activate_oqs_scheduler]' )
          )
    BEGIN
        DROP PROC [oqs].[activate_oqs_scheduler];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[oqs].[purge_oqs]' )
          )
    BEGIN
        DROP PROC [oqs].[purge_oqs];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[collection_metadata]' )
          )
    BEGIN
        DROP TABLE [oqs].[collection_metadata];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[intervals]' )
          )
    BEGIN
        DROP TABLE [oqs].[intervals];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[plans]' )
          )
    BEGIN
        DROP TABLE [oqs].[plans];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[queries]' )
          )
    BEGIN
        DROP TABLE [oqs].[queries];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[query_runtime_stats]' )
          )
    BEGIN
        DROP TABLE [oqs].[query_runtime_stats];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[activity_log]' )
          )
    BEGIN
        DROP TABLE [oqs].[activity_log];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[plan_dbid]' )
          )
    BEGIN
        DROP TABLE [oqs].[plan_dbid];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[wait_stats]' )
          )
    BEGIN
        DROP TABLE [oqs].[wait_stats];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[monitored_databases]' )
          )
    BEGIN
        DROP TABLE [oqs].[monitored_databases];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[tables] AS [T]
              WHERE  [T].[object_id] = OBJECT_ID( N'[oqs].[wait_type_filter]' )
          )
    BEGIN
        DROP TABLE [oqs].[wait_type_filter];
    END;
	
IF EXISTS (   SELECT *
              FROM   [sys].[views] AS [V]
              WHERE  [V].[object_id] = OBJECT_ID( N'[oqs].[query_stats]' )
          )
    BEGIN
        DROP VIEW [oqs].[query_stats];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[server_principals] AS [SP]
              WHERE  [SP].[name] = 'open_query_store'
          )
    BEGIN
        DROP LOGIN [open_query_store];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[certificates] AS [C]
              WHERE  [C].[name] = 'open_query_store'
          )
    BEGIN
        DROP CERTIFICATE [open_query_store];
    END;

IF EXISTS ( SELECT * FROM [sys].[schemas] AS [S] WHERE [S].[name] = 'oqs' )
    BEGIN
        EXEC ( 'DROP SCHEMA oqs' );
    END;

USE [msdb];
GO
IF EXISTS (   SELECT *
              FROM   [dbo].[sysjobs] AS [S]
              WHERE  [S].[name] = 'Open Query Store - Data Collection'
          )
    BEGIN
        EXECUTE [dbo].[sp_delete_job] @job_name = 'Open Query Store - Data Collection',
                                      @delete_history = 1,
                                      @delete_unused_schedule = 1;
    END;

IF EXISTS (   SELECT [name]
                  FROM   [msdb].[dbo].[syscategories]
                  WHERE  [name] = N'Open Query Store'
                         AND [category_class] = 1
              )
    BEGIN
        EXEC  [msdb].[dbo].[sp_delete_category] @class = N'JOB', @name = N'Open Query Store';
    END;
    
USE [master];
GO

IF EXISTS (   SELECT *
              FROM   [sys].[procedures] AS [P]
              WHERE  [P].[object_id] = OBJECT_ID( N'[dbo].[open_query_store_startup]' )
          )
    BEGIN
        DROP PROC [dbo].[open_query_store_startup];
    END;

IF EXISTS (   SELECT *
              FROM   [sys].[certificates] AS [C]
              WHERE  [C].[name] = 'open_query_store'
          )
    BEGIN
        DROP CERTIFICATE [open_query_store];
    END;
