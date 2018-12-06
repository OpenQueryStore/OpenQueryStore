/*********************************************************************************************
Open Query Store
Uninstall Open Query Store (non-database items)
v0.1 - July 2018

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

IF EXISTS (   SELECT *
              FROM   [sys].[server_principals] AS [SP]
              WHERE  [SP].[name] = 'open_query_store'
          )
    BEGIN
        DROP LOGIN [open_query_store];
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
