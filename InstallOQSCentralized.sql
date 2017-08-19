/*********************************************************************************************
Open Query Store
Centralized version
v0.3 - August 2017

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

-- Make sure there is a database named OpenQueryStore present inside the instance
-- The OpenQueryStore database is where all the information is stored

USE [OpenQueryStore]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ActivityLog](
	[LogID] [int] IDENTITY(1,1) NOT NULL,
	[LogRunID] [int] NULL,
	[DateTime] [datetime] NULL,
	[Message] [varchar](250) NULL,
 CONSTRAINT [PK_Log] PRIMARY KEY CLUSTERED 
(
	[LogID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Databases](
	[database_id] [int] IDENTITY(1,1) NOT NULL,
	[database_name] [nvarchar](150) NULL,
 CONSTRAINT [PK_DatabaseID] PRIMARY KEY CLUSTERED 
(
	[database_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Intervals](
	[IntervalId] [int] IDENTITY(1,1) NOT NULL,
	[IntervalStart] [datetime] NULL,
	[IntervalEnd] [datetime] NULL,
 CONSTRAINT [PK_Intervals] PRIMARY KEY CLUSTERED 
(
	[IntervalId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[PlanDBID](
	[plan_handle] [varbinary](64) NOT NULL,
	[dbid] [int] NOT NULL,
 CONSTRAINT [PK_PlanDBID] PRIMARY KEY CLUSTERED 
(
	[plan_handle] ASC,
	[dbid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Plans](
	[plan_id] [int] IDENTITY(1,1) NOT NULL,
	[plan_MD5] [varbinary](32) NOT NULL,
	[plan_handle] [varbinary](64) NULL,
	[plan_firstfound] [datetime] NULL,
	[plan_database] [nvarchar](150) NULL,
	[plan_refcounts] [int] NULL,
	[plan_usecounts] [int] NULL,
	[plan_sizeinbytes] [int] NULL,
	[plan_type] [nvarchar](50) NULL,
	[plan_objecttype] [nvarchar](20) NULL,
	[plan_executionplan] [xml] NULL,
 CONSTRAINT [PK_Plans] PRIMARY KEY CLUSTERED 
(
	[plan_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [dbo].[Queries](
	[query_id] [int] IDENTITY(1,1) NOT NULL,
	[plan_id] [int] NOT NULL,
	[query_hash] [binary](8) NULL,
	[query_plan_MD5] [varbinary](72) NULL,
	[query_statement_text] [nvarchar](max) NULL,
	[query_statement_start_offset] [int] NULL,
	[query_statement_end_offset] [int] NULL,
	[query_creation_time] [datetime] NULL,
 CONSTRAINT [PK_Queries] PRIMARY KEY CLUSTERED 
(
	[query_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [dbo].[QueryRuntimeStats](
	[query_id] [int] NOT NULL,
	[interval_id] [int] NOT NULL,
	[creation_time] [datetime] NOT NULL,
	[last_execution_time] [datetime] NOT NULL,
	[execution_count] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[last_elapsed_time] [bigint] NOT NULL,
	[min_elapsed_time] [bigint] NOT NULL,
	[max_elapsed_time] [bigint] NOT NULL,
	[avg_elapsed_time] [bigint] NOT NULL,
	[total_rows] [bigint] NOT NULL,
	[last_rows] [bigint] NOT NULL,
	[min_rows] [bigint] NOT NULL,
	[max_rows] [bigint] NOT NULL,
	[avg_rows] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[last_worker_time] [bigint] NOT NULL,
	[min_worker_time] [bigint] NOT NULL,
	[max_worker_time] [bigint] NOT NULL,
	[avg_worker_time] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[last_physical_reads] [bigint] NOT NULL,
	[min_physical_reads] [bigint] NOT NULL,
	[max_physical_reads] [bigint] NOT NULL,
	[avg_physical_reads] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[last_logical_reads] [bigint] NOT NULL,
	[min_logical_reads] [bigint] NOT NULL,
	[max_logical_reads] [bigint] NOT NULL,
	[avg_logical_reads] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[last_logical_writes] [bigint] NOT NULL,
	[min_logical_writes] [bigint] NOT NULL,
	[max_logical_writes] [bigint] NOT NULL,
	[avg_logical_writes] [bigint] NOT NULL,
 CONSTRAINT [PK_QueryRuntimeStats] PRIMARY KEY CLUSTERED 
(
	[query_id] ASC,
	[interval_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[WaitStats](
	[interval_id] [int] NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[waiting_tasks_count] [bigint] NOT NULL,
	[wait_time_ms] [bigint] NOT NULL,
	[max_wait_time_ms] [bigint] NOT NULL,
	[signal_wait_time_ms] [bigint] NOT NULL,
 CONSTRAINT [PK_WaitStats] PRIMARY KEY CLUSTERED 
(
	[interval_id] ASC,
	[wait_type] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create the OQS query_stats view as a version specific abstraction of sys.dm_exec_query_stats
DECLARE @MajorVersion   TINYINT,
        @MinorVersion   TINYINT,
        @Version        NVARCHAR(128),
        @ViewDefinition NVARCHAR(MAX);

SELECT @Version = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR);

SELECT @MajorVersion = PARSENAME(CONVERT(VARCHAR(32), @Version), 4),
       @MinorVersion = PARSENAME(CONVERT(VARCHAR(32), @Version), 3);

SET @ViewDefinition
    = 'CREATE VIEW [QueryStats]
AS
SELECT [sql_handle],
       [statement_start_offset],
       [statement_end_offset],
       [plan_generation_num],
       [plan_handle],
       [creation_time],
       [last_execution_time],
       [execution_count],
       [total_worker_time],
       [last_worker_time],
       [min_worker_time],
       [max_worker_time],
       [total_physical_reads],
       [last_physical_reads],
       [min_physical_reads],
       [max_physical_reads],
       [total_logical_writes],
       [last_logical_writes],
       [min_logical_writes],
       [max_logical_writes],
       [total_logical_reads],
       [last_logical_reads],
       [min_logical_reads],
       [max_logical_reads],
       [total_clr_time],
       [last_clr_time],
       [min_clr_time],
       [max_clr_time],
       [total_elapsed_time],
       [last_elapsed_time],
       [min_elapsed_time],
       [max_elapsed_time],' + CASE
                                   WHEN @MajorVersion = 9 THEN 'CAST(NULL as binary (8)) '
                                   ELSE '' END + '[query_hash],' + -- query_hash appears in sql 2008
CASE
     WHEN @MajorVersion = 9 THEN 'CAST(NULL as binary (8)) '
     ELSE '' END + '[query_plan_hash],' + -- query_plan_hash appears in sql 2008
CASE
     WHEN @MajorVersion = 9
       OR (   @MajorVersion = 10
        AND   @MinorVersion < 50) THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_rows],' + -- total_rows appears in sql 2008r2
CASE
     WHEN @MajorVersion = 9
       OR (   @MajorVersion = 10
        AND   @MinorVersion < 50) THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_rows],' + -- last_rows appears in sql 2008r2
CASE
     WHEN @MajorVersion = 9
       OR (   @MajorVersion = 10
        AND   @MinorVersion < 50) THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_rows],' + -- min_rows appears in sql 2008r2
CASE
     WHEN @MajorVersion = 9
       OR (   @MajorVersion = 10
        AND   @MinorVersion < 50) THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_rows],' + -- max_rows appears in sql 2008r2
CASE
     WHEN @MajorVersion < 12 THEN 'CAST(NULL as varbinary (64)) '
     ELSE '' END + '[statement_sql_handle],' + -- statement_sql_handle appears in sql 2014
CASE
     WHEN @MajorVersion < 12 THEN 'CAST(NULL as bigint) '
     ELSE '' END + '[statement_context_id],' + -- statement_context_id appears in sql 2014
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_dop],' + -- total_dop appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_dop],' + -- last_dop appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_dop],' + -- min_dop appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_dop],' + -- max_dop appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_grant_kb],' + -- total_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_grant_kb],' + -- last_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_grant_kb],' + -- min_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_grant_kb],' + -- max_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_used_grant_kb],' + -- total_used_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_used_grant_kb],' + -- last_used_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_used_grant_kb],' + -- min_rows appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_used_grant_kb],' + -- max_used_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_ideal_grant_kb],' + -- total_ideal_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_ideal_grant_kb],' + -- last_ideal_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_ideal_grant_kb],' + -- min_ideal_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_ideal_grant_kb],' + -- max_ideal_grant_kb appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_reserved_threads],' + -- total_reserved_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_reserved_threads],' + -- last_reserved_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_reserved_threads],' + -- min_reserved_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_reserved_threads],' + -- max_reserved_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[total_used_threads],' + -- total_used_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[last_used_threads],' + -- last_used_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[min_used_threads],' + -- min_used_threads appears in sql 2016
CASE
     WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) '
     ELSE '' END + '[max_used_threads]' + -- max_used_threads appears in sql 2016
' FROM [sys].[dm_exec_query_stats];';

EXEC (@ViewDefinition);
GO

-- Create the OQS Purge OQS Stored Procedure
CREATE PROCEDURE [PurgeOQS]
AS
TRUNCATE TABLE [ActivityLog];
TRUNCATE TABLE [Intervals];
TRUNCATE TABLE [PlanDBID];
TRUNCATE TABLE [Plans];
TRUNCATE TABLE [Queries];
TRUNCATE TABLE [QueryRuntimeStats];
TRUNCATE TABLE [WaitStats];
GO

-- Create the OQS Gather_Statistics Stored Procedure
CREATE PROCEDURE [dbo].[GatherStatistics]
    @debug INT = 0,
    @logmode INT = 0
AS
DECLARE @log_logrunid INT;
DECLARE @log_newplans INT;
DECLARE @log_newqueries INT;
DECLARE @log_runtime_stats INT;
DECLARE @log_wait_stats INT;

BEGIN

    SET NOCOUNT ON;

    IF @logmode = 1
    BEGIN
        SET @log_logrunid = (SELECT ISNULL(MAX([LogRunID]), 0) + 1 FROM [ActivityLog]);
    END;

    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore capture script started...');
    END;

    -- Create a new interval
    INSERT INTO [Intervals] ([IntervalStart])
    VALUES (GETDATE());

    -- We capture plans based on databases that are present in the Databases table of the OpenQueryStore database
    INSERT INTO [PlanDBID] ([plan_handle],
                                  [dbid])
    SELECT [plan_handle],
           CONVERT(INT, [pvt].[dbid])
      FROM (   SELECT [plan_handle],
                      [epa].[attribute],
                      [epa].[value]
                 FROM [sys].[dm_exec_cached_plans]
                OUTER APPLY [sys].[dm_exec_plan_attributes]([plan_handle]) AS [epa]
                WHERE [cacheobjtype] = 'Compiled Plan') AS [ecpa]
      PIVOT (   MAX([value])
                FOR [attribute] IN ("dbid", "sql_handle")) AS [pvt]
     WHERE [plan_handle] NOT IN (SELECT [plan_handle] FROM [PlanDBID])
       AND [pvt].[dbid] IN (select db_id(database_name) FROM Databases)
     ORDER BY [pvt].[sql_handle]

    -- Start execution plan insertion
    -- Get plans from the plan cache that do not exist in the OQS_Plans table
    -- for the database on the current context
    ;
    WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
    INSERT INTO [Plans] ([plan_MD5],
                               [plan_handle],
                               [plan_firstfound],
                               [plan_database],
                               [plan_refcounts],
                               [plan_usecounts],
                               [plan_sizeinbytes],
                               [plan_type],
                               [plan_objecttype],
                               [plan_executionplan])
    SELECT SUBSTRING([master].[sys].[fn_repl_hash_binary](CONVERT(VARBINARY(MAX), [n].[query]('.'))), 1, 32),
           [cp].[plan_handle],
           GETDATE(),
           DB_NAME([pd].[dbid]),
           [cp].[refcounts],
           [cp].[usecounts],
           [cp].[size_in_bytes],
           [cp].[cacheobjtype],
           [cp].[objtype],
           [qp].[query_plan]
      FROM [PlanDBID] AS [pd]
     INNER JOIN [sys].[dm_exec_cached_plans] AS [cp]
        ON [pd].[plan_handle] = [cp].[plan_handle]
     CROSS APPLY [sys].[dm_exec_query_plan]([cp].[plan_handle]) AS [qp]
     CROSS APPLY [query_plan].[nodes]('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/RelOp') AS [q]([n])
     CROSS APPLY [sys].[dm_exec_sql_text]([cp].[plan_handle])
     WHERE [cp].[cacheobjtype]                                               = 'Compiled Plan'
       AND ([qp].[query_plan] IS NOT NULL)
       AND CONVERT(
               VARBINARY,
               SUBSTRING([master].[sys].[fn_repl_hash_binary](CONVERT(VARBINARY(MAX), [n].[query]('.'))), 1, 32)) NOT IN (SELECT [plan_MD5] FROM [Plans])
       AND [qp].[query_plan].[exist]('//ColumnReference[@Schema = "[oqs]"]') = 0;

    SET @log_newplans = @@ROWCOUNT;

    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid,
                GETDATE(),
                'OpenQueryStore captured ' + CONVERT(VARCHAR, @log_newplans) + ' new plan(s)...');
    END

	-- Grab all of the queries (statement level) that are connected to the plans inside the OQS
    ;
    WITH [CTE_Queries] ([plan_id], [plan_handle], [plan_MD5])
      AS (SELECT [plan_id],
                 [plan_handle],
                 [plan_MD5]
            FROM [Plans])
    INSERT INTO [Queries] ([plan_id],
                                 [query_hash],
                                 [query_plan_MD5],
                                 [query_statement_text],
                                 [query_statement_start_offset],
                                 [query_statement_end_offset],
                                 [query_creation_time])
    SELECT [cte].[plan_id],
           [qs].[query_hash],
           ([cte].[plan_MD5] + [qs].[query_hash]) AS [Query_plan_MD5],
           SUBSTRING(
               [st].[text],
               ([qs].[statement_start_offset] / 2) + 1,
               ((CASE [qs].[statement_end_offset]
                      WHEN-1 THEN DATALENGTH([st].[text])
                      ELSE [qs].[statement_end_offset] END - [qs].[statement_start_offset]) / 2) + 1) AS [statement_text],
           [qs].[statement_start_offset],
           [qs].[statement_end_offset],
           [qs].[creation_time]
      FROM [CTE_Queries] AS [cte]
     INNER JOIN [QueryStats] AS [qs]
        ON [cte].[plan_handle] = [qs].[plan_handle]
     CROSS APPLY [sys].[dm_exec_sql_text]([qs].[sql_handle]) AS [st]
     WHERE ([cte].[plan_MD5] + [qs].[query_hash]) NOT IN (SELECT [query_plan_MD5] FROM [Queries]);

    SET @log_newqueries = @@ROWCOUNT;

    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid,
                GETDATE(),
                'OpenQueryStore captured ' + CONVERT(VARCHAR, @log_newqueries) + ' new queries...');
    END;

    -- Grab the interval_id of the interval we added at the beginning
    DECLARE @Interval_ID INT;
    SET @Interval_ID = IDENT_CURRENT('[Intervals]');

-- Query Runtime Snapshot

    -- Insert runtime statistics for every query statement that is recorded inside the OQS
    INSERT INTO [QueryRuntimeStats] ([query_id],
                                           [interval_id],
                                           [creation_time],
                                           [last_execution_time],
                                           [execution_count],
                                           [total_elapsed_time],
                                           [last_elapsed_time],
                                           [min_elapsed_time],
                                           [max_elapsed_time],
                                           [avg_elapsed_time],
                                           [total_rows],
                                           [last_rows],
                                           [min_rows],
                                           [max_rows],
                                           [avg_rows],
                                           [total_worker_time],
                                           [last_worker_time],
                                           [min_worker_time],
                                           [max_worker_time],
                                           [avg_worker_time],
                                           [total_physical_reads],
                                           [last_physical_reads],
                                           [min_physical_reads],
                                           [max_physical_reads],
                                           [avg_physical_reads],
                                           [total_logical_reads],
                                           [last_logical_reads],
                                           [min_logical_reads],
                                           [max_logical_reads],
                                           [avg_logical_reads],
                                           [total_logical_writes],
                                           [last_logical_writes],
                                           [min_logical_writes],
                                           [max_logical_writes],
                                           [avg_logical_writes])
    SELECT [oqs_q].[query_id],
           @Interval_ID,
           [qs].[creation_time],
           [qs].[last_execution_time],
           [qs].[execution_count],
           [qs].[total_elapsed_time],
           [qs].[last_elapsed_time],
           [qs].[min_elapsed_time],
           [qs].[max_elapsed_time],
           0,
           [qs].[total_rows],
           [qs].[last_rows],
           [qs].[min_rows],
           [qs].[max_rows],
           0,
           [qs].[total_worker_time],
           [qs].[last_worker_time],
           [qs].[min_worker_time],
           [qs].[max_worker_time],
           0,
           [qs].[total_physical_reads],
           [qs].[last_physical_reads],
           [qs].[min_physical_reads],
           [qs].[max_physical_reads],
           0,
           [qs].[total_logical_reads],
           [qs].[last_logical_reads],
           [qs].[min_logical_reads],
           [qs].[max_logical_reads],
           0,
           [qs].[total_logical_writes],
           [qs].[last_logical_writes],
           [qs].[min_logical_writes],
           [qs].[max_logical_writes],
           0
      FROM [Queries] AS [oqs_q]
     INNER JOIN [QueryStats] AS [qs]
        ON (   [oqs_q].[query_hash]                   = [qs].[query_hash]
         AND   [oqs_q].[query_statement_start_offset] = [qs].[statement_start_offset]
         AND   [oqs_q].[query_statement_end_offset]   = [qs].[statement_end_offset]
         AND   [oqs_q].[query_creation_time]          = [qs].[creation_time]);

    -- DEBUG: Get current info of the QRS table
    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of captured runtime statistics';
        SELECT *
          FROM [QueryRuntimeStats];
    END;

    -- Wait stats snapshot
	INSERT INTO [WaitStats]
		(
			[interval_id],
			[wait_type],
			[waiting_tasks_count],
			[wait_time_ms],
			[max_wait_time_ms],
			[signal_wait_time_ms]
		)
	SELECT
		@Interval_ID,
		wait_type,           
		waiting_tasks_count, 
		wait_time_ms,        
		max_wait_time_ms,    
		signal_wait_time_ms  
	FROM
		sys.dm_os_wait_stats
	WHERE
		(waiting_tasks_count > 0 AND wait_time_ms > 0);
	
	-- DEBUG: Get current info of the wait stats table
    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of wait stats';
        SELECT *
          FROM [WaitStats] AS [WS];
    END;

    -- Close the interval now that the statistics are in
    UPDATE [Intervals]
       SET [IntervalEnd] = GETDATE()
     WHERE [IntervalId] = (SELECT MAX([IntervalId]) - 1 FROM [Intervals]);

    -- Now that we have the runtime statistics inside the OQS we need to perform some calculations
    -- so we can see query performance per interval captured

    -- First thing we need is a temporary table to hold our calculated deltas
    IF OBJECT_ID('tempdb..#OQS_Runtime_Stats') IS NOT NULL
    BEGIN
        DROP TABLE [#OQS_Runtime_Stats];
    END

    IF OBJECT_ID('tempdb..#OQS_Wait_Stats') IS NOT NULL
    BEGIN
        DROP TABLE [#OQS_Wait_Stats];
    END

    -- Calculate Deltas for Runtime stats
;
    WITH [CTE_Update_Runtime_Stats] ([query_id], [interval_id], [execution_count], [total_elapsed_time], [total_rows],
                                     [total_worker_time], [total_physical_reads], [total_logical_reads],
                                     [total_logical_writes])
      AS (SELECT [query_id],
                 [interval_id],
                 [execution_count],
                 [total_elapsed_time],
                 [total_rows],
                 [total_worker_time],
                 [total_physical_reads],
                 [total_logical_reads],
                 [total_logical_writes]
            FROM [QueryRuntimeStats]
           WHERE [interval_id] = (SELECT MAX([IntervalId]) - 1 FROM [Intervals]))
    SELECT [cte].[query_id],
           [cte].[interval_id],
           ([qrs].[execution_count] - [cte].[execution_count]) AS [Delta Exec Count],
           ([qrs].[total_elapsed_time] - [cte].[total_elapsed_time]) AS [Delta Time],
           ISNULL(
               (([qrs].[total_elapsed_time] - [cte].[total_elapsed_time])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Time],
           ([qrs].[total_rows] - [cte].[total_rows]) AS [Delta Total Rows],
           ISNULL(
               (([qrs].[total_rows] - [cte].[total_rows])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Rows],
           ([qrs].[total_worker_time] - [cte].[total_worker_time]) AS [Delta Total Worker Time],
           ISNULL(
               (([qrs].[total_worker_time] - [cte].[total_worker_time])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Worker Time],
           ([qrs].[total_physical_reads] - [cte].[total_physical_reads]) AS [Delta Total Phys Reads],
           ISNULL(
               (([qrs].[total_physical_reads] - [cte].[total_physical_reads])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Phys reads],
           ([qrs].[total_logical_reads] - [cte].[total_logical_reads]) AS [Delta Total Log Reads],
           ISNULL(
               (([qrs].[total_logical_reads] - [cte].[total_logical_reads])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Log reads],
           ([qrs].[total_logical_writes] - [cte].[total_logical_writes]) AS [Delta Total Log Writes],
           ISNULL(
               (([qrs].[total_logical_writes] - [cte].[total_logical_writes])
                / NULLIF(([qrs].[execution_count] - [cte].[execution_count]), 0)),
               0) AS [Avg. Log writes]
    INTO   [#OQS_Runtime_Stats]
      FROM [CTE_Update_Runtime_Stats] AS [cte]
     INNER JOIN [QueryRuntimeStats] AS [qrs]
        ON [cte].[query_id] = [qrs].[query_id]
     WHERE [qrs].[interval_id] = (SELECT MAX([IntervalId]) FROM [Intervals]);

    SET @log_runtime_stats = @@ROWCOUNT;

    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid,
                GETDATE(),
                'OpenQueryStore captured ' + CONVERT(VARCHAR, @log_runtime_stats) + ' runtime statistics...');
    END;

    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of runtime statistics deltas';
        SELECT *
          FROM [#OQS_Runtime_Stats];
    END;

    -- Update the runtime statistics of the queries captured in the previous interval
    -- with the delta runtime information
    UPDATE [qrs]
       SET [qrs].[execution_count] = [tqrs].[Delta Exec Count],
           [qrs].[total_elapsed_time] = [tqrs].[Delta Time],
           [qrs].[avg_elapsed_time] = [tqrs].[Avg. Time],
           [qrs].[total_rows] = [tqrs].[Delta Total Rows],
           [qrs].[avg_rows] = [tqrs].[Avg. Rows],
           [qrs].[total_worker_time] = [tqrs].[Delta Total Worker Time],
           [qrs].[avg_worker_time] = [tqrs].[Avg. Worker Time],
           [qrs].[total_physical_reads] = [tqrs].[Delta Total Phys Reads],
           [qrs].[avg_physical_reads] = [tqrs].[Avg. Phys reads],
           [qrs].[total_logical_reads] = [tqrs].[Delta Total Log Reads],
           [qrs].[avg_logical_reads] = [tqrs].[Avg. Log reads],
           [qrs].[total_logical_writes] = [tqrs].[Delta Total Log Writes],
           [qrs].[avg_logical_writes] = [tqrs].[Avg. Log writes]
      FROM [QueryRuntimeStats] AS [qrs]
     INNER JOIN [#OQS_Runtime_Stats] AS [tqrs]
        ON (   [qrs].[interval_id] = [tqrs].[interval_id]
         AND   [qrs].[query_id]    = [tqrs].[query_id]);

    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of runtime statistics after delta update';
        SELECT *
          FROM [QueryRuntimeStats];
    END;

    -- Calculate Deltas for Wait stats
;
    WITH [CTE_Update_wait_Stats] (
			[interval_id],
			[wait_type],
			[waiting_tasks_count],
			[wait_time_ms],
			[max_wait_time_ms],
			[signal_wait_time_ms]
		)
      AS (SELECT [WS].[interval_id],
                 [WS].[wait_type],
                 [WS].[waiting_tasks_count],
                 [WS].[wait_time_ms],
                 [WS].[max_wait_time_ms],
                 [WS].[signal_wait_time_ms]
            FROM [WaitStats] AS [WS]
           WHERE [interval_id] = (SELECT MAX([IntervalId]) - 1 FROM [Intervals]))
    SELECT [cte].[wait_type],
           [cte].[interval_id],
           ([ws].[waiting_tasks_count] - [cte].[waiting_tasks_count]) AS [Delta Waiting Tasks Count],
           ([ws].[wait_time_ms] - [cte].[wait_time_ms]) AS [Delta Wait Time ms],
           (ws.[max_wait_time_ms] - cte.[max_wait_time_ms]) AS [Delta Max Wait Time ms],
		   (ws.[signal_wait_time_ms] - cte.[signal_wait_time_ms]) AS [Delta Signal Wait Time ms]
    INTO   [#OQS_Wait_Stats]
      FROM [CTE_Update_wait_Stats] AS [cte]
     INNER JOIN [WaitStats] AS [WS]
        ON [cte].[wait_type] = [WS].[wait_type]
     WHERE [WS].[interval_id] = (SELECT MAX([IntervalId]) FROM [Intervals]);

    SET @log_wait_stats = @@ROWCOUNT;

    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid,
                GETDATE(),
                'OpenQueryStore captured ' + CONVERT(VARCHAR, @log_wait_stats) + ' wait statistics...');
    END;

    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of wait stats deltas';
        SELECT *
          FROM [#OQS_Wait_Stats];
    END;


    -- Update the runtime statistics of the queries captured in the previous interval
    -- with the delta runtime information
    UPDATE [WS]
       SET [WS].[waiting_tasks_count] = [tws].[Delta Waiting Tasks Count],
           [WS].[wait_time_ms] = [Delta Wait Time ms],
           [WS].[max_wait_time_ms] =  [Delta Max Wait Time ms],
           [WS].[signal_wait_time_ms] = [Delta Signal Wait Time ms]
      FROM [WaitStats] AS [WS]
     INNER JOIN [#OQS_Wait_Stats] AS [tws]
        ON (   [ws].[interval_id] = [tws].[interval_id]
         AND   [ws].[wait_type]    = [tws].[wait_type]);

    IF @debug = 1
    BEGIN
        SELECT 'Snapshot of runtime statistics after delta update';
        SELECT *
          FROM [WaitStats] AS [WS];
    END;

    -- Remove the wait stats delta's where 0 waits occured
    DELETE FROM [WaitStats]
    WHERE (waiting_tasks_count = 0 AND interval_id = (SELECT MAX([IntervalId]) - 1 FROM [Intervals]))

    -- And we are done!
    IF @logmode = 1
    BEGIN
        INSERT INTO [ActivityLog] ([LogRunID],
                                         [DateTime],
                                         [Message])
        VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore capture script finished...');
    END;

END;

GO

-- Finished base installation!