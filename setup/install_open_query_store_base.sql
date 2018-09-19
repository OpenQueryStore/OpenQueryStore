/*********************************************************************************************
Open Query Store
Install table and view infrastructure for Open Query Store
v0.9 - January 2018

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

-- Create the OQS Schema
IF NOT EXISTS ( SELECT * FROM [sys].[schemas] AS [S] WHERE [S].[name] = 'oqs' )
    BEGIN
        EXEC ( 'CREATE SCHEMA oqs' );
    END;
GO
CREATE TABLE [oqs].[collection_metadata]
    (
        [command]                nvarchar (2000) NOT NULL, -- The command that should be executed by Service Broker
        [collection_interval]    bigint          NOT NULL, -- The interval for looped processing (in seconds)
        [oqs_mode]               varchar (11)    NOT NULL, -- The mode that OQS should run in. May only be "classic" or "centralized" 
        [oqs_classic_db]         nvarchar (128)  NOT NULL, -- The database where OQS resides in classic mode (must be filled when classic mode is chosen, ignored by centralized mode)
        [collection_active]      bit             NOT NULL, -- Should OQS be collecting data or not
        [execution_threshold]    tinyint         NOT NULL, -- The minimum executions of a query plan before we consider it for capture in OQS
        [oqs_maximum_size_mb]    smallint        NOT NULL, -- The maximum size in MB that OQS data store should be (actual size can be slightly larger, but this is the "high water mark" to control data collection)
        [data_cleanup_active]    bit             NOT NULL, -- Should OQS automatically clean up old data
        [data_cleanup_threshold] tinyint         NOT NULL, -- How many days should OQS keep data for (automated cleanup removes data older than this)
        [data_cleanup_throttle]  smallint        NOT NULL, -- How many rows can be deleted in one pass. This avoids large deletions from trashing the transaction log and blocking OQS tables.
    );
GO

-- We want to have defaults and checks for certain settings
ALTER TABLE [oqs].[collection_metadata]
ADD CONSTRAINT [chk_oqs_mode]           CHECK ( [oqs_mode] IN ( N'classic', N'centralized' )),
    CONSTRAINT [df_collection_interval] DEFAULT ( 60 )   FOR [collection_interval],
    CONSTRAINT [df_collection_active]   DEFAULT ( 0 )    FOR [collection_active],
    CONSTRAINT [df_execution_threshold] DEFAULT ( 2 )    FOR [execution_threshold],
    CONSTRAINT [df_oqs_maximum_size_mb] DEFAULT ( 100 )  FOR [oqs_maximum_size_mb],
    CONSTRAINT [df_cleanup_active]      DEFAULT ( 0 )    FOR [data_cleanup_active],
    CONSTRAINT [df_cleanup_threshold]   DEFAULT ( 30 )   FOR [data_cleanup_threshold],
    CONSTRAINT [df_cleanup_throttle]    DEFAULT ( 5000 ) FOR [data_cleanup_throttle];


-- Semi-hidden way of documenting the version of OQS that is installed. The value will be automatically bumped upon a new version build/release
EXEC sys.sp_addextendedproperty @name=N'oqs_version', @value=N'2.3.0' , @level0type=N'SCHEMA',@level0name=N'oqs', @level1type=N'TABLE',@level1name=N'collection_metadata'
GO

-- Default values for initial installation = Logging turned on, run every 60 seconds, collection deactivated, execution_threshold = 2 to skip single-use plans
INSERT INTO [oqs].[collection_metadata] (   [command],
                                            [collection_interval],
                                            [oqs_mode],
                                            [oqs_classic_db],
                                            [collection_active],
                                            [execution_threshold],
											[oqs_maximum_size_mb],
                                            [data_cleanup_active],
                                            [data_cleanup_threshold],
                                            [data_cleanup_throttle]
                                        )
VALUES ( N'EXEC [oqs].[gather_statistics] @logmode=1', DEFAULT , '{OQSMode}','{DatabaseWhereOQSIsRunning}',DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT);
GO

CREATE TABLE [oqs].[activity_log]
    (
        [log_id]        [int]           IDENTITY (1, 1) NOT NULL,
        [log_run_id]    [int]           NULL,
        [log_timestamp] [datetime]      NULL,
        [log_message]   [varchar] (250) NULL,
        CONSTRAINT [pk_log_id]
            PRIMARY KEY CLUSTERED ( [log_id] ASC )
    ) ON [PRIMARY];
GO

CREATE TABLE [oqs].[monitored_databases]
    (
        [database_name] [nvarchar] (128) NOT NULL,
        CONSTRAINT [pk_monitored_databases]
            PRIMARY KEY CLUSTERED ( [database_name] ASC )
    ) ON [PRIMARY];
GO

CREATE TABLE [oqs].[intervals]
    (
        [interval_id]    [int]      IDENTITY (1, 1) NOT NULL,
        [interval_start] [datetime] NULL,
        [interval_end]   [datetime] NULL,
        CONSTRAINT [pk_intervals]
            PRIMARY KEY CLUSTERED ( [interval_id] ASC )
    ) ON [PRIMARY];
GO

CREATE TABLE [oqs].[plan_dbid]
    (
        [plan_handle] [varbinary] (64) NOT NULL,
        [dbid]        [int]            NOT NULL,
        CONSTRAINT [pk_plan_dbid]
            PRIMARY KEY CLUSTERED ( [plan_handle] ASC, [dbid] ASC )
    ) ON [PRIMARY];
GO

CREATE TABLE [oqs].[plans]
    (
        [plan_id]                                   [int]              IDENTITY (1, 1) NOT NULL,
        [plan_MD5]                                  [varbinary] (32)   NOT NULL,
        [plan_handle]                               [varbinary] (64)   NULL,
        [plan_firstfound]                           [datetime]         NULL,
        [plan_database]                             [nvarchar] (150)   NULL,
        [plan_refcounts]                            [int]              NULL,
        [plan_usecounts]                            [int]              NULL,
        [plan_sizeinbytes]                          [int]              NULL,
        [plan_type]                                 [nvarchar] (50)    NULL,
        [plan_objecttype]                           [nvarchar] (20)    NULL,
        [plan_executionplan]                        [xml]              NULL,
		[plan_optimization]							[varchar]  (10)    NULL,
		[xml_processed]								[int]			   NULL
        CONSTRAINT [pk_plans]
            PRIMARY KEY CLUSTERED ( [plan_id] ASC )
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

ALTER TABLE [oqs].[plans] ADD  CONSTRAINT [DF_plans_xml_processed]  DEFAULT ((0)) FOR [xml_processed]
GO

CREATE TABLE [oqs].[queries]
    (
        [query_id]                     [int]            IDENTITY (1, 1) NOT NULL,
        [plan_id]                      [int]            NOT NULL,
        [query_hash]                   [binary] (8)     NULL,
        [query_plan_MD5]               [varbinary] (72) NULL,
        [query_statement_text]         [nvarchar] (MAX) NULL,
        [query_statement_start_offset] [int]            NULL,
        [query_statement_end_offset]   [int]            NULL,
        [query_creation_time]          [datetime]       NULL,
        CONSTRAINT [pk_queries]
            PRIMARY KEY CLUSTERED ( [query_id] ASC )
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

CREATE UNIQUE NONCLUSTERED INDEX uncl_queries_cleanup ON [oqs].[queries] ([query_id],[plan_id]);
GO

CREATE TABLE [oqs].[excluded_queries]
    (
        [query_id] int NOT NULL,
        CONSTRAINT [pk_excluded_queries_query_id]
            PRIMARY KEY CLUSTERED ( [query_id] ASC )
    )
GO

CREATE TABLE [oqs].[query_runtime_stats]
    (
        [query_id]             [int]      NOT NULL,
        [interval_id]          [int]      NOT NULL,
        [creation_time]        [datetime] NOT NULL,
        [last_execution_time]  [datetime] NOT NULL,
        [execution_count]      [bigint]   NOT NULL,
        [total_elapsed_time]   [bigint]   NOT NULL,
        [last_elapsed_time]    [bigint]   NOT NULL,
        [min_elapsed_time]     [bigint]   NOT NULL,
        [max_elapsed_time]     [bigint]   NOT NULL,
        [avg_elapsed_time]     [bigint]   NOT NULL,
        [total_rows]           [bigint]   NOT NULL,
        [last_rows]            [bigint]   NOT NULL,
        [min_rows]             [bigint]   NOT NULL,
        [max_rows]             [bigint]   NOT NULL,
        [avg_rows]             [bigint]   NOT NULL,
        [total_worker_time]    [bigint]   NOT NULL,
        [last_worker_time]     [bigint]   NOT NULL,
        [min_worker_time]      [bigint]   NOT NULL,
        [max_worker_time]      [bigint]   NOT NULL,
        [avg_worker_time]      [bigint]   NOT NULL,
        [total_physical_reads] [bigint]   NOT NULL,
        [last_physical_reads]  [bigint]   NOT NULL,
        [min_physical_reads]   [bigint]   NOT NULL,
        [max_physical_reads]   [bigint]   NOT NULL,
        [avg_physical_reads]   [bigint]   NOT NULL,
        [total_logical_reads]  [bigint]   NOT NULL,
        [last_logical_reads]   [bigint]   NOT NULL,
        [min_logical_reads]    [bigint]   NOT NULL,
        [max_logical_reads]    [bigint]   NOT NULL,
        [avg_logical_reads]    [bigint]   NOT NULL,
        [total_logical_writes] [bigint]   NOT NULL,
        [last_logical_writes]  [bigint]   NOT NULL,
        [min_logical_writes]   [bigint]   NOT NULL,
        [max_logical_writes]   [bigint]   NOT NULL,
        [avg_logical_writes]   [bigint]   NOT NULL,
        CONSTRAINT [pk_query_runtime_stats]
            PRIMARY KEY CLUSTERED ( [query_id] ASC, [interval_id] ASC )
    ) ON [PRIMARY];
GO

CREATE UNIQUE NONCLUSTERED INDEX uncl_query_runtime_stats_cleanup ON [oqs].[query_runtime_stats] ([query_id],[interval_id]) INCLUDE ([last_execution_time]);
GO

CREATE TABLE [oqs].[wait_stats]
    (
        [interval_id]         [int]           NOT NULL,
        [wait_type]           [nvarchar] (60) NOT NULL,
        [waiting_tasks_count] [bigint]        NOT NULL,
        [wait_time_ms]        [bigint]        NOT NULL,
        [max_wait_time_ms]    [bigint]        NOT NULL,
        [signal_wait_time_ms] [bigint]        NOT NULL,
        CONSTRAINT [pk_wait_stats]
            PRIMARY KEY CLUSTERED ( [interval_id] ASC, [wait_type] ASC )
    ) ON [PRIMARY];
GO
CREATE TABLE [oqs].[wait_type_filter]
    (
        [wait_type]   nvarchar (60) NOT NULL,
        [oqs_default] bit           NOT NULL
            CONSTRAINT [df_wait_type_filter_oqs_default]
            DEFAULT ( 0 ),
        CONSTRAINT [pk_wait_type_filter]
            PRIMARY KEY CLUSTERED ( [wait_type] )
    ) ON [PRIMARY];
GO
-- Populate the wait_type_filter table with predefined "benign" waits as defined by Paul Randal (https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/)
INSERT INTO [oqs].[wait_type_filter] (   [wait_type],
                                         [oqs_default]
                                     )
            SELECT N'BROKER_EVENTHANDLER', 1
            UNION ALL
            SELECT N'BROKER_RECEIVE_WAITFOR', 1
            UNION ALL
            SELECT N'BROKER_TASK_STOP', 1
            UNION ALL
            SELECT N'BROKER_TO_FLUSH', 1
            UNION ALL
            SELECT N'BROKER_TRANSMITTER', 1
            UNION ALL
            SELECT N'CHECKPOINT_QUEUE', 1
            UNION ALL
            SELECT N'CHKPT', 1
            UNION ALL
            SELECT N'CLR_AUTO_EVENT', 1
            UNION ALL
            SELECT N'CLR_MANUAL_EVENT', 1
            UNION ALL
            SELECT N'CLR_SEMAPHORE', 1
            UNION ALL
            SELECT N'DBMIRROR_DBM_EVENT', 1
            UNION ALL
            SELECT N'DBMIRROR_EVENTS_QUEUE', 1
            UNION ALL
            SELECT N'DBMIRROR_WORKER_QUEUE', 1
            UNION ALL
            SELECT N'DBMIRRORING_CMD', 1
            UNION ALL
            SELECT N'DIRTY_PAGE_POLL', 1
            UNION ALL
            SELECT N'DISPATCHER_QUEUE_SEMAPHORE', 1
            UNION ALL
            SELECT N'EXECSYNC', 1
            UNION ALL
            SELECT N'FSAGENT', 1
            UNION ALL
            SELECT N'FT_IFTS_SCHEDULER_IDLE_WAIT', 1
            UNION ALL
            SELECT N'FT_IFTSHC_MUTEX', 1
            UNION ALL
            SELECT N'HADR_CLUSAPI_CALL', 1
            UNION ALL
            SELECT N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 1
            UNION ALL
            SELECT N'HADR_LOGCAPTURE_WAIT', 1
            UNION ALL
            SELECT N'HADR_NOTIFICATION_DEQUEUE', 1
            UNION ALL
            SELECT N'HADR_TIMER_TASK', 1
            UNION ALL
            SELECT N'HADR_WORK_QUEUE', 1
            UNION ALL
            SELECT N'KSOURCE_WAKEUP', 1
            UNION ALL
            SELECT N'LAZYWRITER_SLEEP', 1
            UNION ALL
            SELECT N'LOGMGR_QUEUE', 1
            UNION ALL
            SELECT N'MEMORY_ALLOCATION_EXT', 1
            UNION ALL
            SELECT N'ONDEMAND_TASK_QUEUE', 1
            UNION ALL
            SELECT N'PREEMPTIVE_XE_GETTARGETSTATE', 1
            UNION ALL
            SELECT N'PWAIT_ALL_COMPONENTS_INITIALIZED', 1
            UNION ALL
            SELECT N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', 1
            UNION ALL
            SELECT N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 1
            UNION ALL
            SELECT N'QDS_ASYNC_QUEUE', 1
            UNION ALL
            SELECT N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 1
            UNION ALL
            SELECT N'QDS_SHUTDOWN_QUEUE', 1
            UNION ALL
            SELECT N'REDO_THREAD_PENDING_WORK', 1
            UNION ALL
            SELECT N'REQUEST_FOR_DEADLOCK_SEARCH', 1
            UNION ALL
            SELECT N'RESOURCE_QUEUE', 1
            UNION ALL
            SELECT N'SERVER_IDLE_CHECK', 1
            UNION ALL
            SELECT N'SLEEP_BPOOL_FLUSH', 1
            UNION ALL
            SELECT N'SLEEP_DBSTARTUP', 1
            UNION ALL
            SELECT N'SLEEP_DCOMSTARTUP', 1
            UNION ALL
            SELECT N'SLEEP_MASTERDBREADY', 1
            UNION ALL
            SELECT N'SLEEP_MASTERMDREADY', 1
            UNION ALL
            SELECT N'SLEEP_MASTERUPGRADED', 1
            UNION ALL
            SELECT N'SLEEP_MSDBSTARTUP', 1
            UNION ALL
            SELECT N'SLEEP_SYSTEMTASK', 1
            UNION ALL
            SELECT N'SLEEP_TASK', 1
            UNION ALL
            SELECT N'SLEEP_TEMPDBSTARTUP', 1
            UNION ALL
            SELECT N'SNI_HTTP_ACCEPT', 1
            UNION ALL
            SELECT N'SP_SERVER_DIAGNOSTICS_SLEEP', 1
            UNION ALL
            SELECT N'SQLTRACE_BUFFER_FLUSH', 1
            UNION ALL
            SELECT N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 1
            UNION ALL
            SELECT N'SQLTRACE_WAIT_ENTRIES', 1
            UNION ALL
            SELECT N'WAIT_FOR_RESULTS', 1
            UNION ALL
            SELECT N'WAITFOR', 1
            UNION ALL
            SELECT N'WAITFOR_TASKSHUTDOWN', 1
            UNION ALL
            SELECT N'WAIT_XTP_RECOVERY', 1
            UNION ALL
            SELECT N'WAIT_XTP_HOST_WAIT', 1
            UNION ALL
            SELECT N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 1
            UNION ALL
            SELECT N'WAIT_XTP_CKPT_CLOSE', 1
            UNION ALL
            SELECT N'XE_DISPATCHER_JOIN', 1
            UNION ALL
            SELECT N'XE_DISPATCHER_WAIT', 1
            UNION ALL
            SELECT N'XE_TIMER_EVENT', 1

GO

-- Create the OQS query_stats view as a version specific abstraction of sys.dm_exec_query_stats
DECLARE @MajorVersion   tinyint,
        @MinorVersion   tinyint,
		@BuildVersion	int,
        @Version        nvarchar (128),
        @ViewDefinition nvarchar (MAX);

SELECT @Version = CAST(SERVERPROPERTY( 'ProductVersion' ) AS nvarchar);

SELECT @MajorVersion = PARSENAME( CONVERT( varchar (32), @Version ), 4 ),
       @MinorVersion = PARSENAME( CONVERT( varchar (32), @Version ), 3 ),
       @BuildVersion = PARSENAME( CONVERT( varchar (32), @Version ), 2 );

SET @ViewDefinition = 'CREATE VIEW [oqs].[query_stats]
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
       [max_elapsed_time],' + 
CASE WHEN @MajorVersion = 9 THEN 'CAST(NULL as binary (8)) ' ELSE '' END + '[query_hash],' +											-- query_hash appears in sql 2008
CASE WHEN @MajorVersion = 9 THEN 'CAST(NULL as binary (8)) ' ELSE '' END + '[query_plan_hash],' +										-- query_plan_hash appears in sql 2008
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50) OR ( @MajorVersion = 10 AND @MinorVersion = 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_rows],' +	-- total_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50) OR ( @MajorVersion = 10 AND @MinorVersion = 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_rows],' +	-- last_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50) OR ( @MajorVersion = 10 AND @MinorVersion = 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_rows],' +	-- min_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50) OR ( @MajorVersion = 10 AND @MinorVersion = 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_rows],' +	-- max_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion < 12 THEN 'CAST(NULL as varbinary (64)) ' ELSE '' END + '[statement_sql_handle],' +								-- statement_sql_handle appears in sql 2014
CASE WHEN @MajorVersion < 12 THEN 'CAST(NULL as bigint) ' ELSE '' END + '[statement_context_id],' +										-- statement_context_id appears in sql 2014
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_dop],' +													-- total_dop appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_dop],' +													-- last_dop appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_dop],' +														-- min_dop appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_dop],' +														-- max_dop appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_grant_kb],' +												-- total_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_grant_kb],' +												-- last_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_grant_kb],' +												-- min_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_grant_kb],' +												-- max_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_used_grant_kb],' +											-- total_used_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_used_grant_kb],' +											-- last_used_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_used_grant_kb],' +											-- min_rows appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_used_grant_kb],' +											-- max_used_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_ideal_grant_kb],' +										-- total_ideal_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_ideal_grant_kb],' +											-- last_ideal_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_ideal_grant_kb],' +											-- min_ideal_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_ideal_grant_kb],' +											-- max_ideal_grant_kb appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_reserved_threads],' +										-- total_reserved_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_reserved_threads],' +										-- last_reserved_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_reserved_threads],' +										-- min_reserved_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_reserved_threads],' +										-- max_reserved_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_used_threads],' +											-- total_used_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_used_threads],' +											-- last_used_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_used_threads],' +											-- min_used_threads appears in sql 2016
CASE WHEN @MajorVersion < 13 THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_used_threads]' +												-- max_used_threads appears in sql 2016
' FROM [sys].[dm_exec_query_stats];';

EXEC ( @ViewDefinition );
GO

-- Create the OQS Purge OQS Stored Procedure
CREATE PROCEDURE [oqs].[purge_oqs]
AS
BEGIN
    
    SET NOCOUNT ON

    TRUNCATE TABLE [oqs].[activity_log];
    TRUNCATE TABLE [oqs].[intervals];
    TRUNCATE TABLE [oqs].[plan_dbid];
    TRUNCATE TABLE [oqs].[plans];
    TRUNCATE TABLE [oqs].[queries];
    TRUNCATE TABLE [oqs].[query_runtime_stats];
    TRUNCATE TABLE [oqs].[wait_stats];
    TRUNCATE TABLE [oqs].[excluded_queries];
END
GO

CREATE PROCEDURE oqs.exclude_query_from_dashboard @query_id int
AS
BEGIN
    INSERT INTO [oqs].[excluded_queries] ( [query_id] )
    VALUES ( @query_id )
END
GO

CREATE PROCEDURE [oqs].[data_cleanup]
    @data_cleanup_threshold smallint,
    @data_cleanup_throttle  smallint
AS
    BEGIN

        SELECT @data_cleanup_threshold = [CM].[data_cleanup_threshold],
               @data_cleanup_throttle  = [CM].[data_cleanup_throttle]
        FROM   [oqs].[collection_metadata] AS [CM];

        DELETE TOP ( @data_cleanup_throttle )
        FROM [oqs].[activity_log]
        WHERE [log_timestamp] < DATEADD( DAY, -@data_cleanup_threshold, GETDATE());

        -- We're going to collect query_ids to allow for a controlled deletion in this purge step
        -- There are multiple tables to be cleaned up, so we store the deletion candidates in a temp table

        IF OBJECT_ID( 'tempdb..#query_id_deletion_candidates' ) IS NOT NULL
            DROP TABLE [#query_id_deletion_candidates];

        CREATE TABLE [#query_id_deletion_candidates]
            (
                [query_id] int NOT NULL,
                [plan_id]  int NOT NULL,
                PRIMARY KEY CLUSTERED ( [query_id], [plan_id] )
            );

        WITH [deletion_candidates] ( [query_id], [last_execution_date] )
        AS ( SELECT   [QRS].[query_id],
                      MAX( [QRS].[last_execution_time] ) AS [last_execution_time]
             FROM     [oqs].[query_runtime_stats] AS [QRS]
             GROUP BY [QRS].[query_id]
           )
        INSERT INTO [#query_id_deletion_candidates] (   [query_id],
                                                        [plan_id]
                                                    )
                    SELECT [Q].[query_id],
                           [Q].[plan_id]
                    FROM   [deletion_candidates]      AS [DC]
                           INNER JOIN [oqs].[queries] AS [Q] ON [Q].[query_id] = [DC].[query_id]
                    WHERE  [DC].[last_execution_date] < DATEADD( DAY, -@data_cleanup_threshold, GETDATE());
        ;


        DELETE TOP ( @data_cleanup_throttle )
        [QRS]
        FROM [#query_id_deletion_candidates]        AS [QIDC]
             INNER JOIN [oqs].[query_runtime_stats] AS [QRS] ON [QRS].[query_id] = [QIDC].[query_id];

        DELETE TOP ( @data_cleanup_throttle )
        [Q]
        FROM [#query_id_deletion_candidates] AS [QIDC]
             INNER JOIN [oqs].[queries]      AS [Q] ON [Q].[plan_id] = [QIDC].[plan_id]
                                                       AND [Q].[query_id] = [QIDC].[query_id];

        DELETE TOP ( @data_cleanup_throttle )
        [P]
        FROM  [#query_id_deletion_candidates] AS [QIDC]
              INNER JOIN [oqs].[plans]        AS [P] ON [P].[plan_id] = [QIDC].[plan_id]
        WHERE NOT EXISTS ( SELECT * FROM [oqs].[queries] AS [Q] WHERE [P].[plan_id] = [Q].[plan_id] );

        DECLARE @interval_id int;

        -- For wait_stats we identify deletion candidates by getting the youngest interval within the deletion threshold
        SELECT @interval_id = MAX( [I].[interval_id] )
        FROM   [oqs].[intervals] AS [I]
        WHERE  [I].[interval_end] < DATEADD( DAY, -@data_cleanup_threshold, GETDATE());

        DELETE TOP ( @data_cleanup_throttle )
        FROM [oqs].[wait_stats]
        WHERE [interval_id] <= @interval_id;


    END;
GO

CREATE VIEW oqs.object_catalog
AS
SELECT o.[name] AS [object_name],
       o.[object_id] AS [object_id],
       o.[type] AS [object_type],
	   ps.row_count,
	   ( ps.reserved_page_count * 8 ) AS [space_used_kb]
FROM sys.objects o
    INNER JOIN sys.schemas s
        ON o.[schema_id] = s.[schema_id]
	LEFT JOIN sys.dm_db_partition_stats ps ON ps.object_id = o.object_id AND ps.index_id IN (0,1)
WHERE s.[name] = 'oqs';
GO