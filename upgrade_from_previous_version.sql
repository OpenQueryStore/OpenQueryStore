-- change to SQLCMD mode with ALT+Q+M
:SETVAR OQSMode "classic"
:SETVAR DatabaseWhereOQSIsRunning "OQS"

USE $(DatabaseWhereOQSIsRunning)
GO

ALTER TABLE [oqs].[collection_metadata]
ADD	   [data_cleanup_active]    bit             NOT NULL, -- Should OQS automatically clean up old data
        [data_cleanup_threshold] tinyint         NOT NULL, -- How many days should OQS keep data for (automated cleanup removes data older than this)
        [data_cleanup_throttle]  smallint        NOT NULL, -- How many rows can be deleted in one pass. This avoids large deletions from trashing the transaction log and blocking OQS tables.
	   CONSTRAINT [df_cleanup_active] DEFAULT (0) FOR [data_cleanup_active],
	   CONSTRAINT [df_cleanup_threshold] DEFAULT (2) FOR [data_cleanup_threshold],
	   CONSTRAINT [df_cleanup_throttle] DEFAULT (5000) FOR [data_cleanup_throttle]

-- Semi-hidden way of documenting the version of OQS that is installed. The value will be automatically bumped upon a new version build/release
EXEC sys.sp_addextendedproperty @name=N'oqs_version', @value=N'2.1.0' , @level0type=N'SCHEMA',@level0name=N'oqs', @level1type=N'TABLE',@level1name=N'collection_metadata'
GO  

DELETE FROM [oqs].[collection_metadata]
GO

INSERT INTO [oqs].[collection_metadata] (   [command],
                                            [collection_interval],
                                            [oqs_mode],
                                            [oqs_classic_db],
                                            [collection_active],
                                            [execution_threshold],
                                            [data_cleanup_active],
                                            [data_cleanup_threshold],
                                            [data_cleanup_throttle]
                                        )
VALUES ( N'EXEC [oqs].[gather_statistics] @logmode=1', 60 , '$(OQSMode)','$(DatabaseWhereOQSIsRunning)',1,2,0,30,5000);
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

CREATE UNIQUE NONCLUSTERED INDEX uncl_query_runtime_stats_cleanup ON [oqs].[query_runtime_stats] ([query_id],[interval_id]) INCLUDE ([last_execution_time]);
GO

IF OBJECT_ID('[oqs].[query_stats]') IS NOT NULL
    DROP VIEW [oqs].[query_stats]
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
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[total_rows],' +	-- total_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[last_rows],' +	-- last_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[min_rows],' +	-- min_rows appears in sql 2008r2 SP1
CASE WHEN @MajorVersion = 9 OR ( @MajorVersion = 10 AND @MinorVersion < 50 AND @BuildVersion < 2500 ) THEN 'CAST(0 as bigint) ' ELSE '' END + '[max_rows],' +	-- max_rows appears in sql 2008r2 SP1
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

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [oqs].[gather_statistics]
    @debug   int = 0,
    @logmode int = 0
AS
    DECLARE @log_logrunid int;
    DECLARE @log_newplans int;
    DECLARE @log_newqueries int;
    DECLARE @log_runtime_stats int;
    DECLARE @log_wait_stats int;
    DECLARE @execution_threshold int;
    DECLARE @collection_active bit;
    DECLARE @data_cleanup_active bit;
    DECLARE @data_cleanup_threshold bit;
    DECLARE @data_cleanup_throttle bit;
    BEGIN

        SET NOCOUNT ON;

        SELECT @collection_active   = [collection_active],
               @data_cleanup_active = [data_cleanup_active]
        FROM   [oqs].[collection_metadata];

        -- Data collection must be activated for us to actually collect data
        IF  ( SELECT [collection_active] FROM [oqs].[collection_metadata] ) = 1
            BEGIN

                IF @logmode = 1
                    BEGIN
                        SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                                FROM   [oqs].[activity_log] AS [AL]
                                            );
                    END;

                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore capture script started...' );
                    END;

                -- Create a new interval
                INSERT INTO [oqs].[intervals] ( [interval_start] ) VALUES ( GETDATE());

                -- Setup the execution_threshold. If we have no setting, we default to 2 to avoid single use plans
                SELECT @execution_threshold = [execution_threshold]
                FROM   [oqs].[collection_metadata];

                IF @execution_threshold IS NULL SET @execution_threshold = 2;


                IF OBJECT_ID('tempdb..#plan_dbid') IS NOT NULL
                    DROP TABLE #plan_dbid;
					
				CREATE TABLE #plan_dbid (
					plan_handle varbinary(64) NOT NULL,
					dbid int NOT NULL,
					PRIMARY KEY CLUSTERED (plan_handle, dbid)
				)

				-- We capture plans based on databases that are present in the Databases table of the OpenQueryStore database				
				-- First, everythihng does into a temp table (we'll need this data for the next query)	
                INSERT INTO #plan_dbid (   [plan_handle],
                                           [dbid]
                                       )
                            SELECT   [plan_handle],
                                     CONVERT( int, [pvt].[dbid] )
                            FROM     (   SELECT [plan_handle],
                                                [epa].[attribute],
                                                [epa].[value]
                                         FROM   [sys].[dm_exec_cached_plans]
                                                OUTER APPLY [sys].[dm_exec_plan_attributes]( [plan_handle] ) AS [epa]
                                         WHERE  [cacheobjtype] = 'Compiled Plan'
                                                AND [usecounts] >= @execution_threshold
                                     ) AS [ecpa]
                            PIVOT (   MAX([value])
                                      FOR [attribute] IN ( "dbid", "sql_handle" )
                                  ) AS [pvt]
                            WHERE    [plan_handle] NOT IN ( SELECT [PD].[plan_handle] FROM [oqs].[plan_dbid] AS [PD] )
                                     AND [pvt].[dbid] IN (   SELECT DB_ID( [MD].[database_name] )
                                                             FROM   [oqs].[monitored_databases] AS [MD]
                                                         )
                            ORDER BY [pvt].[sql_handle];

                -- Next, we add the rows to the destination table
                INSERT INTO [oqs].[plan_dbid] (   [plan_handle] ,
                                                  [dbid]
                                              )
                            SELECT [plan_handle], [dbid]
                            FROM #plan_dbid;

                -- Start execution plan insertion
                -- Get plans from the plan cache that do not exist in the OQS_Plans table
                -- for the database on the current context
                ;
                WITH XMLNAMESPACES ( DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' )
                INSERT INTO [oqs].[plans] (   [plan_MD5],
                                              [plan_handle],
                                              [plan_firstfound],
                                              [plan_database],
                                              [plan_refcounts],
                                              [plan_usecounts],
                                              [plan_sizeinbytes],
                                              [plan_type],
                                              [plan_objecttype],
                                              [plan_executionplan],
                                              [estimated_available_degree_of_parallelism],
                                              [estimated_statement_subtree_cost],
                                              [estimated_available_memory_grant],
                                              [cost_threshold_for_parallelism]
                                          )
                            SELECT SUBSTRING( [master].[sys].[fn_repl_hash_binary]( CONVERT( varbinary (MAX), [n].[query]( '.' ))), 1, 32 ),
                                   [cp].[plan_handle],
                                   GETDATE(),
                                   DB_NAME( [pd].[dbid] ),
                                   [cp].[refcounts],
                                   [cp].[usecounts],
                                   [cp].[size_in_bytes],
                                   [cp].[cacheobjtype],
                                   [cp].[objtype],
                                   [qp].[query_plan],
                                   [qp].[query_plan].[value]( '(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/OptimizerHardwareDependentProperties/@EstimatedAvailableDegreeOfParallelism)[1]', 'int' ) AS [available_degree_of_parallelism],
                                   CAST([qp].[query_plan].[value]( '(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementSubTreeCost)[1]', 'real' ) AS numeric (19, 12))                                      AS [estimated_statement_subtree_cost],
                                   [qp].[query_plan].[value]( '(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/OptimizerHardwareDependentProperties/@EstimatedAvailableMemoryGrant)[1]', 'int' )         AS [estimated_available_memory_grant],
                                   ( SELECT CAST([C].[value_in_use] AS int) FROM   [sys].[configurations] AS [C] WHERE  [C].[name] = 'cost threshold for parallelism' )                                                       AS [cost_threshold_for_parallelism]
                            FROM   [oqs].[plan_dbid] AS [pd]
							       INNER MERGE JOIN #plan_dbid AS tpdb ON [tpdb].[plan_handle] = [pd].[plan_handle] AND [tpdb].[dbid] = [pd].[dbid]
                                   INNER HASH JOIN [sys].[dm_exec_cached_plans] AS [cp] ON [pd].[plan_handle] = [cp].[plan_handle]
                                   CROSS APPLY [sys].[dm_exec_query_plan]( [cp].[plan_handle] ) AS [qp]
                                   CROSS APPLY [query_plan].[nodes]( '/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/RelOp' ) AS [q]([n])
                                   CROSS APPLY [sys].[dm_exec_sql_text]( [cp].[plan_handle] )
                            WHERE  [cp].[cacheobjtype] = 'Compiled Plan'
                                   AND ( [qp].[query_plan] IS NOT NULL )
                                   AND CONVERT( varbinary, SUBSTRING( [master].[sys].[fn_repl_hash_binary]( CONVERT( varbinary (MAX), [n].[query]( '.' ))), 1, 32 )) NOT IN ( SELECT [plan_MD5] FROM [oqs].[plans] )
                                   AND [qp].[query_plan].[exist]( '//ColumnReference[@Schema = "[oqs]"]' ) = 0;

                SET @log_newplans = @@RowCount;

                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_newplans ) + ' new plan(s)...' );
                    END

                -- Grab all of the queries (statement level) that are connected to the plans inside the OQS
                ;
                WITH [CTE_Queries] ( [plan_id], [plan_handle], [plan_MD5] )
                AS ( SELECT [plan_id], [plan_handle], [plan_MD5] FROM [oqs].[plans] )
                INSERT INTO [oqs].[queries] (   [plan_id],
                                                [query_hash],
                                                [query_plan_MD5],
                                                [query_statement_text],
                                                [query_statement_start_offset],
                                                [query_statement_end_offset],
                                                [query_creation_time]
                                            )
                            SELECT [cte].[plan_id],
                                   [qs].[query_hash],
                                   ( [cte].[plan_MD5] + [qs].[query_hash] ) AS [Query_plan_MD5],
                                   SUBSTRING(   [st].[text], ( [qs].[statement_start_offset] / 2 ) + 1, (( CASE [qs].[statement_end_offset]
                                                                                                                WHEN-1 THEN DATALENGTH( [st].[text] )
                                                                                                                ELSE [qs].[statement_end_offset]
                                                                                                           END - [qs].[statement_start_offset]
                                                                                                         ) / 2
                                                                                                        ) + 1
                                            )                               AS [statement_text],
                                   [qs].[statement_start_offset],
                                   [qs].[statement_end_offset],
                                   [qs].[creation_time]
                            FROM   [CTE_Queries] AS [cte]
                                   INNER JOIN [oqs].[query_stats] AS [qs] ON [cte].[plan_handle] = [qs].[plan_handle]
                                   CROSS APPLY [sys].[dm_exec_sql_text]( [qs].[sql_handle] ) AS [st]
                            WHERE  ( [cte].[plan_MD5] + [qs].[query_hash] ) NOT IN ( SELECT [Query_plan_MD5] FROM [oqs].[queries] );

                SET @log_newqueries = @@RowCount;

                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_newqueries ) + ' new queries...' );
                    END;

                -- Grab the interval_id of the interval we added at the beginning
                DECLARE @Interval_ID int;
                SET @Interval_ID = IDENT_CURRENT( '[oqs].[intervals]' );

                -- Query Runtime Snapshot

                -- Insert runtime statistics for every query statement that is recorded inside the OQS
                INSERT INTO [oqs].[query_runtime_stats] (   [query_id],
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
                                                            [avg_logical_writes]
                                                        )
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
                            FROM   [oqs].[queries] AS [oqs_q]
                                   INNER JOIN [oqs].[query_stats] AS [qs] ON (   [oqs_q].[query_hash] = [qs].[query_hash]
                                                                                 AND [oqs_q].[query_statement_start_offset] = [qs].[statement_start_offset]
                                                                                 AND [oqs_q].[query_statement_end_offset] = [qs].[statement_end_offset]
                                                                                 AND [oqs_q].[query_creation_time] = [qs].[creation_time]
                                                                             );

                -- DEBUG: Get current info of the QRS table
                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of captured runtime statistics';
                        SELECT * FROM [oqs].[query_runtime_stats] AS [QRS];
                    END;

                -- Wait stats snapshot
                INSERT INTO [oqs].[wait_stats] (   [interval_id],
                                                   [wait_type],
                                                   [waiting_tasks_count],
                                                   [wait_time_ms],
                                                   [max_wait_time_ms],
                                                   [signal_wait_time_ms]
                                               )
                            SELECT @Interval_ID,
                                   [wait_type],
                                   [waiting_tasks_count],
                                   [wait_time_ms],
                                   [max_wait_time_ms],
                                   [signal_wait_time_ms]
                            FROM   [sys].[dm_os_wait_stats]
                            WHERE  (   [waiting_tasks_count] > 0
                                       AND [wait_time_ms] > 0
                                   );

                -- DEBUG: Get current info of the wait stats table
                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of wait stats';
                        SELECT * FROM [oqs].[wait_stats] AS [WS];
                    END;

                -- Close the previous interval now that the statistics are updated
                UPDATE [oqs].[intervals]
                SET    [interval_end] = GETDATE()
                WHERE  [interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] );

                -- Now that we have the runtime statistics inside the OQS we need to perform some calculations
                -- so we can see query performance per interval captured

                -- First thing we need is a temporary table to hold our calculated deltas
                IF OBJECT_ID( 'tempdb..#OQS_Runtime_Stats' ) IS NOT NULL
                    BEGIN
                        DROP TABLE [#OQS_Runtime_Stats];
                    END;

                IF OBJECT_ID( 'tempdb..#OQS_Wait_Stats' ) IS NOT NULL
                    BEGIN
                        DROP TABLE [#OQS_Wait_Stats];
                    END

                -- Calculate Deltas for Runtime stats
                ;
                WITH [CTE_Update_Runtime_Stats] ( [query_id], [interval_id], [execution_count], [total_elapsed_time], [total_rows], [total_worker_time], [total_physical_reads], [total_logical_reads], [total_logical_writes] )
                AS ( SELECT [QRS].[query_id],
                            [QRS].[interval_id],
                            [QRS].[execution_count],
                            [QRS].[total_elapsed_time],
                            [QRS].[total_rows],
                            [QRS].[total_worker_time],
                            [QRS].[total_physical_reads],
                            [QRS].[total_logical_reads],
                            [QRS].[total_logical_writes]
                     FROM   [oqs].[query_runtime_stats] AS [QRS]
                     WHERE  [QRS].[interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] )
                   )
                SELECT [cte].[query_id],
                       [cte].[interval_id],
                       ( [qrs].[execution_count] - [cte].[execution_count] )                                                                                            AS [Delta Exec Count],
                       ( [qrs].[total_elapsed_time] - [cte].[total_elapsed_time] )                                                                                      AS [Delta Time],
                       ISNULL((( [qrs].[total_elapsed_time] - [cte].[total_elapsed_time] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )     AS [Avg. Time],
                       ( [qrs].[total_rows] - [cte].[total_rows] )                                                                                                      AS [Delta Total Rows],
                       ISNULL((( [qrs].[total_rows] - [cte].[total_rows] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )                     AS [Avg. Rows],
                       ( [qrs].[total_worker_time] - [cte].[total_worker_time] )                                                                                        AS [Delta Total Worker Time],
                       ISNULL((( [qrs].[total_worker_time] - [cte].[total_worker_time] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )       AS [Avg. Worker Time],
                       ( [qrs].[total_physical_reads] - [cte].[total_physical_reads] )                                                                                  AS [Delta Total Phys Reads],
                       ISNULL((( [qrs].[total_physical_reads] - [cte].[total_physical_reads] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 ) AS [Avg. Phys reads],
                       ( [qrs].[total_logical_reads] - [cte].[total_logical_reads] )                                                                                    AS [Delta Total Log Reads],
                       ISNULL((( [qrs].[total_logical_reads] - [cte].[total_logical_reads] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )   AS [Avg. Log reads],
                       ( [qrs].[total_logical_writes] - [cte].[total_logical_writes] )                                                                                  AS [Delta Total Log Writes],
                       ISNULL((( [qrs].[total_logical_writes] - [cte].[total_logical_writes] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 ) AS [Avg. Log writes]
                INTO   [#OQS_Runtime_Stats]
                FROM   [CTE_Update_Runtime_Stats] AS [cte]
                       INNER JOIN [oqs].[query_runtime_stats] AS [qrs] ON [cte].[query_id] = [qrs].[query_id]
                WHERE  [qrs].[interval_id] = ( SELECT MAX( [interval_id] ) FROM [oqs].[intervals] );

                SET @log_runtime_stats = @@RowCount;

                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_runtime_stats ) + ' runtime statistics...' );
                    END;

                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of runtime statistics deltas';
                        SELECT * FROM [#OQS_Runtime_Stats];
                    END;

                -- Update the runtime statistics of the queries captured in the previous interval
                -- with the delta runtime information
                UPDATE [qrs]
                SET    [qrs].[execution_count] = [tqrs].[Delta Exec Count],
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
                FROM   [oqs].[query_runtime_stats] AS [qrs]
                       INNER JOIN [#OQS_Runtime_Stats] AS [tqrs] ON (   [qrs].[interval_id] = [tqrs].[interval_id]
                                                                        AND [qrs].[query_id] = [tqrs].[query_id]
                                                                    );

                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of runtime statistics after delta update';
                        SELECT * FROM [oqs].[query_runtime_stats] AS [qrs];
                    END;

                    -- Calculate Deltas for Wait stats
                ;
                WITH [CTE_Update_wait_Stats] ( [interval_id], [wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms] )
                    AS ( SELECT [WS].[interval_id],
                                [WS].[wait_type],
                                [WS].[waiting_tasks_count],
                                [WS].[wait_time_ms],
                                [WS].[max_wait_time_ms],
                                [WS].[signal_wait_time_ms]
                         FROM   [oqs].[wait_stats] AS [WS]
                         WHERE  [WS].[interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] )
                       )
                SELECT [cte].[wait_type],
                       [cte].[interval_id],
                       ( [WS].[waiting_tasks_count] - [cte].[waiting_tasks_count] ) AS [Delta Waiting Tasks Count],
                       ( [WS].[wait_time_ms] - [cte].[wait_time_ms] )               AS [Delta Wait Time ms],
                       ( [WS].[max_wait_time_ms] - [cte].[max_wait_time_ms] )       AS [Delta Max Wait Time ms],
                       ( [WS].[signal_wait_time_ms] - [cte].[signal_wait_time_ms] ) AS [Delta Signal Wait Time ms]
                INTO   [#OQS_Wait_Stats]
                FROM   [CTE_Update_wait_Stats] AS [cte]
                       INNER JOIN [oqs].[wait_stats] AS [WS] ON [cte].[wait_type] = [WS].[wait_type]
                WHERE  [WS].[interval_id] = ( SELECT MAX( [interval_id] ) FROM [oqs].[intervals] );

                SET @log_wait_stats = @@RowCount;

                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_wait_stats ) + ' wait statistics...' );
                    END;

                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of wait stats deltas';
                        SELECT * FROM [#OQS_Wait_Stats];
                    END;

                -- Update the runtime statistics of the queries captured in the previous interval
                -- with the delta runtime information
                UPDATE [WS]
                SET    [WS].[waiting_tasks_count] = [tws].[Delta Waiting Tasks Count],
                       [WS].[wait_time_ms] = [Delta Wait Time ms],
                       [WS].[max_wait_time_ms] = [Delta Max Wait Time ms],
                       [WS].[signal_wait_time_ms] = [Delta Signal Wait Time ms]
                FROM   [oqs].[wait_stats] AS [WS]
                       INNER JOIN [#OQS_Wait_Stats] AS [tws] ON (   [WS].[interval_id] = [tws].[interval_id]
                                                                    AND [WS].[wait_type] = [tws].[wait_type]
                                                                );

                IF @debug = 1
                    BEGIN
                        SELECT 'Snapshot of runtime statistics after delta update';
                        SELECT * FROM [oqs].[wait_stats] AS [WS];
                    END;

                -- Remove the wait stats delta's where 0 waits occured
                DELETE FROM [oqs].[wait_stats]
                WHERE (   [waiting_tasks_count] = 0
                          AND [interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] )
                      );

                -- Run regular OQS store cleanup if activated
                IF @data_cleanup_active = 1
                    BEGIN
                        IF @logmode = 1
                            BEGIN
                                INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                                     [log_timestamp],
                                                                     [log_message]
                                                                 )
                                VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore data cleanup process executed.' );
                            END;


                        EXEC [oqs].[data_cleanup] @data_cleanup_threshold = @data_cleanup_threshold,
                                                  @data_cleanup_throttle = @data_cleanup_throttle;

                    END;


                -- And we are done!
                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                             [log_timestamp],
                                                             [log_message]
                                                         )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore capture script finished...' );
                    END;
            END;
        ELSE
            BEGIN
                BEGIN
                    SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                            FROM   [oqs].[activity_log] AS [AL]
                                        );

                    INSERT INTO [oqs].[activity_log] (   [log_run_id],
                                                         [log_timestamp],
                                                         [log_message]
                                                     )
                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore data capture not activated. Collection skipped' );
                END;
            END;
    END;
GO